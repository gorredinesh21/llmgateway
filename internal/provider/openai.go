package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// OpenAIEmbedder calls an OpenAI-compatible embeddings HTTP endpoint. Many
// providers (OpenAI, Azure OpenAI, Together, Ollama's /v1, vLLM, LiteLLM, most
// self-hosted gateways) speak this exact shape, so one implementation covers a
// lot of backends:
//
//	POST {baseURL}/embeddings
//	Authorization: Bearer <apiKey>
//	{ "model": "<model>", "input": "<text>" }
//
//	200 -> { "data": [ { "embedding": [0.1, 0.2, ...] } ] }
//
// Config comes from the environment (see NewOpenAIFromEnv) — secrets are never
// hardcoded. It implements the Embedder interface just like MockEmbedder, so the
// pipeline does not care which backend it is talking to.
//
// Pointing at Claude, conceptually: Anthropic's API is not OpenAI-shaped, and
// as of this writing Anthropic exposes messages/completions rather than a public
// embeddings endpoint. To use a Claude-backed service here you would either (a)
// front it with an OpenAI-compatible proxy (LiteLLM can do this) and set
// EMBED_BASE_URL to that proxy, or (b) write a sibling ClaudeEmbedder here using
// the official Anthropic Go SDK behind the same Embed(ctx, input) signature.
// Either way the pipeline code stays unchanged — that is the point of the
// interface.
type OpenAIEmbedder struct {
	BaseURL string
	APIKey  string
	Model   string
	client  *http.Client
}

// NewOpenAI builds an embedder for an explicit config. httpTimeout caps a single
// HTTP request (a separate safety net alongside the per-call context timeout the
// pipeline applies).
func NewOpenAI(baseURL, apiKey, model string, httpTimeout time.Duration) *OpenAIEmbedder {
	baseURL = strings.TrimRight(baseURL, "/")
	if httpTimeout <= 0 {
		httpTimeout = 30 * time.Second
	}
	return &OpenAIEmbedder{
		BaseURL: baseURL,
		APIKey:  apiKey,
		Model:   model,
		client:  &http.Client{Timeout: httpTimeout},
	}
}

// NewOpenAIFromEnv reads EMBED_BASE_URL, EMBED_API_KEY and EMBED_MODEL from the
// environment. It returns an error if the base URL is missing so callers can
// fall back to the mock provider.
func NewOpenAIFromEnv() (*OpenAIEmbedder, error) {
	base := os.Getenv("EMBED_BASE_URL")
	if base == "" {
		return nil, fmt.Errorf("EMBED_BASE_URL not set")
	}
	model := os.Getenv("EMBED_MODEL")
	if model == "" {
		model = "text-embedding-3-small" // a common default
	}
	return NewOpenAI(base, os.Getenv("EMBED_API_KEY"), model, 30*time.Second), nil
}

// request/response shapes for the OpenAI-compatible embeddings API.
type embedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type embedResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	// Some servers return an error object instead of data.
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Embed sends one text to the provider and returns its vector. It respects ctx
// (cancellation/timeout propagate to the HTTP request) and returns a non-nil
// error on any non-2xx response so the pipeline's retry logic can kick in.
func (o *OpenAIEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	body, err := json.Marshal(embedRequest{Model: o.Model, Input: input})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.BaseURL+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if o.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+o.APIKey)
	}

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do: %w", err) // network errors are transient -> retried
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // cap at 1MiB

	if resp.StatusCode/100 != 2 {
		// Non-2xx: transient by default (429/5xx), so let it be retried. We do
		// not wrap ErrPermanent here — a 4xx like 401 will simply exhaust
		// retries, which is acceptable and keeps this code simple.
		return nil, fmt.Errorf("embeddings HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed embedResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if parsed.Error != nil {
		return nil, fmt.Errorf("provider error: %s", parsed.Error.Message)
	}
	if len(parsed.Data) == 0 || len(parsed.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("no embedding in response")
	}
	return parsed.Data[0].Embedding, nil
}
