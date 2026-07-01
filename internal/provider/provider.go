// Package provider abstracts the embedding backend behind a tiny interface so the
// pipeline is provider-agnostic. A MockEmbedder is included so the whole system
// runs and benchmarks with no API key and no cost.
package provider

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"time"
)

// Embedder turns text into a vector. Real implementations call an API; the mock
// computes a deterministic vector locally. Implementations must respect ctx.
type Embedder interface {
	Embed(ctx context.Context, input string) ([]float32, error)
}

// MockEmbedder returns deterministic vectors after a simulated network latency.
// Deterministic output makes tests reproducible; the latency makes the
// concurrency win measurable (serial vs pooled) without hitting a real API.
type MockEmbedder struct {
	Dim     int           // vector dimension
	Latency time.Duration // simulated per-call latency
}

func NewMock(dim int, latency time.Duration) *MockEmbedder {
	if dim <= 0 {
		dim = 8
	}
	return &MockEmbedder{Dim: dim, Latency: latency}
}

func (m *MockEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	// Simulate a network call that respects cancellation.
	if m.Latency > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.Latency):
		}
	} else if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Deterministic pseudo-embedding derived from a hash of the input.
	sum := sha256.Sum256([]byte(input))
	vec := make([]float32, m.Dim)
	for i := range vec {
		// Cheaply expand the 32-byte digest into Dim floats in [0,1).
		off := (i * 4) % (len(sum) - 3)
		u := binary.BigEndian.Uint32(sum[off : off+4])
		vec[i] = float32(u) / float32(^uint32(0))
	}
	return vec, nil
}

// --- Real providers go here, all implementing Embedder ---
//
// For a Claude-backed endpoint (chat, rerank, summarize), use the official
// Anthropic Go SDK and the latest models (Claude Opus 4.8 / Sonnet 4.6):
//
//	import "github.com/anthropics/anthropic-sdk-go"
//	// client := anthropic.NewClient() // reads ANTHROPIC_API_KEY from env
//
// For embeddings specifically, wrap your provider's HTTP API here behind the
// same Embed(ctx, input) signature. Read keys from the environment — never
// hardcode them.
