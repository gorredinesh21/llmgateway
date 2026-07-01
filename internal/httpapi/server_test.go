package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorredinesh21/llmgateway/internal/embed"
	"github.com/gorredinesh21/llmgateway/internal/provider"
)

// newTestServer builds a Server backed by the zero-cost mock provider.
func newTestServer() *Server {
	mock := provider.NewMock(8, 0)
	pipe := embed.New(mock, embed.Config{Workers: 4, CacheSize: 64})
	return New(pipe)
}

// TestHealthz checks the liveness endpoint returns 200 + JSON.
func TestHealthz(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status field=%q want ok", body["status"])
	}
}

// TestEmbedHandler posts a small batch and checks the response shape.
func TestEmbedHandler(t *testing.T) {
	srv := newTestServer()
	reqBody := `{"inputs":["alpha","beta","alpha"],"workers":4}`
	req := httptest.NewRequest(http.MethodPost, "/embed", strings.NewReader(reqBody))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200, body=%s", rec.Code, rec.Body.String())
	}
	var resp embedResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("bad JSON: %v", err)
	}
	if resp.Total != 3 || resp.OK != 3 {
		t.Fatalf("total=%d ok=%d want 3,3", resp.Total, resp.OK)
	}
	if len(resp.Results) != 3 {
		t.Fatalf("got %d results want 3", len(resp.Results))
	}
	// First result should carry a vector.
	if len(resp.Results[0].Vector) == 0 {
		t.Fatal("expected a non-empty vector")
	}
	// Deterministic mock: "alpha" appears twice -> same vector.
	if resp.Results[0].Vector[0] != resp.Results[2].Vector[0] {
		t.Fatal("identical inputs should yield identical vectors")
	}
}

// TestEmbedBadRequest checks that an empty inputs array is rejected.
func TestEmbedBadRequest(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/embed", strings.NewReader(`{"inputs":[]}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", rec.Code)
	}
}

// TestMetrics scrapes /metrics after a request and checks the Prometheus format.
func TestMetrics(t *testing.T) {
	srv := newTestServer()

	// Generate some activity so counters are non-zero.
	body := strings.NewReader(`{"inputs":["a","b"]}`)
	er := httptest.NewRecorder()
	srv.Handler().ServeHTTP(er, httptest.NewRequest(http.MethodPost, "/embed", body))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	out := rec.Body.String()
	for _, want := range []string{
		"# TYPE llmgateway_requests_total counter",
		"llmgateway_requests_total 1",
		"# TYPE llmgateway_embeddings_total counter",
		"# TYPE llmgateway_workers_in_flight gauge",
		"llmgateway_cache_misses_total",
		"llmgateway_retries_total",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, out)
		}
	}
}

// TestEmbedStream checks the SSE endpoint emits progress + a final result event.
func TestEmbedStream(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest(http.MethodPost, "/embed/stream", strings.NewReader(`{"inputs":["a","b","c"]}`))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want 200", rec.Code)
	}
	out := rec.Body.String()
	if !strings.Contains(out, "event: progress") {
		t.Fatalf("missing progress event:\n%s", out)
	}
	if !strings.Contains(out, "event: result") {
		t.Fatalf("missing result event:\n%s", out)
	}
	if !strings.Contains(out, `"total":3`) {
		t.Fatalf("missing total in progress:\n%s", out)
	}
}
