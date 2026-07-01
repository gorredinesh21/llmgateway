// Package httpapi exposes the embedding pipeline over HTTP: a batch endpoint, a
// health check, hand-written Prometheus metrics, and an SSE progress stream.
//
// The server holds a single *embed.Pipeline shared across all requests (it is
// safe for concurrent use). Each request builds its own context from
// r.Context(), so if the client disconnects mid-batch the whole batch is
// cancelled — no wasted provider calls.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorredinesh21/llmgateway/internal/embed"
)

// Server wires the pipeline and metrics into an http.Handler.
type Server struct {
	pipe    *embed.Pipeline
	metrics *Metrics
	// baseline cache stats captured so per-request deltas can be reported; the
	// pipeline's cache counters are cumulative across the process.
}

// New returns a Server backed by the given pipeline.
func New(pipe *embed.Pipeline) *Server {
	return &Server{pipe: pipe, metrics: NewMetrics()}
}

// Handler returns the http.Handler with all routes registered. Using an
// explicit mux keeps routing obvious and testable with httptest.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/embed", s.handleEmbed)
	mux.HandleFunc("/embed/stream", s.handleEmbedStream)
	return mux
}

// --- request / response types ---

// embedRequest is the JSON body for POST /embed and /embed/stream.
type embedRequest struct {
	Inputs  []string `json:"inputs"`
	Workers int      `json:"workers"` // informational: pipeline worker count is fixed at startup
	RPS     float64  `json:"rps"`     // informational
}

// itemResult is one embedding in the response. Vector is omitted on error.
type itemResult struct {
	Input  string    `json:"input"`
	Vector []float32 `json:"vector,omitempty"`
	Cached bool      `json:"cached"`
	Error  string    `json:"error,omitempty"`
}

// embedResponse is the JSON returned by POST /embed.
type embedResponse struct {
	Results   []itemResult `json:"results"`
	Total     int          `json:"total"`
	OK        int          `json:"ok"`
	Failed    int          `json:"failed"`
	CacheHits int          `json:"cache_hits"`
	ElapsedMs float64      `json:"elapsed_ms"`
}

// --- handlers ---

// handleHealth is a trivial liveness probe.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleMetrics renders the hand-written Prometheus exposition format. It syncs
// the pipeline's cache counters into the metrics registry first so the scrape
// reflects current totals.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Prometheus expects this exact content type.
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	s.metrics.WriteProm(w)
}

// handleEmbed runs a batch synchronously and returns all vectors as JSON. The
// batch is bound to r.Context(): a client disconnect cancels it.
func (s *Server) handleEmbed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	req, err := decodeEmbedRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.metrics.IncRequests()
	s.metrics.EnterInFlight()
	defer s.metrics.LeaveInFlight()

	retriesBefore := s.pipe.Retries()
	out, st := s.pipe.EmbedBatch(r.Context(), req.Inputs)
	s.recordBatch(st, retriesBefore)

	resp := embedResponse{
		Results:   make([]itemResult, len(out)),
		Total:     st.Total,
		OK:        st.OK,
		Failed:    st.Failed,
		CacheHits: st.CacheHits,
		ElapsedMs: float64(st.Elapsed.Microseconds()) / 1000.0,
	}
	for i, e := range out {
		resp.Results[i] = itemResult{Input: e.Input, Vector: e.Vector, Cached: e.Cached}
		if e.Err != nil {
			resp.Results[i].Error = e.Err.Error()
			resp.Results[i].Vector = nil
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleEmbedStream streams progress as Server-Sent Events (SSE): a series of
// {"done":N,"total":M} events as items complete, then a final result event.
//
// SSE is just a long-lived text/event-stream response where each message is
// "data: <payload>\n\n". We embed items one worker-batch at a time to report
// incremental progress cheaply while still using the pipeline.
func (s *Server) handleEmbedStream(w http.ResponseWriter, r *http.Request) {
	req, err := decodeEmbedRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	s.metrics.IncRequests()
	s.metrics.EnterInFlight()
	defer s.metrics.LeaveInFlight()

	ctx := r.Context()
	total := len(req.Inputs)
	retriesBefore := s.pipe.Retries()

	// Process in small chunks so we can emit progress between them. Each chunk
	// still runs concurrently inside the pipeline's worker pool.
	const chunkSize = 8
	allResults := make([]itemResult, 0, total)
	var agg embed.Stats
	agg.Total = total

	for done := 0; done < total; {
		if err := ctx.Err(); err != nil {
			return // client disconnected
		}
		end := done + chunkSize
		if end > total {
			end = total
		}
		out, st := s.pipe.EmbedBatch(ctx, req.Inputs[done:end])
		for _, e := range out {
			ir := itemResult{Input: e.Input, Vector: e.Vector, Cached: e.Cached}
			if e.Err != nil {
				ir.Error = e.Err.Error()
				ir.Vector = nil
			}
			allResults = append(allResults, ir)
		}
		agg.OK += st.OK
		agg.Failed += st.Failed
		agg.CacheHits += st.CacheHits
		agg.Elapsed += st.Elapsed
		done = end

		// Emit a progress event.
		fmt.Fprintf(w, "event: progress\ndata: {\"done\":%d,\"total\":%d}\n\n", done, total)
		flusher.Flush()
	}
	s.recordBatch(agg, retriesBefore)

	// Emit the final result event with all embeddings.
	final := embedResponse{
		Results:   allResults,
		Total:     agg.Total,
		OK:        agg.OK,
		Failed:    agg.Failed,
		CacheHits: agg.CacheHits,
		ElapsedMs: float64(agg.Elapsed.Microseconds()) / 1000.0,
	}
	payload, _ := json.Marshal(final)
	fmt.Fprintf(w, "event: result\ndata: %s\n\n", payload)
	flusher.Flush()
}

// recordBatch folds one batch's stats into the metrics registry.
func (s *Server) recordBatch(st embed.Stats, retriesBefore int64) {
	s.metrics.AddEmbeddings(st.OK)
	s.metrics.AddCacheHits(st.CacheHits)
	s.metrics.AddCacheMisses(st.OK - st.CacheHits + st.Failed)
	s.metrics.AddRetries(s.pipe.Retries() - retriesBefore)
	s.metrics.AddDuration(st.Elapsed)
}

// --- helpers ---

func decodeEmbedRequest(r *http.Request) (embedRequest, error) {
	var req embedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return req, fmt.Errorf("invalid JSON body: %w", err)
	}
	if len(req.Inputs) == 0 {
		return req, fmt.Errorf("field 'inputs' must be a non-empty array")
	}
	return req, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Serve starts an HTTP server on addr and blocks until ctx is cancelled, then
// shuts the server down gracefully (draining in-flight requests) within a short
// timeout. Callers wire ctx to SIGINT/SIGTERM for a clean Ctrl-C.
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:    addr,
		Handler: s.Handler(),
	}

	// Run ListenAndServe in a goroutine so we can watch ctx for shutdown.
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err // server failed to start / crashed
	case <-ctx.Done():
		// Graceful shutdown: stop accepting new conns, let in-flight ones finish.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
