package httpapi

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// Metrics holds process-wide counters exposed at GET /metrics in the Prometheus
// text exposition format. We write that format BY HAND (no client_golang) — it
// is just plain text: an optional # HELP and # TYPE line, then "name value".
//
// All fields are atomics so handlers running on many goroutines can bump them
// without a mutex. This is exactly how you'd instrument a real service: cheap
// counters on the hot path, formatted lazily only when /metrics is scraped.
type Metrics struct {
	requestsTotal     atomic.Int64 // total HTTP /embed requests handled
	embeddingsTotal   atomic.Int64 // total embeddings successfully produced
	workersInFlight   atomic.Int64 // gauge: batches currently being processed
	cacheHits         atomic.Int64
	cacheMisses       atomic.Int64
	retriesTotal      atomic.Int64
	requestDurationNs atomic.Int64 // cumulative /embed handler time (for an avg)
}

// NewMetrics returns a zeroed metrics registry.
func NewMetrics() *Metrics { return &Metrics{} }

// The following helpers are the only way handlers mutate metrics, keeping the
// call sites readable.

func (m *Metrics) IncRequests()          { m.requestsTotal.Add(1) }
func (m *Metrics) AddEmbeddings(n int)   { m.embeddingsTotal.Add(int64(n)) }
func (m *Metrics) EnterInFlight()        { m.workersInFlight.Add(1) }
func (m *Metrics) LeaveInFlight()        { m.workersInFlight.Add(-1) }
func (m *Metrics) AddCacheHits(n int)    { m.cacheHits.Add(int64(n)) }
func (m *Metrics) AddCacheMisses(n int)  { m.cacheMisses.Add(int64(n)) }
func (m *Metrics) AddRetries(n int64)    { m.retriesTotal.Add(n) }
func (m *Metrics) AddDuration(d time.Duration) { m.requestDurationNs.Add(int64(d)) }

// WriteProm writes all metrics in Prometheus text exposition format. Each metric
// gets a HELP line (human description), a TYPE line (counter or gauge), then the
// current value. Counters only go up; a gauge can go up or down.
func (m *Metrics) WriteProm(w io.Writer) {
	writeMetric(w, "counter", "llmgateway_requests_total",
		"Total number of /embed requests handled.", float64(m.requestsTotal.Load()))

	writeMetric(w, "counter", "llmgateway_embeddings_total",
		"Total number of embeddings successfully produced.", float64(m.embeddingsTotal.Load()))

	writeMetric(w, "gauge", "llmgateway_workers_in_flight",
		"Number of embedding batches currently being processed.", float64(m.workersInFlight.Load()))

	writeMetric(w, "counter", "llmgateway_cache_hits_total",
		"Total cache hits.", float64(m.cacheHits.Load()))

	writeMetric(w, "counter", "llmgateway_cache_misses_total",
		"Total cache misses.", float64(m.cacheMisses.Load()))

	writeMetric(w, "counter", "llmgateway_retries_total",
		"Total provider-call retries performed.", float64(m.retriesTotal.Load()))

	writeMetric(w, "counter", "llmgateway_request_duration_seconds_total",
		"Cumulative /embed handler wall-clock time in seconds.",
		time.Duration(m.requestDurationNs.Load()).Seconds())
}

// writeMetric emits the three-line block for one metric.
func writeMetric(w io.Writer, typ, name, help string, value float64) {
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s %s\n", name, typ)
	// %g gives a compact representation Prometheus accepts (e.g. 0.42, 1200).
	fmt.Fprintf(w, "%s %g\n", name, value)
}
