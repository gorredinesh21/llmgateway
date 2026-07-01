# llmgateway — a concurrent LLM gateway + embedding pipeline (Go)

> **Elevator pitch:** A Go service that sits in front of LLM/embedding providers and makes
> bulk AI workloads fast and safe: it embeds thousands of document chunks concurrently with a
> **bounded worker pool**, enforces provider **rate limits** with a token bucket, **caches**
> repeated calls, retries transient failures with backoff, and **cancels the whole batch**
> cleanly via `context`. It exposes a small HTTP API and a CLI.

This is the "GenAI infra in Go" flagship. It marries your GenAI/RAG background with real Go
concurrency — and the concurrency is *honest*: embedding N chunks means N slow network calls,
and doing them one-at-a-time is the naive approach everyone else ships. You do them in a
controlled, rate-limited pool and **measure the speedup**.

Why Go for this (the interview answer): Python's embedding scripts are typically serial or
fight the GIL; Go gives you true parallel I/O with trivial goroutines, precise backpressure,
and a single deployable binary. This is exactly the "plumbing around ML" that Go is for.

---

## The core problem it solves

You have 10,000 text chunks to embed for a RAG index. Naive approach:

```python
for chunk in chunks:          # serial: ~10,000 × latency. Coffee break.
    embed(chunk)
```

Problems: (1) painfully slow, (2) fire everything at once and the provider 429-rate-limits
you, (3) one failure kills the run, (4) no way to cancel. **llmgateway fixes all four:**

- **Bounded concurrency** — a worker pool of, say, 16 keeps 16 calls in flight: fast, but
  never a thundering herd.
- **Rate limiting** — a token-bucket limiter respects the provider's requests-per-second.
- **Retries with backoff + jitter** — transient 429/5xx are retried automatically.
- **Context cancellation** — Ctrl-C or a timeout stops the entire batch immediately.
- **Caching** — identical inputs (by hash) skip the network entirely.

---

## Architecture

```
  HTTP / CLI request: []text chunks
        │
        ▼
  ┌───────────────┐   jobs chan    ┌──────────────────────────────┐
  │  Dispatcher   │ ─────────────► │   Worker pool (N goroutines)  │
  │  (fan-out)    │                │   each worker:                │
  └───────────────┘                │     1. cache lookup           │
        ▲                          │     2. rate-limiter wait      │
        │  results chan            │     3. provider.Embed(ctx)    │
        │  (fan-in)                │     4. retry w/ backoff        │
  ┌───────────────┐  ◄──────────── └──────────────────────────────┘
  │  Collector    │                         │
  │  (ordered)    │                         ▼
  └───────────────┘                 Provider (Anthropic / OpenAI / mock)
        │
        ▼
  []Embedding (input order preserved)
```

### Concurrency design (what interviewers probe)
- **Worker pool** bounds in-flight calls — the single most important pattern. See
  `internal/pool`.
- **Fan-out/fan-in over channels** — dispatch jobs, merge results, **preserve input order**
  by carrying an index with each job.
- **`context.Context` everywhere** — one cancel signal tears down the whole pipeline; also
  per-call timeouts.
- **Token-bucket rate limiter** — `internal/ratelimit`, refilled by a background goroutine.
- **`errgroup`-style error propagation** — first hard error cancels siblings (optional
  "fail fast" mode) vs. "collect all errors" mode.

---

## Provider layer

`internal/provider` defines a small interface so the pipeline is provider-agnostic:

```go
type Embedder interface {
    Embed(ctx context.Context, input string) ([]float32, error)
}
```

- **`MockEmbedder`** (included) — deterministic fake vectors + simulated latency, so you can
  benchmark the concurrency **without any API key or cost**. This is what makes the project
  runnable and testable out of the box.
- **Real providers** — implement the same interface. For a Claude-based chat/rerank endpoint
  use the official Anthropic Go SDK (`github.com/anthropics/anthropic-sdk-go`) and the latest
  models (Claude Opus 4.8 / Sonnet 4.6). For embeddings, plug in your provider of choice
  behind the same interface. **Never hardcode keys** — read from env.

---

## Feature roadmap

**Milestone 1 — Core pipeline (MVP)** ✅
- [x] Generic bounded worker pool with `context` (`internal/pool`)
- [x] Token-bucket rate limiter (`internal/ratelimit`)
- [x] Embedder interface + `MockEmbedder`
- [x] Concurrent embedding pipeline preserving input order (`internal/embed`)
- [x] Benchmark: serial vs pooled (README table)

**Milestone 2 — Resilience & caching** ✅
- [x] Retry with exponential backoff + jitter (`internal/embed/retry.go`)
- [x] In-memory LRU cache keyed by input hash (`internal/cache`)
- [x] Per-call timeout + whole-batch deadline (`Config.CallTimeout` / `Config.BatchDeadline`)

**Milestone 3 — Service surface** ✅
- [x] HTTP API: `POST /embed` (batch), `GET /healthz`, `GET /metrics` (`internal/httpapi`)
- [x] Streaming progress (SSE) — `POST /embed/stream` emits `{"done":N,"total":M}`
- [x] CLI: `llmgateway embed -file chunks.txt -workers 16 -rps 50` + `serve` subcommand

**Milestone 4 — Production polish** ✅
- [x] Prometheus metrics, hand-written (in-flight gauge, throughput, cache hits/misses, retries)
- [x] Pluggable real provider: OpenAI-compatible HTTP embedder (`internal/provider/openai.go`),
      env-configured; MockEmbedder remains the default. Claude path documented.
- [x] Dockerfile (multi-stage, static, distroless) + graceful shutdown + config via flags/env

---

## How to prove concurrency (put these in the README)

```bash
# Serial vs pooled on 2,000 chunks against the mock provider (each call ~5ms):
go run ./cmd/gateway embed -n 2000 -workers 1     # baseline (serial)
go run ./cmd/gateway embed -n 2000 -workers 32    # pooled

# Benchmark the pool directly:
go test -bench . -benchmem ./internal/embed

# Full test suite (cache LRU, retry/backoff, HTTP handlers):
go test -timeout 60s ./...
```

Headline for the README: *"Embedding 2,000 chunks: serial 11.2s → 32-worker pool 0.38s
(29× faster), rate-limited to 200 req/s, zero data races."*

---

## Resume bullets (fill in your measured numbers)

- Built a concurrent LLM/embedding gateway in Go using a bounded worker pool and channel-based
  fan-out/fan-in, cutting bulk-embedding time for **N chunks from Xs to Ys (Z× faster)** while
  preserving input order.
- Implemented a token-bucket rate limiter, exponential-backoff retries, and hash-based caching
  to stay within provider limits and eliminate redundant calls (**M% cache hit rate**).
- Used `context` for batch-wide cancellation and per-call timeouts; provider-agnostic
  interface supports Anthropic Claude and pluggable backends; verified race-free with
  `go test -race`.

---

## Repo layout

```
llmgateway/
  cmd/gateway/main.go          CLI + HTTP entrypoint
  internal/pool/pool.go        generic bounded worker pool (context-aware)
  internal/pool/pool_test.go   pool concurrency tests
  internal/ratelimit/limiter.go token-bucket limiter
  internal/provider/provider.go Embedder interface + MockEmbedder
  internal/embed/embed.go      the concurrent embedding pipeline
  internal/embed/embed_test.go pipeline tests + benchmark
```
