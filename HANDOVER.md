# llmgateway — Handover

A working-status handover for anyone picking this project up (including future me). It
explains the goal, what is done, **what is written but not yet verified**, how to run it, and
what remains.

> ⚠️ **Read section 3 first.** The core pipeline is built and verified. The service layer
> (HTTP API, cache, retry, real provider) was written but **not yet build/test-verified** —
> verifying it is the very first task.

---

## 1. What we set out to build

A **concurrent LLM / embedding gateway in Go** — infrastructure that makes bulk AI workloads
fast and safe. The motivating problem: turning a large batch of text (e.g. 10,000 document
chunks for a RAG index) into embeddings is painfully slow if done one call at a time. This
service does it with **bounded concurrency**, so many provider calls run at once without
overwhelming the provider.

Chosen as a portfolio project because it pairs GenAI/RAG domain knowledge with real Go
concurrency engineering — the "plumbing around ML" that Go excels at.

Design goals:
1. **Bounded worker pool** — many calls in flight, but never a thundering herd.
2. **Rate limiting, retries, caching** — production-grade resilience.
3. **Cancellable** — one `context` cancel tears down the whole batch.
4. **Measurable speedup** — serial vs pooled, with real numbers.
5. **Runs with no API key** — a mock provider makes it demoable and testable out of the box.

---

## 2. What we built and verified (✅)

The **core pipeline** is complete, compiles, passes tests, and was benchmarked live:

- **Generic bounded worker pool** (`internal/pool`) — fan-out/fan-in over channels,
  `context`-aware, **preserves input order**. Uses Go generics so it works for any
  input/output type.
- **Token-bucket rate limiter** (`internal/ratelimit`) — background refill goroutine;
  `rps <= 0` means unlimited (returns immediately). *(Note: an earlier deadlock where
  "unlimited" starved after the initial burst was found and fixed — see section 5.)*
- **Provider interface + MockEmbedder** (`internal/provider`) — deterministic fake vectors
  with simulated latency, so the whole system runs without any API key or cost.
- **Embedding pipeline** (`internal/embed`) — glues the above into `EmbedBatch(ctx, inputs)`.
- **CLI demo** (`cmd/gateway embed`) — serial vs pooled.

**Verified numbers (mock provider @ 5 ms/call, Ryzen 5 Pro 7535U):**
- Embedding 2,000 chunks: **serial 10.69s (187/sec) → 32-worker pool 0.33s (6023/sec), ~32×**.
- Pool micro-benchmark shows near-linear scaling: 1→4→16→32 workers ≈ 1×→4×→16×→31×.

---

## 3. What is written but NOT yet verified (⚠️ verify first)

The following milestone-2/3/4 features were added afterward. **The build/test/smoke-test
verification pass was interrupted before it completed**, so this code should be treated as
*code-complete but unverified* — it may need small fixes before it compiles/runs cleanly.

- **Resilience** (`internal/embed`) — retry with exponential backoff + jitter, per-call
  timeout, optional whole-batch deadline.
- **Cache** (`internal/cache`) — hash-keyed (SHA-256) LRU so repeated inputs skip the
  provider; tracks hits/misses.
- **Real provider** (`internal/provider`) — OpenAI-compatible HTTP embedder, configured via
  env vars `EMBED_BASE_URL`, `EMBED_API_KEY`, `EMBED_MODEL`. Mock stays the default.
- **HTTP API** (`internal/httpapi`) — `POST /embed`, `POST /embed/stream` (SSE progress),
  `GET /healthz`, `GET /metrics` (hand-written Prometheus text; no external client library).
- **`serve` subcommand** (`cmd/gateway`) — starts the HTTP API with graceful shutdown.
- Updated `Dockerfile`, `Makefile`/`make.ps1`, `README.md`, `SPEC.md`.

### ▶ First task for whoever picks this up
Run, in order, and fix anything that fails:
```bash
go build ./...
go vet ./...
go test ./...
# then a live smoke test of the service:
go run ./cmd/gateway serve -addr :8080
#   GET  http://localhost:8080/healthz
#   GET  http://localhost:8080/metrics
#   POST http://localhost:8080/embed   with {"inputs":["hello","hello","world"]}
```
Watch specifically for: the rate-limiter "unlimited" path (must not block), SSE flushing, and
the cache's concurrency safety.

---

## 4. How to build & run

Requires the Go toolchain (Go 1.24+). Everything is **pure Go standard library — no external
dependencies** (deliberate: keeps it dependency-free and buildable offline).

```bash
# CLI demo (verified):
go run ./cmd/gateway embed -n 2000 -workers 32
go run ./cmd/gateway embed -file chunks.txt -workers 16

# HTTP service (needs verification per section 3):
go run ./cmd/gateway serve -addr :8080 -workers 16 -cache-size 1024 -max-retries 3

# Real provider (any OpenAI-compatible embeddings endpoint):
export EMBED_BASE_URL="https://api.openai.com/v1"
export EMBED_API_KEY="..."          # never hardcode; read from env
export EMBED_MODEL="text-embedding-3-small"
go run ./cmd/gateway serve -provider openai
```

**Note:** on Windows a `:8080` bind may be IPv6 — use `http://localhost:8080` or `[::1]`.

---

## 5. What is pending / next steps

1. **Verify the section-3 code** (highest priority — it's the difference between "MVP" and
   "product").
2. **Harden the real provider** — request batching, respect provider rate-limit headers,
   handle partial failures per item.
3. **A first-class Claude path** — Anthropic's API is message-based, not an OpenAI-style
   embeddings endpoint; either front it with an OpenAI-compatible proxy or add a sibling
   embedder implementing the same `Embed(ctx, input)` interface. The pipeline stays unchanged.
4. **Auth** on the HTTP API (API key / bearer token).
5. **Persistent cache** (currently in-memory only).
6. **Richer metrics** + a small dashboard.
7. **Broader test coverage** for the service layer.

### Known history / gotchas
- A rate-limiter deadlock (unlimited mode starved after the initial burst) was found by
  running the tests and fixed — the fix is in `internal/ratelimit`. Keep the invariant:
  `rps <= 0` ⇒ `Wait` returns immediately.
- A test bug (`TestMapCancel` never triggered its cancel condition) was also fixed.

---

## 6. Repo layout

```
llmgateway/
  cmd/gateway/          CLI (`embed`) + HTTP (`serve`) entrypoint
  internal/pool/        generic bounded worker pool (+ tests)   [verified]
  internal/ratelimit/   token-bucket limiter                    [verified]
  internal/provider/    Embedder interface, MockEmbedder, OpenAI-compatible HTTP provider
  internal/embed/       pipeline: retry, cache, timeouts (+ tests)
  internal/cache/       hash-keyed LRU cache
  internal/httpapi/     HTTP handlers: /embed, /embed/stream, /healthz, /metrics
  Dockerfile, Makefile, make.ps1
  README.md, SPEC.md, HANDOVER.md
```
