# llmgateway

A **concurrent LLM / embedding gateway** in Go. It embeds large batches of text fast and
safely: a bounded worker pool keeps many provider calls in flight, a token-bucket limiter
respects rate limits, and `context` cancels the whole batch cleanly. Provider-agnostic, with
a zero-cost mock backend so it runs out of the box.

## Quick start (no API key needed)

```bash
# serial baseline vs pooled — watch the time drop:
go run ./cmd/gateway embed -n 2000 -workers 1     # ~ n × latency
go run ./cmd/gateway embed -n 2000 -workers 32    # bounded-concurrent

# add a rate limit (requests/sec):
go run ./cmd/gateway embed -n 2000 -workers 32 -rps 200
```

## Prove the concurrency

```bash
go test -bench BenchmarkPoolSpeedup -benchtime 3x ./internal/embed   # 1 vs 32 workers
go test -timeout 60s ./...                                           # full suite
```

Measured on a Ryzen 5 Pro 7535U (12 threads), mock provider @ 5 ms/call:

> **Embedding 2,000 chunks: serial 10.69s (187/sec) → 32-worker pool 0.33s (6023/sec) — ~32× faster.**

Pool micro-benchmark (500 chunks), near-linear scaling:

| Workers | Time | Speedup |
|--------:|-----:|--------:|
| 1  | 1128 ms | 1× |
| 4  | 285 ms | ~4× |
| 16 | 70 ms | ~16× |
| 32 | 36 ms | ~31× |

## Design

- **Generic bounded worker pool** (`internal/pool`) — fan-out/fan-in over channels,
  `context`-aware, preserves input order.
- **Token-bucket rate limiter** (`internal/ratelimit`) — background refill goroutine.
- **Provider interface** (`internal/provider`) — `MockEmbedder` included; drop in the
  Anthropic Go SDK (Claude Opus 4.8 / Sonnet 4.6) or any backend behind the same interface.
- **Pipeline** (`internal/embed`) — glues them together into `EmbedBatch(ctx, inputs)`.

- **Resilience** — every provider call is wrapped in retry-with-exponential-backoff + full
  jitter (`internal/embed/retry.go`), a per-call timeout, and an optional whole-batch
  deadline. A hash-keyed LRU cache (`internal/cache`) skips the provider for repeated inputs.

See [SPEC.md](SPEC.md) for architecture, roadmap, and resume bullets.

## HTTP API

Start the server (mock provider, no key needed):

```bash
go run ./cmd/gateway serve -addr :8080 -workers 16 -cache-size 1024 -max-retries 3
# flags: -addr -workers -rps -cache-size -max-retries -provider mock|openai -call-timeout
```

On Windows a `:8080` bind may be IPv6 — connect via `http://localhost:8080` or `http://[::1]:8080`.

### `GET /healthz` — liveness

```bash
curl http://localhost:8080/healthz
# {"status":"ok"}
```

### `POST /embed` — batch embed

Cancels the whole batch if the client disconnects (`r.Context()`).

```bash
curl -s http://localhost:8080/embed \
  -H 'Content-Type: application/json' \
  -d '{"inputs":["hello world","hello world","goodbye"],"workers":16,"rps":0}'
```
```json
{"results":[{"input":"hello world","vector":[0.72,0.57,...],"cached":false}, ...],
 "total":3,"ok":3,"failed":0,"cache_hits":0,"elapsed_ms":6.1}
```

Re-issuing the same inputs returns `"cached":true` and bumps `cache_hits` (the vectors are
served from the LRU with no provider call).

### `POST /embed/stream` — SSE progress

Emits incremental progress events then a final result event:

```bash
curl -N http://localhost:8080/embed/stream \
  -H 'Content-Type: application/json' \
  -d '{"inputs":["a","b","c","d","e","f","g","h","i","j"]}'
```
```
event: progress
data: {"done":8,"total":10}

event: progress
data: {"done":10,"total":10}

event: result
data: {"results":[...],"total":10,"ok":10,"failed":0,"cache_hits":0,"elapsed_ms":12.0}
```

### `GET /metrics` — Prometheus (hand-written, no client_golang)

```bash
curl http://localhost:8080/metrics
```
```
# TYPE llmgateway_requests_total counter
llmgateway_requests_total 1
# TYPE llmgateway_embeddings_total counter
llmgateway_embeddings_total 3
# TYPE llmgateway_workers_in_flight gauge
llmgateway_workers_in_flight 0
llmgateway_cache_hits_total 0
llmgateway_cache_misses_total 3
llmgateway_retries_total 0
llmgateway_request_duration_seconds_total 0.0061
```

The server shuts down gracefully on SIGINT/SIGTERM (drains in-flight requests via
`http.Server.Shutdown`).

## Real provider (OpenAI-compatible)

Point the gateway at any OpenAI-compatible embeddings endpoint (OpenAI, Azure, Together,
vLLM, Ollama's `/v1`, or a LiteLLM proxy) via environment variables — **secrets are never
hardcoded**:

```bash
export EMBED_BASE_URL="https://api.openai.com/v1"
export EMBED_API_KEY="sk-..."
export EMBED_MODEL="text-embedding-3-small"
go run ./cmd/gateway serve -provider openai
```

It calls `POST {EMBED_BASE_URL}/embeddings` with `{model, input}` and parses
`data[].embedding`. **Claude:** Anthropic's API is not OpenAI-shaped and exposes messages
rather than a public embeddings endpoint — front it with an OpenAI-compatible proxy (e.g.
LiteLLM) and set `EMBED_BASE_URL` to that, or add a sibling `ClaudeEmbedder` implementing the
same `Embed(ctx, input)` interface. The pipeline code stays unchanged either way.

If `EMBED_BASE_URL` is unset, `serve` falls back to the zero-cost mock provider.

## CLI

```bash
go run ./cmd/gateway embed -n 2000 -workers 32          # synthetic chunks
go run ./cmd/gateway embed -file chunks.txt -workers 16 # one embedding per non-empty line
go run ./cmd/gateway serve  -addr :8080                 # HTTP API
```

## Docker

```bash
docker build -t llmgateway:latest .          # multi-stage, static CGO_ENABLED=0, distroless
docker run --rm -p 8080:8080 llmgateway      # serves the API on :8080
docker run --rm llmgateway embed -n 2000 -workers 32   # CLI demo
```

## Make targets

`make build | test | vet | run | serve | bench | docker | clean` (Unix), or on the portable
Windows Go SDK: `.\make.ps1 <target>`.
