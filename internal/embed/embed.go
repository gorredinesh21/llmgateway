// Package embed is the concurrent embedding pipeline: it takes a batch of texts
// and returns their embeddings in order, using a bounded worker pool and a rate
// limiter, cancellable as a whole via context.
//
// On top of the raw fan-out/fan-in it layers three resilience features:
//
//   - a hash-keyed LRU cache so identical inputs skip the provider (internal/cache),
//   - retry-with-exponential-backoff + jitter around each provider call (retry.go),
//   - per-call timeouts and an optional whole-batch deadline via context.
package embed

import (
	"context"
	"time"

	"github.com/gorredinesh21/llmgateway/internal/cache"
	"github.com/gorredinesh21/llmgateway/internal/pool"
	"github.com/gorredinesh21/llmgateway/internal/provider"
	"github.com/gorredinesh21/llmgateway/internal/ratelimit"
)

// Pipeline embeds batches of text concurrently.
type Pipeline struct {
	emb      provider.Embedder
	limiter  *ratelimit.Limiter
	cache    *cache.LRU
	workers  int
	retry    RetryConfig
	callTO   time.Duration // per-call timeout (0 = none)
	batchTO  time.Duration // whole-batch deadline (0 = none)
	rstats   retryStats
}

// Config configures a Pipeline.
type Config struct {
	Workers int     // max concurrent provider calls
	RPS     float64 // provider rate limit (0 = unlimited)
	Burst   int     // token-bucket burst size

	CacheSize   int           // LRU capacity (0 = caching disabled)
	Retry       RetryConfig   // retry/backoff policy (zero value = try once)
	CallTimeout time.Duration // per-provider-call timeout (0 = none)
	BatchDeadline time.Duration // deadline for the whole batch (0 = none)
}

// New builds a Pipeline. Call Close to release the rate limiter's goroutine.
func New(emb provider.Embedder, cfg Config) *Pipeline {
	if cfg.Workers < 1 {
		cfg.Workers = 8
	}
	if cfg.Burst < 1 {
		cfg.Burst = cfg.Workers
	}
	return &Pipeline{
		emb:     emb,
		limiter: ratelimit.New(cfg.RPS, cfg.Burst),
		cache:   cache.NewLRU(cfg.CacheSize),
		workers: cfg.Workers,
		retry:   cfg.Retry,
		callTO:  cfg.CallTimeout,
		batchTO: cfg.BatchDeadline,
	}
}

func (p *Pipeline) Close() { p.limiter.Close() }

// Cache exposes the underlying cache so callers (e.g. the HTTP metrics handler)
// can read hit/miss stats.
func (p *Pipeline) Cache() *cache.LRU { return p.cache }

// Retries returns the cumulative number of retry attempts performed so far.
func (p *Pipeline) Retries() int64 { return p.rstats.retries.Load() }

// Embedding is one result: the original text, its vector, whether it came from
// the cache, and any error.
type Embedding struct {
	Input  string
	Vector []float32
	Cached bool
	Err    error
}

// Stats summarizes a single batch.
type Stats struct {
	Total     int
	OK        int
	Failed    int
	CacheHits int
	Elapsed   time.Duration
}

// EmbedBatch embeds every input concurrently (bounded by the worker count and
// rate limit) and returns results in input order plus batch stats. Cancelling
// ctx stops the whole batch; unfinished items come back with a context error.
//
// The per-item flow inside a worker is: cache lookup → rate-limiter wait →
// provider call wrapped in retry+timeout → cache populate.
func (p *Pipeline) EmbedBatch(ctx context.Context, inputs []string) ([]Embedding, Stats) {
	start := time.Now()

	// Apply an optional whole-batch deadline. Every worker shares this context,
	// so once it fires the entire batch tears down together.
	if p.batchTO > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.batchTO)
		defer cancel()
	}

	work := func(ctx context.Context, in string) (embedResult, error) {
		key := cache.Key(in)

		// 1. Cache lookup — skip the network on a hit.
		if vec, ok := p.cache.Get(key); ok {
			return embedResult{vector: vec, cached: true}, nil
		}

		// 2. Respect the provider's rate limit before making the call.
		if err := p.limiter.Wait(ctx); err != nil {
			return embedResult{}, err
		}

		// 3. Provider call wrapped in retry-with-backoff and a per-call timeout.
		vec, err := withRetry(ctx, p.retry, &p.rstats, func(ctx context.Context) ([]float32, error) {
			callCtx := ctx
			if p.callTO > 0 {
				var cancel context.CancelFunc
				callCtx, cancel = context.WithTimeout(ctx, p.callTO)
				defer cancel()
			}
			return p.emb.Embed(callCtx, in)
		})
		if err != nil {
			return embedResult{}, err
		}

		// 4. Populate the cache for next time.
		p.cache.Put(key, vec)
		return embedResult{vector: vec}, nil
	}

	results := pool.Map(ctx, p.workers, inputs, work)

	out := make([]Embedding, len(results))
	st := Stats{Total: len(inputs)}
	for i, r := range results {
		out[i] = Embedding{
			Input:  inputs[i],
			Vector: r.Value.vector,
			Cached: r.Value.cached,
			Err:    r.Err,
		}
		if r.Err != nil {
			st.Failed++
		} else {
			st.OK++
			if r.Value.cached {
				st.CacheHits++
			}
		}
	}
	st.Elapsed = time.Since(start)
	return out, st
}

// embedResult is the internal per-item output carried through the pool: the
// vector plus whether it was a cache hit.
type embedResult struct {
	vector []float32
	cached bool
}
