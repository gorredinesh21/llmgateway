// Command gateway serves the llmgateway HTTP API: a batch embedding endpoint,
// health check, Prometheus metrics and an SSE progress stream.
//
// Runs with a zero-cost mock provider out of the box (demoable with no API
// key); set -provider openai (plus EMBED_* env vars) for a real backend.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorredinesh21/llmgateway/internal/embed"
	"github.com/gorredinesh21/llmgateway/internal/httpapi"
	"github.com/gorredinesh21/llmgateway/internal/provider"
)

func main() {
	var (
		addr         = flag.String("addr", ":8080", "listen address")
		workers      = flag.Int("workers", 16, "max concurrent provider calls")
		rps          = flag.Float64("rps", 0, "provider rate limit (0 = unlimited)")
		cacheSize    = flag.Int("cache-size", 1024, "LRU cache capacity (0 = off)")
		maxRetries   = flag.Int("max-retries", 3, "extra attempts after the first")
		callTimeout  = flag.Duration("call-timeout", 15*time.Second, "per-provider-call timeout")
		providerName = flag.String("provider", "mock", "embedding provider: mock | openai")
		mockDim      = flag.Int("mock-dim", 8, "mock provider vector dimension")
		mockLatency = flag.Duration("mock-latency", 5*time.Millisecond, "simulated per-call latency")
	)
	flag.Parse()

	var emb provider.Embedder
	switch *providerName {
	case "mock":
		emb = provider.NewMock(*mockDim, *mockLatency)
	case "openai":
		real, err := provider.NewOpenAIFromEnv()
		if err != nil {
			log.Fatalf("openai provider: %v", err)
		}
		emb = real
	default:
		log.Fatalf("unknown provider %q (want mock|openai)", *providerName)
	}

	pipe := embed.New(emb, embed.Config{
		Workers:      *workers,
		RPS:          *rps,
		Burst:        *workers,
		CacheSize:    *cacheSize,
		Retry:        embed.RetryConfig{MaxRetries: *maxRetries, BaseDelay: 50 * time.Millisecond, MaxDelay: 2 * time.Second},
		CallTimeout:  *callTimeout,
	})
	defer pipe.Close()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(pipe).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Graceful shutdown on SIGTERM/SIGINT (Cloud Run sends SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("llmgateway listening on %s (provider=%s workers=%d cache=%d retries=%d)",
		*addr, *providerName, *workers, *cacheSize, *maxRetries)

	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown: %v", err)
		}
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}
}
