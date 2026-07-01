package embed

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

// RetryConfig controls the retry-with-exponential-backoff behaviour applied
// around each provider call.
//
// Backoff math: attempt k (0-indexed) waits roughly BaseDelay * 2^k, capped at
// MaxDelay, plus a random "jitter" of up to that same amount. Jitter spreads
// retries out in time so a fleet of workers that all failed at once do not
// retry in lockstep and hammer the provider together (the "thundering herd").
type RetryConfig struct {
	MaxRetries int           // extra attempts after the first (0 = try once)
	BaseDelay  time.Duration // delay before the first retry
	MaxDelay   time.Duration // ceiling on any single backoff wait
}

// DefaultRetry returns sensible defaults: up to 3 retries, 50ms base, 2s cap.
func DefaultRetry() RetryConfig {
	return RetryConfig{MaxRetries: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 2 * time.Second}
}

// transient reports whether an error is worth retrying. Context cancellation /
// deadline are NOT transient — if the caller gave up or the batch deadline
// passed, retrying is pointless and wasteful.
//
// The Embedder interface only returns a plain error, so by default we treat any
// non-context error as transient (a 429/5xx or a dropped connection would land
// here). A provider that wants to mark an error permanent can wrap it with
// ErrPermanent.
func transient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, ErrPermanent) {
		return false
	}
	return true
}

// ErrPermanent wraps an error the caller knows should not be retried (e.g. a
// 400 bad-request or 401 auth failure). Use errors.Join or fmt.Errorf("%w", ...)
// to attach it: fmt.Errorf("bad request: %w", ErrPermanent).
var ErrPermanent = errors.New("permanent error")

// backoff computes the wait before the given (0-indexed) retry attempt, with
// full jitter, clamped to MaxDelay.
func backoff(cfg RetryConfig, attempt int, rng *rand.Rand) time.Duration {
	// Exponential term: BaseDelay * 2^attempt.
	exp := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt))
	if exp > float64(cfg.MaxDelay) {
		exp = float64(cfg.MaxDelay)
	}
	// Full jitter: sleep a random duration in [0, exp]. This is AWS's
	// recommended jitter strategy and avoids synchronized retries.
	return time.Duration(rng.Int63n(int64(exp) + 1))
}

// retryStats counts how many retry attempts (beyond the first call) happened
// across a batch, so the metrics endpoint can report it.
type retryStats struct {
	retries atomic.Int64
}

// withRetry runs fn, retrying transient failures with exponential backoff and
// jitter. It respects ctx between attempts (a cancelled/expired context aborts
// immediately) and increments stats.retries for every retry it performs.
//
// Each call gets its own *rand.Rand seeded from the current time + a per-call
// nonce; math/rand's top-level funcs are safe for concurrency but a local rng
// avoids lock contention when many workers back off at once.
func withRetry[O any](
	ctx context.Context,
	cfg RetryConfig,
	stats *retryStats,
	fn func(context.Context) (O, error),
) (O, error) {
	var zero O
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	var lastErr error
	// attempt 0 is the initial call; attempts 1..MaxRetries are retries.
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		out, err := fn(ctx)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if !transient(err) {
			return zero, err // give up on permanent / context errors
		}
		// Do not sleep after the final attempt.
		if attempt == cfg.MaxRetries {
			break
		}
		stats.retries.Add(1)

		// Sleep for the backoff, but wake early if ctx is cancelled.
		wait := backoff(cfg, attempt, rng)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
	}
	return zero, lastErr
}
