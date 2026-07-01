// Package ratelimit implements a token-bucket rate limiter.
//
// A bucket holds up to `burst` tokens and refills at `rps` tokens/second from a
// background goroutine. Each call to Wait blocks until a token is available (or
// the context is cancelled), smoothing bursty load to a provider's allowed rate.
package ratelimit

import (
	"context"
	"time"
)

type Limiter struct {
	tokens    chan struct{}
	stop      chan struct{}
	unlimited bool
}

// New returns a limiter allowing `rps` requests per second with up to `burst`
// requests in a short spike. Call Close when done to stop the refill goroutine.
//
// rps <= 0 means unlimited: Wait never blocks (aside from honoring ctx). This is
// the common case for the mock provider and local benchmarking.
func New(rps float64, burst int) *Limiter {
	l := &Limiter{stop: make(chan struct{})}
	if rps <= 0 {
		l.unlimited = true
		return l
	}
	if burst < 1 {
		burst = 1
	}
	l.tokens = make(chan struct{}, burst)
	// Start full so an initial burst is allowed.
	for i := 0; i < burst; i++ {
		l.tokens <- struct{}{}
	}
	interval := time.Duration(float64(time.Second) / rps)
	go l.refill(interval)
	return l
}

func (l *Limiter) refill(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			// Add a token if there's room; drop it otherwise (bucket full).
			select {
			case l.tokens <- struct{}{}:
			default:
			}
		}
	}
}

// Wait blocks until a token is available or ctx is cancelled. When the limiter
// is unlimited it returns immediately (still honoring ctx cancellation).
func (l *Limiter) Wait(ctx context.Context) error {
	if l.unlimited {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.tokens:
		return nil
	}
}

// Close stops the background refill goroutine. Safe to call once.
func (l *Limiter) Close() {
	select {
	case <-l.stop:
		// already closed
	default:
		close(l.stop)
	}
}
