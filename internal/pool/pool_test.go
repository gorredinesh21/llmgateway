package pool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestMapOrder verifies output order matches input order even though work
// finishes out of order.
func TestMapOrder(t *testing.T) {
	inputs := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	fn := func(ctx context.Context, n int) (int, error) {
		// Later indices finish sooner, scrambling completion order.
		time.Sleep(time.Duration(10-n) * time.Millisecond)
		return n * n, nil
	}
	res := Map(context.Background(), 4, inputs, fn)
	for i, r := range res {
		if r.Err != nil || r.Value != i*i {
			t.Fatalf("index %d: got %d,%v want %d", i, r.Value, r.Err, i*i)
		}
	}
}

// TestMapBoundsConcurrency verifies no more than `workers` run at once.
func TestMapBoundsConcurrency(t *testing.T) {
	const workers = 5
	var inFlight, maxSeen int32
	fn := func(ctx context.Context, n int) (int, error) {
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxSeen)
			if cur <= m || atomic.CompareAndSwapInt32(&maxSeen, m, cur) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		return n, nil
	}
	inputs := make([]int, 200)
	Map(context.Background(), workers, inputs, fn)
	if maxSeen > workers {
		t.Fatalf("saw %d concurrent workers, want <= %d", maxSeen, workers)
	}
}

// TestMapCancel verifies cancellation short-circuits remaining work.
func TestMapCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fn := func(ctx context.Context, n int) (int, error) {
		if n == 10 {
			cancel() // cancel partway through the batch
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Millisecond):
			return n, nil
		}
	}
	inputs := make([]int, 1000)
	for i := range inputs {
		inputs[i] = i
	}
	res := Map(ctx, 2, inputs, fn)
	cancelled := 0
	for _, r := range res {
		if errors.Is(r.Err, context.Canceled) {
			cancelled++
		}
	}
	if cancelled == 0 {
		t.Fatal("expected some cancelled results")
	}
}
