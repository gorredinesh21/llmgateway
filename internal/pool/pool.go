// Package pool implements a generic, context-aware bounded worker pool.
//
// It is the heart of the gateway: given a slice of inputs and a worker function,
// it runs at most `workers` invocations concurrently, preserves input order in
// the results, and stops everything promptly if the context is cancelled.
//
// Uses Go generics so it can process any input/output types, not just embeddings.
package pool

import (
	"context"
	"sync"
)

// Result pairs an output (or error) with the index of its input, so callers can
// reassemble results in the original order regardless of completion order.
type Result[O any] struct {
	Index int
	Value O
	Err   error
}

// WorkFn transforms a single input into an output. It must respect ctx and
// return promptly when ctx is done.
type WorkFn[I, O any] func(ctx context.Context, in I) (O, error)

// Map runs fn over inputs using at most `workers` concurrent goroutines. Results
// are returned in the same order as inputs. If ctx is cancelled, remaining work
// is skipped and those slots carry ctx.Err().
//
// This is the classic fan-out (dispatch jobs) / fan-in (collect results) pattern
// with bounded concurrency — the pattern every backend interviewer wants to see.
func Map[I, O any](ctx context.Context, workers int, inputs []I, fn WorkFn[I, O]) []Result[O] {
	if workers < 1 {
		workers = 1
	}
	results := make([]Result[O], len(inputs))

	type job struct {
		idx int
		in  I
	}
	jobs := make(chan job)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for j := range jobs {
				// Bail out fast if the batch was cancelled.
				if err := ctx.Err(); err != nil {
					results[j.idx] = Result[O]{Index: j.idx, Err: err}
					continue
				}
				val, err := fn(ctx, j.in)
				results[j.idx] = Result[O]{Index: j.idx, Value: val, Err: err}
			}
		}()
	}

	// Track which indices were actually dispatched. Anything not dispatched
	// (because the context was cancelled mid-run) is marked cancelled below.
	dispatched := make([]bool, len(inputs))
	go func() {
		defer close(jobs)
		for i, in := range inputs {
			select {
			case <-ctx.Done():
				return
			case jobs <- job{idx: i, in: in}:
				dispatched[i] = true
			}
		}
	}()

	wg.Wait()

	// Slots that were never dispatched carry the cancellation error so callers
	// see a complete, ordered result set with no silent gaps.
	if err := ctx.Err(); err != nil {
		for i := range results {
			if !dispatched[i] {
				results[i] = Result[O]{Index: i, Err: err}
			}
		}
	}
	return results
}
