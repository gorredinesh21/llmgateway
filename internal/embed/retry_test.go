package embed

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// flakyEmbedder fails the first `failN` calls with a transient error, then
// succeeds. It counts total calls so tests can assert how many attempts ran.
type flakyEmbedder struct {
	failN int32
	calls atomic.Int32
}

func (f *flakyEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	n := f.calls.Add(1)
	if n <= f.failN {
		return nil, errors.New("transient boom")
	}
	return []float32{1, 2, 3}, nil
}

// TestRetryEventuallySucceeds checks that a call failing twice then succeeding
// is retried and ultimately produces a vector, and that the retry counter
// records exactly the number of retries performed.
func TestRetryEventuallySucceeds(t *testing.T) {
	flaky := &flakyEmbedder{failN: 2}
	p := New(flaky, Config{
		Workers: 1,
		Retry:   RetryConfig{MaxRetries: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
	})
	defer p.Close()

	out, st := p.EmbedBatch(context.Background(), []string{"hello"})
	if st.Failed != 0 || st.OK != 1 {
		t.Fatalf("stats: ok=%d failed=%d want 1,0", st.OK, st.Failed)
	}
	if out[0].Err != nil {
		t.Fatalf("unexpected error: %v", out[0].Err)
	}
	if flaky.calls.Load() != 3 { // 1 initial + 2 retries
		t.Fatalf("provider called %d times, want 3", flaky.calls.Load())
	}
	if p.Retries() != 2 {
		t.Fatalf("retries counted %d, want 2", p.Retries())
	}
}

// TestRetryExhausted verifies that if failures exceed MaxRetries the item ends
// in error and retries are still counted.
func TestRetryExhausted(t *testing.T) {
	flaky := &flakyEmbedder{failN: 100} // always fails
	p := New(flaky, Config{
		Workers: 1,
		Retry:   RetryConfig{MaxRetries: 2, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
	})
	defer p.Close()

	out, st := p.EmbedBatch(context.Background(), []string{"hello"})
	if st.Failed != 1 {
		t.Fatalf("expected 1 failure, got %d", st.Failed)
	}
	if out[0].Err == nil {
		t.Fatal("expected an error after exhausting retries")
	}
	if p.Retries() != 2 {
		t.Fatalf("retries counted %d, want 2", p.Retries())
	}
}

// TestPermanentErrorNotRetried verifies ErrPermanent short-circuits retries.
type permEmbedder struct{ calls atomic.Int32 }

func (p *permEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	p.calls.Add(1)
	return nil, errors.Join(errors.New("bad request"), ErrPermanent)
}

func TestPermanentErrorNotRetried(t *testing.T) {
	pe := &permEmbedder{}
	p := New(pe, Config{Workers: 1, Retry: RetryConfig{MaxRetries: 5, BaseDelay: time.Millisecond}})
	defer p.Close()

	out, _ := p.EmbedBatch(context.Background(), []string{"x"})
	if out[0].Err == nil {
		t.Fatal("expected permanent error")
	}
	if pe.calls.Load() != 1 {
		t.Fatalf("permanent error retried: called %d times, want 1", pe.calls.Load())
	}
}

// TestCacheSkipsProvider verifies a second identical input hits the cache and
// does not call the provider again.
func TestCacheSkipsProvider(t *testing.T) {
	flaky := &flakyEmbedder{failN: 0} // always succeeds, counts calls
	p := New(flaky, Config{Workers: 4, CacheSize: 128})
	defer p.Close()

	// Same input three times.
	out, st := p.EmbedBatch(context.Background(), []string{"dup", "dup", "dup"})
	if st.OK != 3 {
		t.Fatalf("ok=%d want 3", st.OK)
	}
	// With a warm cache, exactly one provider call should have happened for the
	// unique input (the other two are cache hits). Note: within a single batch
	// the three may race, so allow <=3 but assert at least one was cached over
	// two batches below.
	_ = out

	// Second batch of the same input: now the cache is definitely warm.
	callsAfterFirst := flaky.calls.Load()
	_, st2 := p.EmbedBatch(context.Background(), []string{"dup"})
	if st2.CacheHits != 1 {
		t.Fatalf("second batch cache hits=%d want 1", st2.CacheHits)
	}
	if flaky.calls.Load() != callsAfterFirst {
		t.Fatal("provider called again despite warm cache")
	}
}
