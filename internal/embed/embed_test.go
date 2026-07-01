package embed

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/gorredinesh21/llmgateway/internal/provider"
)

func makeInputs(n int) []string {
	in := make([]string, n)
	for i := range in {
		in[i] = "chunk-" + strconv.Itoa(i)
	}
	return in
}

// TestEmbedBatchOrder verifies results come back in input order and the mock is
// deterministic (same input -> same vector).
func TestEmbedBatchOrder(t *testing.T) {
	mock := provider.NewMock(8, 0)
	p := New(mock, Config{Workers: 8})
	defer p.Close()

	inputs := makeInputs(100)
	out, _ := p.EmbedBatch(context.Background(), inputs)
	if len(out) != len(inputs) {
		t.Fatalf("got %d results, want %d", len(out), len(inputs))
	}
	for i, r := range out {
		if r.Err != nil {
			t.Fatalf("index %d: unexpected error %v", i, r.Err)
		}
		if r.Input != inputs[i] {
			t.Fatalf("index %d out of order: got %q want %q", i, r.Input, inputs[i])
		}
		want, _ := mock.Embed(context.Background(), inputs[i])
		if r.Vector[0] != want[0] {
			t.Fatalf("index %d: non-deterministic vector", i)
		}
	}
}

// TestEmbedBatchCancel checks that cancelling the context stops the batch and
// unfinished items report the context error (no silent gaps).
func TestEmbedBatchCancel(t *testing.T) {
	mock := provider.NewMock(8, 50*time.Millisecond) // slow calls
	p := New(mock, Config{Workers: 2})
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	out, _ := p.EmbedBatch(ctx, makeInputs(500))
	errs := 0
	for _, r := range out {
		if r.Err != nil {
			errs++
		}
	}
	if errs == 0 {
		t.Fatal("expected some items to be cancelled")
	}
}

// BenchmarkPoolSpeedup demonstrates the concurrency win. Run:
//
//	go test -bench BenchmarkPoolSpeedup -benchtime 3x ./internal/embed
//
// Compare workers=1 vs workers=32 with a realistic per-call latency.
func BenchmarkPoolSpeedup(b *testing.B) {
	inputs := makeInputs(500)
	for _, workers := range []int{1, 4, 16, 32} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) {
			mock := provider.NewMock(8, 2*time.Millisecond)
			p := New(mock, Config{Workers: workers})
			defer p.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				p.EmbedBatch(context.Background(), inputs)
			}
		})
	}
}
