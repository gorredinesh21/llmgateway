package cache

import (
	"strconv"
	"sync"
	"testing"
)

// TestLRUHitMiss checks basic get/put and hit/miss accounting.
func TestLRUHitMiss(t *testing.T) {
	c := NewLRU(4)

	if _, ok := c.Get("a"); ok {
		t.Fatal("empty cache should miss")
	}
	c.Put("a", []float32{1, 2, 3})
	got, ok := c.Get("a")
	if !ok || got[0] != 1 {
		t.Fatalf("expected hit with value, got %v ok=%v", got, ok)
	}

	hits, misses := c.Stats()
	if hits != 1 || misses != 1 {
		t.Fatalf("stats: hits=%d misses=%d want 1,1", hits, misses)
	}
}

// TestLRUEviction verifies the least-recently-used entry is evicted when the
// cache is full, and that touching an entry protects it from eviction.
func TestLRUEviction(t *testing.T) {
	c := NewLRU(3)
	c.Put("a", []float32{1})
	c.Put("b", []float32{2})
	c.Put("c", []float32{3})

	// Touch "a" so it becomes most-recently-used; "b" is now the LRU.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should be present")
	}

	// Inserting "d" overflows capacity -> evicts "b" (least recently used).
	c.Put("d", []float32{4})

	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("%s should still be present", k)
		}
	}
	if c.Len() != 3 {
		t.Fatalf("len=%d want 3", c.Len())
	}
}

// TestLRUDisabled checks that a zero/negative capacity disables caching.
func TestLRUDisabled(t *testing.T) {
	c := NewLRU(0)
	c.Put("a", []float32{1})
	if _, ok := c.Get("a"); ok {
		t.Fatal("disabled cache should never hit")
	}
	if c.Len() != 0 {
		t.Fatalf("disabled cache len=%d want 0", c.Len())
	}
}

// TestLRUConcurrent hammers the cache from many goroutines to shake out races
// (the mutex must serialize map + list access).
func TestLRUConcurrent(t *testing.T) {
	c := NewLRU(64)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := strconv.Itoa((g*500 + i) % 128)
				if _, ok := c.Get(k); !ok {
					c.Put(k, []float32{float32(i)})
				}
			}
		}(g)
	}
	wg.Wait()
	if c.Len() > 64 {
		t.Fatalf("len=%d exceeds capacity 64", c.Len())
	}
}

// TestKeyDeterministic verifies identical inputs hash to the same key and
// different inputs differ.
func TestKeyDeterministic(t *testing.T) {
	if Key("hello") != Key("hello") {
		t.Fatal("same input should hash identically")
	}
	if Key("hello") == Key("world") {
		t.Fatal("different inputs should hash differently")
	}
}
