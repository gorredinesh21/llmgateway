// Package cache is a small, concurrency-safe in-memory LRU cache for embeddings.
//
// The pipeline consults it before calling the provider: identical input text
// (matched by a sha256 hash of the bytes) returns a cached vector and skips the
// network entirely. That is a big win for RAG workloads where the same chunk
// (boilerplate, headers, repeated FAQ text) shows up many times.
//
// LRU = Least Recently Used. When the cache is full, the entry that has gone
// unused the longest is evicted to make room. We implement this with the two
// classic building blocks, both from the standard library:
//
//   - a map for O(1) lookup by key, and
//   - a container/list doubly-linked list to track recency order cheaply.
//
// Every access moves the touched entry to the front of the list; eviction pops
// from the back. A single sync.Mutex guards both structures so concurrent
// workers can share one cache safely.
package cache

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"
)

// Key returns the cache key for a piece of input text: the hex-encoded sha256
// of its bytes. Hashing means the key is fixed-size regardless of input length,
// and identical inputs always collapse to the same key.
func Key(input string) string {
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// entry is what we store in each list element. We keep the key alongside the
// value so that when we evict from the back of the list we know which map entry
// to delete.
type entry struct {
	key    string
	vector []float32
}

// LRU is a fixed-capacity, thread-safe least-recently-used cache mapping a
// string key to an embedding vector.
type LRU struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List               // front = most recently used, back = least
	items    map[string]*list.Element // key -> node in ll

	// Counters are read via /metrics from other goroutines, so use atomics to
	// avoid needing the mutex just to read a stat.
	hits   atomic.Int64
	misses atomic.Int64
}

// NewLRU builds a cache holding up to `capacity` entries. A capacity <= 0
// disables caching: Get always misses and Put is a no-op. That keeps callers
// simple — they can always hold an *LRU and never nil-check.
func NewLRU(capacity int) *LRU {
	return &LRU{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

// Get returns the cached vector for key and whether it was present. A hit moves
// the entry to the front (most-recently-used); both hits and misses are counted.
func (c *LRU) Get(key string) ([]float32, bool) {
	if c.capacity <= 0 {
		c.misses.Add(1)
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el) // mark as recently used
		c.hits.Add(1)
		return el.Value.(*entry).vector, true
	}
	c.misses.Add(1)
	return nil, false
}

// Put inserts or updates key -> vector, marking it most-recently-used. If the
// cache is over capacity afterwards, the least-recently-used entry is evicted.
func (c *LRU) Put(key string, vector []float32) {
	if c.capacity <= 0 {
		return // caching disabled
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Update in place if the key already exists.
	if el, ok := c.items[key]; ok {
		el.Value.(*entry).vector = vector
		c.ll.MoveToFront(el)
		return
	}

	// Insert a fresh entry at the front.
	el := c.ll.PushFront(&entry{key: key, vector: vector})
	c.items[key] = el

	// Evict from the back while over capacity (normally just one entry).
	for c.ll.Len() > c.capacity {
		oldest := c.ll.Back()
		if oldest == nil {
			break
		}
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*entry).key)
	}
}

// Len returns the current number of cached entries (mostly for tests).
func (c *LRU) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Stats returns cumulative hit/miss counts. Safe to call concurrently.
func (c *LRU) Stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}
