package resilience

import (
	"iter"
	"sync"
	"time"
)

// Limiter keeps an independent TokenBucket for every key.
type Limiter[K comparable] struct {
	clock    Clock
	capacity int
	interval time.Duration

	mu      sync.Mutex
	buckets map[K]*TokenBucket
}

// NewLimiter reports invalid arguments exactly like NewTokenBucket does.
func NewLimiter[K comparable](clock Clock, capacity int, interval time.Duration) (*Limiter[K], error) {
	if err := validateBucket(clock, capacity, interval); err != nil {
		return nil, err
	}
	return &Limiter[K]{
		clock:    clock,
		capacity: capacity,
		interval: interval,
		buckets:  make(map[K]*TokenBucket),
	}, nil
}

// Allow takes a token from the key's bucket, creating a full bucket on first use.
func (l *Limiter[K]) Allow(key K) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		b = newTokenBucket(l.clock, l.capacity, l.interval)
		l.buckets[key] = b
	}
	return b.Allow()
}

// Do runs op if the key has a token, otherwise returns ErrRateLimited.
func (l *Limiter[K]) Do(key K, op func() error) error {
	if !l.Allow(key) {
		return ErrRateLimited
	}
	return op()
}

// Tokens reports the tokens available for key. An unknown key has a full bucket;
// asking does not create anything.
func (l *Limiter[K]) Tokens(key K) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b, ok := l.buckets[key]; ok {
		return b.Tokens()
	}
	return l.capacity
}

// Size returns the number of keys that currently have a bucket.
func (l *Limiter[K]) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// Sweep deletes the buckets that are full and returns how many were deleted.
// A full bucket is indistinguishable from a new one, so Sweep never changes
// the answers of Allow, Do and Tokens.
func (l *Limiter[K]) Sweep() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	removed := 0
	for key, b := range l.buckets {
		if b.Tokens() == l.capacity {
			delete(l.buckets, key)
			removed++
		}
	}
	return removed
}

// Idle iterates over the keys whose buckets are full, i.e. the keys Sweep would delete.
// The order is unspecified. The limiter is not locked while the loop body runs,
// so the body may freely call the limiter's methods.
func (l *Limiter[K]) Idle() iter.Seq[K] {
	return func(yield func(K) bool) {
		l.mu.Lock()
		var keys []K
		for key, b := range l.buckets {
			if b.Tokens() == l.capacity {
				keys = append(keys, key)
			}
		}
		l.mu.Unlock()
		for _, key := range keys {
			if !yield(key) {
				return
			}
		}
	}
}
