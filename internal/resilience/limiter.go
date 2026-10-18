package resilience

import (
	"hash/maphash"
	"iter"
	"sync"
	"time"
)

// shardCount is the number of independently locked parts of a Limiter. Keys of
// different shards never contend with each other, and Sweep and Idle work on one
// shard at a time, so they never stop the whole limiter.
const shardCount = 64

type shard[K comparable] struct {
	mu      sync.Mutex
	buckets map[K]bucketState
}

// Limiter keeps an independent token bucket for every key.
type Limiter[K comparable] struct {
	clock    Clock
	capacity int
	interval time.Duration

	seed   maphash.Seed
	shards [shardCount]shard[K]
}

// NewLimiter reports invalid arguments exactly like NewTokenBucket does.
func NewLimiter[K comparable](clock Clock, capacity int, interval time.Duration) (*Limiter[K], error) {
	if err := validateBucket(clock, capacity, interval); err != nil {
		return nil, err
	}
	l := &Limiter[K]{
		clock:    clock,
		capacity: capacity,
		interval: interval,
		seed:     maphash.MakeSeed(),
	}
	for i := range l.shards {
		l.shards[i].buckets = make(map[K]bucketState)
	}
	return l, nil
}

func (l *Limiter[K]) shardOf(key K) *shard[K] {
	return &l.shards[maphash.Comparable(l.seed, key)%shardCount]
}

// Allow takes a token from the key's bucket, creating a full bucket on first use.
func (l *Limiter[K]) Allow(key K) bool {
	now := l.clock.Now() // read outside the lock: the critical section is only a map access
	s := l.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.buckets[key]
	if !ok {
		st = bucketState{tokens: l.capacity, last: now}
	}
	st.refill(now, l.capacity, l.interval)
	allowed := st.tokens > 0
	if allowed {
		st.tokens--
	}
	s.buckets[key] = st
	return allowed
}

// Do runs op if the key has a token, otherwise returns ErrRateLimited.
func (l *Limiter[K]) Do(key K, op func() error) error {
	if !l.Allow(key) {
		return ErrRateLimited
	}
	return op()
}

// Tokens reports the tokens available for key. An unknown key has a full bucket;
// asking does not create or change anything.
func (l *Limiter[K]) Tokens(key K) int {
	now := l.clock.Now()
	s := l.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.buckets[key] // st is a copy, so refill below does not touch the map
	if !ok {
		return l.capacity
	}
	st.refill(now, l.capacity, l.interval)
	return st.tokens
}

// Size returns the number of keys that currently have a bucket. Shards are counted
// one after another, so under concurrent use the result is only approximate.
func (l *Limiter[K]) Size() int {
	n := 0
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		n += len(s.buckets)
		s.mu.Unlock()
	}
	return n
}

// Sweep deletes the buckets that are full and returns how many were deleted.
// A full bucket is indistinguishable from a new one, so Sweep never changes
// the answers of Allow, Do and Tokens. Shards are locked one at a time.
func (l *Limiter[K]) Sweep() int {
	now := l.clock.Now()
	removed := 0
	for i := range l.shards {
		s := &l.shards[i]
		s.mu.Lock()
		for key, st := range s.buckets {
			if st.isFull(now, l.capacity, l.interval) {
				delete(s.buckets, key)
				removed++
			}
		}
		s.mu.Unlock()
	}
	return removed
}

// Idle iterates over the keys whose buckets are full, i.e. the keys Sweep would delete.
// The order is unspecified. The limiter is not locked while the loop body runs,
// so the body may freely call the limiter's methods.
//
// Keys are collected one shard at a time, so memory is proportional to the size of a
// shard, not of the whole limiter. A key may stop being idle between the moment it is
// collected and the moment the loop body sees it.
func (l *Limiter[K]) Idle() iter.Seq[K] {
	return func(yield func(K) bool) {
		now := l.clock.Now()
		var keys []K
		for i := range l.shards {
			s := &l.shards[i]
			keys = keys[:0]
			s.mu.Lock()
			for key, st := range s.buckets {
				if st.isFull(now, l.capacity, l.interval) {
					keys = append(keys, key)
				}
			}
			s.mu.Unlock()
			for _, key := range keys {
				if !yield(key) {
					return
				}
			}
		}
	}
}
