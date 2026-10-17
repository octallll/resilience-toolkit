package resilience

import (
	"iter"
	"time"
)

// Limiter keeps an independent TokenBucket for every key.
type Limiter[K comparable] struct {
	// TODO: add the fields you need.
}

// NewLimiter reports invalid arguments exactly like NewTokenBucket does.
func NewLimiter[K comparable](clock Clock, capacity int, interval time.Duration) (*Limiter[K], error) {
	panic("not implemented")
}

// Allow takes a token from the key's bucket, creating a full bucket on first use.
func (l *Limiter[K]) Allow(key K) bool {
	panic("not implemented")
}

// Do runs op if the key has a token, otherwise returns ErrRateLimited.
func (l *Limiter[K]) Do(key K, op func() error) error {
	panic("not implemented")
}

// Tokens reports the tokens available for key. An unknown key has a full bucket;
// asking does not create anything.
func (l *Limiter[K]) Tokens(key K) int {
	panic("not implemented")
}

// Size returns the number of keys that currently have a bucket.
func (l *Limiter[K]) Size() int {
	panic("not implemented")
}

// Sweep deletes the buckets that are full and returns how many were deleted.
func (l *Limiter[K]) Sweep() int {
	panic("not implemented")
}

// Idle iterates over the keys whose buckets are full, i.e. the keys Sweep would delete.
func (l *Limiter[K]) Idle() iter.Seq[K] {
	panic("not implemented")
}
