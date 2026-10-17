package resilience

import "time"

// TokenBucket allows at most `capacity` operations in a burst and then
// one more operation every `interval`.
type TokenBucket struct {
	// TODO: add the fields you need.
}

// NewTokenBucket returns a full bucket. For invalid arguments it returns
// an error matching ErrNilClock, ErrInvalidCapacity and/or ErrInvalidInterval
// (all problems at once, see errors.go).
func NewTokenBucket(clock Clock, capacity int, interval time.Duration) (*TokenBucket, error) {
	panic("not implemented")
}

// Allow takes one token if there is one.
func (b *TokenBucket) Allow() bool {
	panic("not implemented")
}

// Tokens reports how many tokens are available right now, without taking any.
func (b *TokenBucket) Tokens() int {
	panic("not implemented")
}

// Do runs op if a token is available and returns op's error as is.
// Otherwise it returns ErrRateLimited without calling op.
func (b *TokenBucket) Do(op func() error) error {
	panic("not implemented")
}
