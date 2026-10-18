package resilience

import (
	"errors"
	"sync"
	"time"
)

// bucketState is the whole mutable state of a token bucket. It is shared by
// TokenBucket and Limiter, which only differ in how they guard and store it.
type bucketState struct {
	tokens int
	last   time.Time // the moment from which the next token is being "cooked"
}

// refill adds the tokens earned since b.last. It is a pure function of the state and
// `now`, so callers may read the clock before taking their lock.
func (b *bucketState) refill(now time.Time, capacity int, interval time.Duration) {
	if now.Before(b.last) { // a stale reading taken before another goroutine's update
		now = b.last
	}
	if b.tokens >= capacity {
		// A full bucket earns nothing: the next token starts "cooking" only
		// after somebody takes one.
		b.last = now
		return
	}
	elapsed := now.Sub(b.last)
	if elapsed < interval {
		return
	}
	n := int64(elapsed / interval)
	if n >= int64(capacity-b.tokens) {
		b.tokens = capacity
		b.last = now
		return
	}
	b.tokens += int(n)
	// Keep the fractional remainder of the interval instead of dropping it.
	b.last = b.last.Add(time.Duration(n) * interval)
}

// isFull reports whether the bucket would be full at `now`, without changing it.
func (b bucketState) isFull(now time.Time, capacity int, interval time.Duration) bool {
	b.refill(now, capacity, interval) // b is a copy
	return b.tokens >= capacity
}

// TokenBucket allows at most `capacity` operations in a burst and then
// one more operation every `interval`.
type TokenBucket struct {
	clock    Clock
	capacity int
	interval time.Duration

	mu sync.Mutex
	st bucketState
}

// validateBucket checks the arguments shared by NewTokenBucket and NewLimiter.
func validateBucket(clock Clock, capacity int, interval time.Duration) error {
	return errors.Join(
		failIf(clock == nil, ErrNilClock),
		failIf(capacity <= 0, ErrInvalidCapacity),
		failIf(interval <= 0, ErrInvalidInterval),
	)
}

// NewTokenBucket returns a full bucket. For invalid arguments it returns
// an error matching ErrNilClock, ErrInvalidCapacity and/or ErrInvalidInterval.
func NewTokenBucket(clock Clock, capacity int, interval time.Duration) (*TokenBucket, error) {
	if err := validateBucket(clock, capacity, interval); err != nil {
		return nil, err
	}
	return &TokenBucket{
		clock:    clock,
		capacity: capacity,
		interval: interval,
		st:       bucketState{tokens: capacity, last: clock.Now()},
	}, nil
}

// Allow takes one token if there is one.
func (b *TokenBucket) Allow() bool {
	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.st.refill(now, b.capacity, b.interval)
	if b.st.tokens == 0 {
		return false
	}
	b.st.tokens--
	return true
}

// Tokens reports how many tokens are available right now, without taking any.
func (b *TokenBucket) Tokens() int {
	now := b.clock.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.st.refill(now, b.capacity, b.interval)
	return b.st.tokens
}

// Do runs op if a token is available and returns op's error as is.
// Otherwise it returns ErrRateLimited without calling op.
func (b *TokenBucket) Do(op func() error) error {
	if !b.Allow() {
		return ErrRateLimited
	}
	return op()
}
