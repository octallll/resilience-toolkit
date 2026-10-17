package resilience

import (
	"errors"
	"sync"
	"time"
)

// TokenBucket allows at most `capacity` operations in a burst and then
// one more operation every `interval`.
type TokenBucket struct {
	clock    Clock
	capacity int
	interval time.Duration

	mu     sync.Mutex
	tokens int
	last   time.Time // the moment from which the next token is being "cooked"
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
	return newTokenBucket(clock, capacity, interval), nil
}

// newTokenBucket builds a bucket from arguments that are already known to be valid.
func newTokenBucket(clock Clock, capacity int, interval time.Duration) *TokenBucket {
	return &TokenBucket{
		clock:    clock,
		capacity: capacity,
		interval: interval,
		tokens:   capacity,
		last:     clock.Now(),
	}
}

// refill adds the tokens earned since b.last. The caller must hold b.mu.
func (b *TokenBucket) refill() {
	now := b.clock.Now()
	if b.tokens >= b.capacity {
		// A full bucket earns nothing: the next token starts "cooking" only
		// after somebody takes one.
		b.last = now
		return
	}
	elapsed := now.Sub(b.last)
	if elapsed < b.interval {
		return
	}
	n := int64(elapsed / b.interval)
	if n >= int64(b.capacity-b.tokens) {
		b.tokens = b.capacity
		b.last = now
		return
	}
	b.tokens += int(n)
	// Keep the fractional remainder of the interval instead of dropping it.
	b.last = b.last.Add(time.Duration(n) * b.interval)
}

// Allow takes one token if there is one.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens == 0 {
		return false
	}
	b.tokens--
	return true
}

// Tokens reports how many tokens are available right now, without taking any.
func (b *TokenBucket) Tokens() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	return b.tokens
}

// Do runs op if a token is available and returns op's error as is.
// Otherwise it returns ErrRateLimited without calling op.
func (b *TokenBucket) Do(op func() error) error {
	if !b.Allow() {
		return ErrRateLimited
	}
	return op()
}
