package resilience

import (
	"errors"
	"math"
	"time"
)

// Backoff computes the pause before the next attempt.
type Backoff interface {
	// Delay returns the pause after the failed attempt number `attempt` (starting from 1).
	Delay(attempt int) time.Duration
}

// ConstantBackoff always returns D.
type ConstantBackoff struct{ D time.Duration }

func (b ConstantBackoff) Delay(int) time.Duration { return b.D }

// ExponentialBackoff returns base * factor^(attempt-1), but never more than max.
// Create it with NewExponentialBackoff. The zero value is valid and never waits.
type ExponentialBackoff struct {
	base   time.Duration
	max    time.Duration
	factor int
}

// NewExponentialBackoff returns an ExponentialBackoff. max == 0 means "no limit":
// the delay then saturates at math.MaxInt64 nanoseconds instead of overflowing.
//
// For invalid arguments it returns an error matching ErrInvalidBase (base <= 0),
// ErrInvalidFactor (factor < 1) and/or ErrInvalidMax (max < 0 or 0 < max < base).
func NewExponentialBackoff(base, max time.Duration, factor int) (ExponentialBackoff, error) {
	if err := errors.Join(
		failIf(base <= 0, ErrInvalidBase),
		failIf(factor < 1, ErrInvalidFactor),
		failIf(max < 0 || (max > 0 && max < base), ErrInvalidMax),
	); err != nil {
		return ExponentialBackoff{}, err
	}
	return ExponentialBackoff{base: base, max: max, factor: factor}, nil
}

// Delay returns the pause after the failed attempt number `attempt`.
// An attempt below 1 is treated as the first one.
func (b ExponentialBackoff) Delay(attempt int) time.Duration {
	if b.base <= 0 || b.factor < 1 { // zero value
		return 0
	}
	limit := time.Duration(math.MaxInt64)
	if b.max > 0 {
		limit = b.max
	}
	if b.factor == 1 {
		return b.base
	}
	f := time.Duration(b.factor)
	d := b.base
	for i := 1; i < attempt; i++ {
		if d > limit/f {
			return limit
		}
		d *= f
	}
	return d
}
