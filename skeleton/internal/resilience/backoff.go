package resilience

import "time"

// Backoff computes the pause before the next attempt.
type Backoff interface {
	// Delay returns the pause after the failed attempt number `attempt` (starting from 1).
	Delay(attempt int) time.Duration
}

// ConstantBackoff always returns D.
type ConstantBackoff struct{ D time.Duration }

func (b ConstantBackoff) Delay(attempt int) time.Duration {
	panic("not implemented")
}

// ExponentialBackoff returns base * factor^(attempt-1), but never more than max.
// Create it with NewExponentialBackoff. The zero value is valid and never waits.
type ExponentialBackoff struct {
	// TODO: add the fields you need (keep them unexported).
}

// NewExponentialBackoff returns an ExponentialBackoff. max == 0 means "no limit":
// the delay then saturates at math.MaxInt64 nanoseconds instead of overflowing.
//
// For invalid arguments it returns an error matching ErrInvalidBase (base <= 0),
// ErrInvalidFactor (factor < 1) and/or ErrInvalidMax (max < 0 or 0 < max < base).
func NewExponentialBackoff(base, max time.Duration, factor int) (ExponentialBackoff, error) {
	panic("not implemented")
}

// Delay returns the pause after the failed attempt number `attempt`.
// An attempt below 1 is treated as the first one.
func (b ExponentialBackoff) Delay(attempt int) time.Duration {
	panic("not implemented")
}
