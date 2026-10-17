package resilience

import (
	"errors"
	"fmt"
)

// RetryError is returned by Retry when every attempt has failed.
type RetryError struct {
	Attempts int   // how many times op was called
	Last     error // the error of the last attempt
}

func (e *RetryError) Error() string {
	return fmt.Sprintf("retries exhausted after %d attempts: %v", e.Attempts, e.Last)
}

// Unwrap makes both errors.Is(err, ErrRetriesExhausted) and errors.Is(err, <last error>) work.
func (e *RetryError) Unwrap() []error { return []error{ErrRetriesExhausted, e.Last} }

// RetryConfig configures Retry.
type RetryConfig struct {
	MaxAttempts int              // total number of calls of op, including the first one; must be >= 1
	Backoff     Backoff          // nil means "no pause"
	Retryable   func(error) bool // nil means "retry any error"
}

// Retry repeats a failing operation.
type Retry struct {
	clock Clock
	cfg   RetryConfig
}

// NewRetry returns an error matching ErrNilClock and/or ErrInvalidMaxAttempts for invalid arguments.
func NewRetry(clock Clock, cfg RetryConfig) (*Retry, error) {
	if err := errors.Join(
		failIf(clock == nil, ErrNilClock),
		failIf(cfg.MaxAttempts < 1, ErrInvalidMaxAttempts),
	); err != nil {
		return nil, err
	}
	return &Retry{clock: clock, cfg: cfg}, nil
}

// Do calls op up to MaxAttempts times.
//
//   - nil from op ends the loop with nil;
//   - a non-retryable error is returned immediately, unwrapped;
//   - after the last failed attempt a *RetryError is returned;
//   - between attempts clock.Sleep is called with Backoff.Delay(attempt),
//     but only when the delay is positive and never after the last attempt.
func (r *Retry) Do(op func() error) error {
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil {
			return nil
		}
		if r.cfg.Retryable != nil && !r.cfg.Retryable(err) {
			return err
		}
		if attempt >= r.cfg.MaxAttempts {
			return &RetryError{Attempts: attempt, Last: err}
		}
		if r.cfg.Backoff != nil {
			if d := r.cfg.Backoff.Delay(attempt); d > 0 {
				r.clock.Sleep(d)
			}
		}
	}
}
