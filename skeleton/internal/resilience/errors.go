package resilience

import "errors"

// Errors returned while executing operations.
var (
	// ErrRateLimited is returned by TokenBucket and Limiter when no token is available.
	ErrRateLimited = errors.New("rate limited")

	// ErrCircuitOpen is returned by CircuitBreaker when the operation was not even attempted.
	ErrCircuitOpen = errors.New("circuit breaker is open")

	// ErrBulkheadFull is returned by Bulkhead when all slots are taken.
	ErrBulkheadFull = errors.New("bulkhead is full")

	// ErrRetriesExhausted is matched (errors.Is) by the *RetryError returned from Retry.
	ErrRetriesExhausted = errors.New("retries exhausted")
)

// Errors returned by constructors for invalid arguments.
//
// A constructor checks all of its arguments and reports every problem it finds:
// when several arguments are invalid, the returned error matches (errors.Is)
// each of the corresponding errors below.
var (
	ErrNilClock                 = errors.New("clock is nil")
	ErrNilPolicy                = errors.New("policy is nil")
	ErrNilBackoff               = errors.New("backoff is nil")
	ErrNilRand                  = errors.New("random source is nil")
	ErrInvalidCapacity          = errors.New("capacity must be positive")
	ErrInvalidInterval          = errors.New("interval must be positive")
	ErrInvalidLimit             = errors.New("limit must be positive")
	ErrInvalidFailureThreshold  = errors.New("failure threshold must be positive")
	ErrInvalidOpenTimeout       = errors.New("open timeout must be positive")
	ErrInvalidHalfOpenSuccesses = errors.New("half-open successes must be positive")
	ErrInvalidMaxAttempts       = errors.New("max attempts must be at least 1")
	ErrInvalidBase              = errors.New("backoff base must be positive")
	ErrInvalidFactor            = errors.New("backoff factor must be at least 1")
	ErrInvalidMax               = errors.New("backoff max must be zero or not less than base")
)

// failIf returns err if cond holds and nil otherwise. It is meant to be used
// together with errors.Join, which skips nil errors:
//
//	return errors.Join(
//		failIf(clock == nil, ErrNilClock),
//		failIf(capacity <= 0, ErrInvalidCapacity),
//	)
func failIf(cond bool, err error) error {
	if cond {
		return err
	}
	return nil
}
