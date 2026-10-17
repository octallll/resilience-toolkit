package resilience_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"resilience/internal/resilience"
)

const (
	ms  = time.Millisecond
	sec = time.Second
)

var (
	errBoom   = errors.New("boom")
	errFatal  = errors.New("fatal")
	errBenign = errors.New("benign")
)

// mustPanic fails the test if f does not panic.
func mustPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		if recover() == nil {
			t.Errorf("%s: expected a panic", name)
		}
	}()
	f()
}

// allConstructorErrors lists every error a constructor may report for invalid arguments.
var allConstructorErrors = []error{
	resilience.ErrNilClock,
	resilience.ErrNilPolicy,
	resilience.ErrNilBackoff,
	resilience.ErrNilRand,
	resilience.ErrInvalidCapacity,
	resilience.ErrInvalidInterval,
	resilience.ErrInvalidLimit,
	resilience.ErrInvalidFailureThreshold,
	resilience.ErrInvalidOpenTimeout,
	resilience.ErrInvalidHalfOpenSuccesses,
	resilience.ErrInvalidMaxAttempts,
	resilience.ErrInvalidBase,
	resilience.ErrInvalidFactor,
	resilience.ErrInvalidMax,
}

// checkInvalid verifies that err matches every error in want and none of the other
// constructor errors, i.e. that the constructor tells the problems apart.
func checkInvalid(t *testing.T, name string, err error, want ...error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: got no error", name)
		return
	}
	for _, e := range allConstructorErrors {
		wanted := slices.Contains(want, e)
		if got := errors.Is(err, e); got != wanted {
			t.Errorf("%s: errors.Is(err, %q) = %v, want %v (err = %v)", name, e, got, wanted, err)
		}
	}
}

func newBucket(t *testing.T, clock resilience.Clock, capacity int, interval time.Duration) *resilience.TokenBucket {
	t.Helper()
	b, err := resilience.NewTokenBucket(clock, capacity, interval)
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	return b
}

func newBreakerCfg(t *testing.T, clock resilience.Clock, cfg resilience.Config) *resilience.CircuitBreaker {
	t.Helper()
	b, err := resilience.NewCircuitBreaker(clock, cfg)
	if err != nil {
		t.Fatalf("NewCircuitBreaker: %v", err)
	}
	return b
}

func newBreaker(t *testing.T, clock resilience.Clock, threshold int, timeout time.Duration, successes int) *resilience.CircuitBreaker {
	t.Helper()
	return newBreakerCfg(t, clock, resilience.Config{
		FailureThreshold:  threshold,
		OpenTimeout:       timeout,
		HalfOpenSuccesses: successes,
	})
}

func newRetryCfg(t *testing.T, clock resilience.Clock, cfg resilience.RetryConfig) *resilience.Retry {
	t.Helper()
	r, err := resilience.NewRetry(clock, cfg)
	if err != nil {
		t.Fatalf("NewRetry: %v", err)
	}
	return r
}

func newRetry(t *testing.T, clock resilience.Clock, attempts int, b resilience.Backoff) *resilience.Retry {
	t.Helper()
	return newRetryCfg(t, clock, resilience.RetryConfig{MaxAttempts: attempts, Backoff: b})
}

func newBulkhead(t *testing.T, limit int) *resilience.Bulkhead {
	t.Helper()
	b, err := resilience.NewBulkhead(limit)
	if err != nil {
		t.Fatalf("NewBulkhead: %v", err)
	}
	return b
}

func newLimiter[K comparable](t *testing.T, clock resilience.Clock, capacity int, interval time.Duration) *resilience.Limiter[K] {
	t.Helper()
	l, err := resilience.NewLimiter[K](clock, capacity, interval)
	if err != nil {
		t.Fatalf("NewLimiter: %v", err)
	}
	return l
}

func newExp(t *testing.T, base, max time.Duration, factor int) resilience.ExponentialBackoff {
	t.Helper()
	b, err := resilience.NewExponentialBackoff(base, max, factor)
	if err != nil {
		t.Fatalf("NewExponentialBackoff: %v", err)
	}
	return b
}

func chain(t *testing.T, policies ...resilience.Policy) resilience.Policy {
	t.Helper()
	p, err := resilience.Chain(policies...)
	if err != nil {
		t.Fatalf("Chain: %v", err)
	}
	return p
}

// withTimeout fails the test if f does not return in time (a deadlock guard).
func withTimeout(t *testing.T, d time.Duration, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("timed out after %v: possible deadlock", d)
	}
}

// counter returns an operation that always returns err and counts its calls.
func counter(calls *int, err error) func() error {
	return func() error {
		*calls++
		return err
	}
}

func ok() error { return nil }
