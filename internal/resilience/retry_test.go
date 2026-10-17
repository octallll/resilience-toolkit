//go:build bonus

package resilience_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"resilience/internal/resilience"
)

// flaky returns an operation that fails the first `failures` calls and then succeeds.
func flaky(calls *int, failures int) func() error {
	return func() error {
		*calls++
		if *calls <= failures {
			return errBoom
		}
		return nil
	}
}

type recordingBackoff struct{ attempts []int }

func (r *recordingBackoff) Delay(attempt int) time.Duration {
	r.attempts = append(r.attempts, attempt)
	return time.Duration(attempt) * sec
}

func TestRetry_SuccessOnFirstAttempt(t *testing.T) {
	clock := resilience.NewFakeClock()
	r := newRetry(t, clock, 3, resilience.ConstantBackoff{D: sec})

	calls := 0
	if err := r.Do(flaky(&calls, 0)); err != nil {
		t.Fatalf("Do() = %v, want nil", err)
	}
	if calls != 1 || len(clock.Sleeps()) != 0 {
		t.Fatalf("calls = %d, sleeps = %v; want 1 call and no sleeps", calls, clock.Sleeps())
	}
}

func TestRetry_SucceedsAfterRetries(t *testing.T) {
	clock := resilience.NewFakeClock()
	r := newRetry(t, clock, 5, newExp(t, 100*ms, sec, 2))

	calls := 0
	start := clock.Now()
	if err := r.Do(flaky(&calls, 2)); err != nil {
		t.Fatalf("Do() = %v, want nil", err)
	}
	if calls != 3 {
		t.Fatalf("op called %d times, want 3", calls)
	}
	if want := []time.Duration{100 * ms, 200 * ms}; !reflect.DeepEqual(clock.Sleeps(), want) {
		t.Fatalf("sleeps = %v, want %v", clock.Sleeps(), want)
	}
	if got := clock.Now().Sub(start); got != 300*ms {
		t.Fatalf("time elapsed = %v, want 300ms", got)
	}
}

func TestRetry_ExhaustionDoesNotSleepAfterTheLastAttempt(t *testing.T) {
	clock := resilience.NewFakeClock()
	r := newRetry(t, clock, 4, newExp(t, 100*ms, sec, 2))

	calls := 0
	err := r.Do(counter(&calls, errBoom))

	if calls != 4 {
		t.Fatalf("op called %d times, want 4", calls)
	}
	if want := []time.Duration{100 * ms, 200 * ms, 400 * ms}; !reflect.DeepEqual(clock.Sleeps(), want) {
		t.Fatalf("sleeps = %v, want %v (three pauses between four attempts)", clock.Sleeps(), want)
	}

	var re *resilience.RetryError
	if !errors.As(err, &re) {
		t.Fatalf("Do() = %v, want *RetryError", err)
	}
	if re.Attempts != 4 || re.Last != errBoom {
		t.Fatalf("RetryError = %+v, want Attempts=4, Last=errBoom", *re)
	}
	if !errors.Is(err, resilience.ErrRetriesExhausted) {
		t.Fatal("errors.Is(err, ErrRetriesExhausted) = false")
	}
	if !errors.Is(err, errBoom) {
		t.Fatal("errors.Is(err, errBoom) = false: the last error must stay reachable")
	}
	if errors.Is(err, errFatal) {
		t.Fatal("errors.Is(err, errFatal) = true for an unrelated error")
	}
}

func TestRetry_ErrorMessage(t *testing.T) {
	err := &resilience.RetryError{Attempts: 3, Last: errBoom}
	if got, want := err.Error(), "retries exhausted after 3 attempts: boom"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

func TestRetry_LastErrorIsTheOneFromTheLastAttempt(t *testing.T) {
	r := newRetry(t, resilience.NewFakeClock(), 3, nil)
	errs := []error{errBoom, errBenign, errFatal}
	i := 0
	err := r.Do(func() error { i++; return errs[i-1] })

	var re *resilience.RetryError
	if !errors.As(err, &re) || re.Last != errFatal {
		t.Fatalf("Do() = %v, want RetryError with Last = errFatal", err)
	}
	if errors.Is(err, errBoom) {
		t.Fatal("errors.Is(err, errBoom) = true: only the last error must be kept")
	}
}

func TestRetry_NonRetryableErrorIsReturnedAsIs(t *testing.T) {
	clock := resilience.NewFakeClock()
	r := newRetryCfg(t, clock, resilience.RetryConfig{
		MaxAttempts: 5,
		Backoff:     resilience.ConstantBackoff{D: sec},
		Retryable:   func(err error) bool { return err != errFatal },
	})

	calls := 0
	err := r.Do(counter(&calls, errFatal))
	if err != errFatal { // not wrapped, not a RetryError
		t.Fatalf("Do() = %#v, want errFatal itself", err)
	}
	if calls != 1 || len(clock.Sleeps()) != 0 {
		t.Fatalf("calls = %d, sleeps = %v; want 1 call and no sleeps", calls, clock.Sleeps())
	}
}

func TestRetry_RetryableSeesTheOperationError(t *testing.T) {
	var seen []error
	r := newRetryCfg(t, resilience.NewFakeClock(), resilience.RetryConfig{
		MaxAttempts: 3,
		Retryable:   func(err error) bool { seen = append(seen, err); return true },
	})
	_ = r.Do(func() error { return errBoom })
	if want := []error{errBoom, errBoom, errBoom}; !reflect.DeepEqual(seen, want) {
		t.Fatalf("Retryable saw %v, want %v", seen, want)
	}
}

func TestRetry_MaxAttemptsOne(t *testing.T) {
	clock := resilience.NewFakeClock()
	r := newRetry(t, clock, 1, resilience.ConstantBackoff{D: sec})

	calls := 0
	err := r.Do(counter(&calls, errBoom))
	var re *resilience.RetryError
	if !errors.As(err, &re) || re.Attempts != 1 {
		t.Fatalf("Do() = %v, want RetryError{Attempts: 1}", err)
	}
	if calls != 1 || len(clock.Sleeps()) != 0 {
		t.Fatalf("calls = %d, sleeps = %v", calls, clock.Sleeps())
	}
}

func TestRetry_BackoffReceivesAttemptNumbersFromOne(t *testing.T) {
	rb := &recordingBackoff{}
	clock := resilience.NewFakeClock()
	r := newRetry(t, clock, 4, rb)

	_ = r.Do(func() error { return errBoom })
	if want := []int{1, 2, 3}; !reflect.DeepEqual(rb.attempts, want) {
		t.Fatalf("Delay called with %v, want %v", rb.attempts, want)
	}
	if want := []time.Duration{1 * sec, 2 * sec, 3 * sec}; !reflect.DeepEqual(clock.Sleeps(), want) {
		t.Fatalf("sleeps = %v, want %v", clock.Sleeps(), want)
	}
}

func TestRetry_NilBackoffAndZeroDelayNeverSleep(t *testing.T) {
	for name, b := range map[string]resilience.Backoff{
		"nil":      nil,
		"zero":     resilience.ConstantBackoff{D: 0},
		"negative": resilience.ConstantBackoff{D: -sec},
	} {
		clock := resilience.NewFakeClock()
		start := clock.Now()
		r := newRetry(t, clock, 3, b)
		calls := 0
		_ = r.Do(counter(&calls, errBoom))
		if calls != 3 {
			t.Errorf("%s: op called %d times, want 3", name, calls)
		}
		if len(clock.Sleeps()) != 0 || !clock.Now().Equal(start) {
			t.Errorf("%s: Sleep must not be called for a non-positive delay, got %v", name, clock.Sleeps())
		}
	}
}

func TestRetry_PanicIsNotSwallowed(t *testing.T) {
	r := newRetry(t, resilience.NewFakeClock(), 3, nil)
	calls := 0
	mustPanic(t, "panic in op", func() {
		_ = r.Do(func() error { calls++; panic("kaboom") })
	})
	if calls != 1 {
		t.Fatalf("op called %d times, want 1", calls)
	}
}

func TestRetry_ImplementsPolicy(t *testing.T) {
	var _ resilience.Policy = (*resilience.Retry)(nil)
}

func TestRetry_InvalidArguments(t *testing.T) {
	clock := resilience.NewFakeClock()
	cases := []struct {
		name     string
		clock    resilience.Clock
		attempts int
		want     []error
	}{
		{"nil clock", nil, 1, []error{resilience.ErrNilClock}},
		{"zero attempts", clock, 0, []error{resilience.ErrInvalidMaxAttempts}},
		{"negative attempts", clock, -2, []error{resilience.ErrInvalidMaxAttempts}},
		{"everything wrong", nil, 0, []error{resilience.ErrNilClock, resilience.ErrInvalidMaxAttempts}},
	}
	for _, c := range cases {
		r, err := resilience.NewRetry(c.clock, resilience.RetryConfig{MaxAttempts: c.attempts})
		if r != nil {
			t.Errorf("%s: a Retry was returned together with an error", c.name)
		}
		checkInvalid(t, c.name, err, c.want...)
	}
}

// Retry on the outside, breaker and limiter on the inside: the retries keep
// knocking until the circuit lets a probe through, all in fake time.
func TestRetry_WaitsForCircuitBreakerRecovery(t *testing.T) {
	clock := resilience.NewFakeClock()
	policy := chain(t,
		newRetry(t, clock, 5, resilience.ConstantBackoff{D: sec}),
		newBreaker(t, clock, 2, 3*sec, 1),
		newBucket(t, clock, 100, sec),
	)

	// The service is broken for its first two calls, then recovers.
	calls := 0
	begin := clock.Now()
	err := policy.Do(flaky(&calls, 2))
	if err != nil {
		t.Fatalf("Do() = %v, want nil", err)
	}
	// attempt 1 fails (t+0), attempt 2 fails (t+1s) and opens the circuit,
	// attempts 3 and 4 (t+2s, t+3s) are rejected without calling op,
	// attempt 5 (t+4s) is the probe: the circuit opened at t+1s, 3s ago.
	if calls != 3 {
		t.Fatalf("real operation calls = %d, want 3", calls)
	}
	if got := clock.Now().Sub(begin); got != 4*sec {
		t.Fatalf("elapsed = %v, want 4s", got)
	}
	if want := []time.Duration{sec, sec, sec, sec}; !reflect.DeepEqual(clock.Sleeps(), want) {
		t.Fatalf("sleeps = %v, want %v", clock.Sleeps(), want)
	}
}

func TestRetry_GivesUpWhileCircuitIsStillOpen(t *testing.T) {
	clock := resilience.NewFakeClock()
	policy := chain(t,
		newRetry(t, clock, 3, resilience.ConstantBackoff{D: sec}),
		newBreaker(t, clock, 1, time.Minute, 1),
	)

	calls := 0
	err := policy.Do(counter(&calls, errBoom))

	var re *resilience.RetryError
	if !errors.As(err, &re) || re.Attempts != 3 {
		t.Fatalf("Do() = %v, want RetryError with 3 attempts", err)
	}
	if !errors.Is(err, resilience.ErrCircuitOpen) {
		t.Fatal("the last attempt was rejected by the breaker: errors.Is(err, ErrCircuitOpen) must hold")
	}
	if errors.Is(err, errBoom) {
		t.Fatal("errBoom belongs to an earlier attempt and must not be reachable")
	}
	if calls != 1 {
		t.Fatalf("op called %d times, want 1 (the circuit opened after the first failure)", calls)
	}
}
