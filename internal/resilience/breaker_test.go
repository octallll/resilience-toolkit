package resilience_test

import (
	"testing"
	"time"

	"resilience/internal/resilience"
)

func fail(t *testing.T, b *resilience.CircuitBreaker, n int) {
	t.Helper()
	for i := range n {
		if err := b.Do(func() error { return errBoom }); err != errBoom {
			t.Fatalf("failure %d: Do() = %v, want errBoom", i+1, err)
		}
	}
}

func succeed(t *testing.T, b *resilience.CircuitBreaker, n int) {
	t.Helper()
	for i := range n {
		if err := b.Do(ok); err != nil {
			t.Fatalf("success %d: Do() = %v, want nil", i+1, err)
		}
	}
}

func wantState(t *testing.T, b *resilience.CircuitBreaker, want resilience.State) {
	t.Helper()
	if got := b.State(); got != want {
		t.Fatalf("State() = %v, want %v", got, want)
	}
}

func TestState_String(t *testing.T) {
	cases := map[resilience.State]string{
		resilience.Closed:   "closed",
		resilience.Open:     "open",
		resilience.HalfOpen: "half-open",
		resilience.State(7): "State(7)",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", int(s), got, want)
		}
	}
}

func TestBreaker_StartsClosedAndPassesErrorsThrough(t *testing.T) {
	b := newBreaker(t, resilience.NewFakeClock(), 3, 5*sec, 1)
	wantState(t, b, resilience.Closed)

	if err := b.Do(ok); err != nil {
		t.Fatalf("Do() = %v, want nil", err)
	}
	if err := b.Do(func() error { return errBoom }); err != errBoom {
		t.Fatalf("Do() = %v, want errBoom (the very same error)", err)
	}
}

func TestBreaker_OpensAfterThresholdConsecutiveFailures(t *testing.T) {
	b := newBreaker(t, resilience.NewFakeClock(), 3, 5*sec, 1)

	fail(t, b, 2)
	wantState(t, b, resilience.Closed)
	fail(t, b, 1)
	wantState(t, b, resilience.Open)
}

func TestBreaker_OpenCircuitDoesNotCallOp(t *testing.T) {
	b := newBreaker(t, resilience.NewFakeClock(), 1, 5*sec, 1)
	fail(t, b, 1)

	calls := 0
	for range 3 {
		if err := b.Do(counter(&calls, nil)); err != resilience.ErrCircuitOpen {
			t.Fatalf("Do() = %v, want ErrCircuitOpen", err)
		}
	}
	if calls != 0 {
		t.Fatalf("op called %d times on an open circuit, want 0", calls)
	}
}

func TestBreaker_SuccessResetsFailureCounter(t *testing.T) {
	b := newBreaker(t, resilience.NewFakeClock(), 3, 5*sec, 1)

	fail(t, b, 2)
	succeed(t, b, 1)
	fail(t, b, 2)
	wantState(t, b, resilience.Closed) // only two failures in a row
	fail(t, b, 1)
	wantState(t, b, resilience.Open)
}

func TestBreaker_OpenTimeoutBoundary(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBreaker(t, clock, 1, 5*sec, 1)
	fail(t, b, 1)

	clock.Advance(5*sec - time.Nanosecond)
	wantState(t, b, resilience.Open)
	if err := b.Do(ok); err != resilience.ErrCircuitOpen {
		t.Fatalf("Do() 1ns before the timeout = %v, want ErrCircuitOpen", err)
	}

	clock.Advance(time.Nanosecond) // exactly OpenTimeout since the circuit opened
	wantState(t, b, resilience.HalfOpen)
}

func TestBreaker_TimeoutIsMeasuredFromTheMomentOfOpening(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBreaker(t, clock, 2, 10*sec, 1)

	fail(t, b, 1)
	clock.Advance(7 * sec) // time spent closed must not count
	fail(t, b, 1)          // opens now
	clock.Advance(9 * sec)
	wantState(t, b, resilience.Open)
	clock.Advance(1 * sec)
	wantState(t, b, resilience.HalfOpen)
}

func TestBreaker_HalfOpenSuccessClosesTheCircuit(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBreaker(t, clock, 2, 5*sec, 2)
	fail(t, b, 2)
	clock.Advance(5 * sec)

	succeed(t, b, 1)
	wantState(t, b, resilience.HalfOpen) // one probe is not enough
	succeed(t, b, 1)
	wantState(t, b, resilience.Closed)

	// After closing, the failure counter starts from zero.
	fail(t, b, 1)
	wantState(t, b, resilience.Closed)
	fail(t, b, 1)
	wantState(t, b, resilience.Open)
}

func TestBreaker_HalfOpenFailureReopensAndRestartsTheTimer(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBreaker(t, clock, 1, 10*sec, 1)
	fail(t, b, 1)
	clock.Advance(10 * sec)
	wantState(t, b, resilience.HalfOpen)

	clock.Advance(4 * sec)
	fail(t, b, 1) // the probe fails at t = 14s
	wantState(t, b, resilience.Open)

	clock.Advance(9 * sec) // t = 23s, 9s after the failed probe
	wantState(t, b, resilience.Open)
	clock.Advance(1 * sec)
	wantState(t, b, resilience.HalfOpen)
}

func TestBreaker_ReopeningForgetsProbeSuccesses(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBreaker(t, clock, 1, 5*sec, 2)
	fail(t, b, 1)

	clock.Advance(5 * sec)
	succeed(t, b, 1) // 1 of 2
	fail(t, b, 1)    // reopen
	wantState(t, b, resilience.Open)

	clock.Advance(5 * sec)
	succeed(t, b, 1)
	wantState(t, b, resilience.HalfOpen) // the earlier success must not count
	succeed(t, b, 1)
	wantState(t, b, resilience.Closed)
}

func TestBreaker_CustomIsFailure(t *testing.T) {
	b := newBreakerCfg(t, resilience.NewFakeClock(), resilience.Config{
		FailureThreshold:  2,
		OpenTimeout:       sec,
		HalfOpenSuccesses: 1,
		IsFailure:         func(err error) bool { return err != errBenign },
	})

	for range 5 {
		if err := b.Do(func() error { return errBenign }); err != errBenign {
			t.Fatalf("Do() = %v, want errBenign returned to the caller", err)
		}
	}
	wantState(t, b, resilience.Closed)

	fail(t, b, 2)
	wantState(t, b, resilience.Open)
}

func TestBreaker_BenignErrorResetsTheFailureStreak(t *testing.T) {
	b := newBreakerCfg(t, resilience.NewFakeClock(), resilience.Config{
		FailureThreshold:  2,
		OpenTimeout:       sec,
		HalfOpenSuccesses: 1,
		IsFailure:         func(err error) bool { return err == errBoom },
	})
	fail(t, b, 1)
	_ = b.Do(func() error { return errBenign }) // counts as a success
	fail(t, b, 1)
	wantState(t, b, resilience.Closed)
}

func TestBreaker_PanicCountsAsFailureAndPropagates(t *testing.T) {
	b := newBreaker(t, resilience.NewFakeClock(), 1, sec, 1)

	func() {
		defer func() {
			if r := recover(); r != "kaboom" {
				t.Errorf("recovered %v, want the original panic value", r)
			}
		}()
		_ = b.Do(func() error { panic("kaboom") })
	}()
	wantState(t, b, resilience.Open)
}

// A panicking IsFailure must not leave the breaker stuck: the call counts as a failure,
// the probe slot is released, and the panic reaches the caller.
func TestBreaker_PanicInIsFailureCountsAsFailureAndFreesTheProbe(t *testing.T) {
	clock := resilience.NewFakeClock()
	b, err := resilience.NewCircuitBreaker(clock, resilience.Config{
		FailureThreshold:  1,
		OpenTimeout:       sec,
		HalfOpenSuccesses: 1,
		IsFailure: func(err error) bool {
			if err == errBenign {
				panic("bad classifier")
			}
			return err != nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fail(t, b, 1)
	clock.Advance(sec)
	wantState(t, b, resilience.HalfOpen)

	func() {
		defer func() {
			if r := recover(); r != "bad classifier" {
				t.Errorf("recovered %v, want the original panic value", r)
			}
		}()
		_ = b.Do(func() error { return errBenign })
	}()
	wantState(t, b, resilience.Open)

	clock.Advance(sec)
	if err := b.Do(ok); err != nil {
		t.Fatalf("Do() after the timeout = %v, want nil: the probe slot must have been freed", err)
	}
	wantState(t, b, resilience.Closed)
}

func TestBreaker_ImplementsPolicy(t *testing.T) {
	var _ resilience.Policy = (*resilience.CircuitBreaker)(nil)
}

func TestBreaker_InvalidArguments(t *testing.T) {
	clock := resilience.NewFakeClock()
	valid := resilience.Config{FailureThreshold: 1, OpenTimeout: sec, HalfOpenSuccesses: 1}
	with := func(f func(*resilience.Config)) resilience.Config {
		cfg := valid
		f(&cfg)
		return cfg
	}

	cases := []struct {
		name  string
		clock resilience.Clock
		cfg   resilience.Config
		want  []error
	}{
		{"nil clock", nil, valid, []error{resilience.ErrNilClock}},
		{"zero threshold", clock, with(func(c *resilience.Config) { c.FailureThreshold = 0 }),
			[]error{resilience.ErrInvalidFailureThreshold}},
		{"negative threshold", clock, with(func(c *resilience.Config) { c.FailureThreshold = -3 }),
			[]error{resilience.ErrInvalidFailureThreshold}},
		{"zero timeout", clock, with(func(c *resilience.Config) { c.OpenTimeout = 0 }),
			[]error{resilience.ErrInvalidOpenTimeout}},
		{"negative timeout", clock, with(func(c *resilience.Config) { c.OpenTimeout = -sec }),
			[]error{resilience.ErrInvalidOpenTimeout}},
		{"negative half-open successes", clock, with(func(c *resilience.Config) { c.HalfOpenSuccesses = -1 }),
			[]error{resilience.ErrInvalidHalfOpenSuccesses}},
		{"everything wrong", nil, resilience.Config{}, []error{
			resilience.ErrNilClock,
			resilience.ErrInvalidFailureThreshold,
			resilience.ErrInvalidOpenTimeout,
			resilience.ErrInvalidHalfOpenSuccesses,
		}},
	}
	for _, c := range cases {
		b, err := resilience.NewCircuitBreaker(c.clock, c.cfg)
		if b != nil {
			t.Errorf("%s: a breaker was returned together with an error", c.name)
		}
		checkInvalid(t, c.name, err, c.want...)
	}
}
