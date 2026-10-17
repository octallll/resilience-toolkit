//go:build bonus

package resilience_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"resilience/internal/resilience"
)

// Run these with -race.

func runParallel(n int, f func(worker int)) {
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			f(i)
		}()
	}
	wg.Wait()
}

func TestConcurrent_TokenBucketNeverHandsOutMoreThanCapacity(t *testing.T) {
	b := newBucket(t, resilience.NewFakeClock(), 100, 1000*sec)

	var granted atomic.Int64
	runParallel(50, func(int) {
		for range 10 {
			if b.Allow() {
				granted.Add(1)
			}
		}
	})
	if got := granted.Load(); got != 100 {
		t.Fatalf("granted %d tokens, want exactly 100", got)
	}
}

func TestConcurrent_LimiterWithSweepNeverLosesOrDuplicatesTokens(t *testing.T) {
	l := newLimiter[string](t, resilience.NewFakeClock(), 100, 1000*sec)

	var granted atomic.Int64
	stop := make(chan struct{})
	var sweeper sync.WaitGroup
	sweeper.Add(1)
	go func() {
		defer sweeper.Done()
		for {
			select {
			case <-stop:
				return
			default:
				l.Sweep()
				for range l.Idle() {
				}
			}
		}
	}()

	runParallel(20, func(int) {
		for range 20 {
			if l.Allow("k") {
				granted.Add(1)
			}
		}
	})
	close(stop)
	sweeper.Wait()

	if got := granted.Load(); got != 100 {
		t.Fatalf("granted %d tokens for one key, want exactly 100", got)
	}
}

func TestConcurrent_BreakerOpensAndStaysConsistent(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBreaker(t, clock, 5, 1000*sec, 1)

	var executed atomic.Int64
	runParallel(30, func(int) {
		for range 10 {
			_ = b.Do(func() error {
				executed.Add(1)
				return errBoom
			})
			_ = b.State()
		}
	})

	wantState(t, b, resilience.Open)
	// All 300 calls failed. Every call that was let in before the circuit opened
	// ran, so at least 5 did; once it is open nobody gets in.
	if got := executed.Load(); got < 5 || got > 300 {
		t.Fatalf("executed = %d, want between 5 and 300", got)
	}
	before := executed.Load()
	if err := b.Do(ok); err != resilience.ErrCircuitOpen {
		t.Fatalf("Do() = %v, want ErrCircuitOpen", err)
	}
	if executed.Load() != before {
		t.Fatal("op ran on an open circuit")
	}
}

func TestConcurrent_BreakerDoesNotHoldItsLockWhileRunningOp(t *testing.T) {
	b := newBreaker(t, resilience.NewFakeClock(), 3, sec, 1)

	withTimeout(t, 2*sec, func() {
		err := b.Do(func() error {
			// Re-entering the breaker from inside the operation must not deadlock.
			if got := b.State(); got != resilience.Closed {
				t.Errorf("State() inside op = %v, want closed", got)
			}
			return b.Do(ok)
		})
		if err != nil {
			t.Errorf("Do() = %v, want nil", err)
		}
	})
}

func TestConcurrent_HalfOpenAllowsOnlyOneProbeAtATime(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBreaker(t, clock, 1, 5*sec, 1)
	fail(t, b, 1)
	clock.Advance(5 * sec)
	wantState(t, b, resilience.HalfOpen)

	var inner error
	err := b.Do(func() error {
		// We are the probe. Anyone else arriving now is turned away.
		inner = b.Do(ok)
		return nil
	})
	if err != nil {
		t.Fatalf("probe Do() = %v, want nil", err)
	}
	if inner != resilience.ErrCircuitOpen {
		t.Fatalf("second call during the probe = %v, want ErrCircuitOpen", inner)
	}
	wantState(t, b, resilience.Closed)
}

func TestConcurrent_StaleResultsDoNotAffectANewerState(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBreaker(t, clock, 1, 5*sec, 1)

	// A slow call starts while the circuit is closed ...
	slowErr := b.Do(func() error {
		// ... meanwhile the circuit opens, waits, and a probe closes it again.
		_ = b.Do(func() error { return errBoom })
		clock.Advance(5 * sec)
		if err := b.Do(ok); err != nil {
			t.Errorf("probe = %v, want nil", err)
		}
		return errBoom // the slow call finally fails, but it is a result from the past
	})
	if slowErr != errBoom {
		t.Fatalf("Do() = %v, want errBoom", slowErr)
	}
	wantState(t, b, resilience.Closed)
}

func TestConcurrent_FakeClockIsRaceFree(t *testing.T) {
	clock := resilience.NewFakeClock()
	runParallel(8, func(int) {
		for range 100 {
			clock.Advance(ms)
			clock.Sleep(ms)
			_ = clock.Now()
			_ = clock.Sleeps()
		}
	})
}
