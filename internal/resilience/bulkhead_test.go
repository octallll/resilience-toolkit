//go:build bonus

package resilience_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"resilience/internal/resilience"
)

func TestBulkhead_RunsOpAndReturnsItsError(t *testing.T) {
	b := newBulkhead(t, 2)

	calls := 0
	if err := b.Do(counter(&calls, nil)); err != nil {
		t.Fatalf("Do() = %v, want nil", err)
	}
	if err := b.Do(counter(&calls, errBoom)); err != errBoom {
		t.Fatalf("Do() = %v, want errBoom", err)
	}
	if calls != 2 {
		t.Fatalf("op called %d times, want 2", calls)
	}
	if got := b.InFlight(); got != 0 {
		t.Fatalf("InFlight() = %d after both calls returned, want 0", got)
	}
}

func TestBulkhead_NestedCallsFillTheSlots(t *testing.T) {
	b := newBulkhead(t, 2)

	calls := 0
	var innermost error
	err := b.Do(func() error { // slot 1
		if got := b.InFlight(); got != 1 {
			t.Errorf("InFlight() = %d at depth 1, want 1", got)
		}
		return b.Do(func() error { // slot 2
			if got := b.InFlight(); got != 2 {
				t.Errorf("InFlight() = %d at depth 2, want 2", got)
			}
			innermost = b.Do(counter(&calls, nil)) // no slot left
			return nil
		})
	})
	if err != nil {
		t.Fatalf("Do() = %v, want nil", err)
	}
	if innermost != resilience.ErrBulkheadFull {
		t.Fatalf("third nested Do() = %v, want ErrBulkheadFull", innermost)
	}
	if calls != 0 {
		t.Fatalf("op called %d times on a full bulkhead, want 0", calls)
	}
	if got := b.InFlight(); got != 0 {
		t.Fatalf("InFlight() = %d after unwinding, want 0", got)
	}
}

func TestBulkhead_LimitOneRejectsImmediately(t *testing.T) {
	b := newBulkhead(t, 1)
	var inner error
	_ = b.Do(func() error {
		inner = b.Do(ok)
		return nil
	})
	if inner != resilience.ErrBulkheadFull {
		t.Fatalf("nested Do() = %v, want ErrBulkheadFull", inner)
	}
}

func TestBulkhead_SlotIsFreedAfterError(t *testing.T) {
	b := newBulkhead(t, 1)
	for range 3 {
		if err := b.Do(func() error { return errBoom }); err != errBoom {
			t.Fatalf("Do() = %v, want errBoom (not ErrBulkheadFull: the slot must be freed)", err)
		}
	}
}

func TestBulkhead_SlotIsFreedAfterPanic(t *testing.T) {
	b := newBulkhead(t, 1)

	func() {
		defer func() {
			if r := recover(); r != "kaboom" {
				t.Errorf("recovered %v, want the original panic value", r)
			}
		}()
		_ = b.Do(func() error { panic("kaboom") })
	}()

	if got := b.InFlight(); got != 0 {
		t.Fatalf("InFlight() = %d after a panic, want 0", got)
	}
	if err := b.Do(ok); err != nil {
		t.Fatalf("Do() after a panic = %v, want nil", err)
	}
}

func TestBulkhead_ParallelCallsBeyondTheLimitAreRejected(t *testing.T) {
	const (
		limit   = 3
		callers = 10
	)
	b := newBulkhead(t, limit)

	entered := make(chan struct{}, callers)
	release := make(chan struct{})
	results := make(chan error, callers)

	for range callers {
		go func() {
			results <- b.Do(func() error {
				entered <- struct{}{}
				<-release
				return nil
			})
		}()
	}

	// Exactly `limit` calls get in; the others bounce off at once.
	for range limit {
		<-entered
	}
	for range callers - limit {
		if err := <-results; err != resilience.ErrBulkheadFull {
			t.Fatalf("rejected call returned %v, want ErrBulkheadFull", err)
		}
	}
	if got := b.InFlight(); got != limit {
		t.Fatalf("InFlight() = %d, want %d", got, limit)
	}

	close(release)
	for range limit {
		if err := <-results; err != nil {
			t.Fatalf("admitted call returned %v, want nil", err)
		}
	}
	if got := b.InFlight(); got != 0 {
		t.Fatalf("InFlight() = %d at the end, want 0", got)
	}
	if err := b.Do(ok); err != nil {
		t.Fatalf("Do() after everything finished = %v, want nil", err)
	}
}

func TestBulkhead_NeverExceedsTheLimitUnderChurn(t *testing.T) {
	const limit = 4
	b := newBulkhead(t, limit)

	var running, peak, rejected, admitted atomic.Int64
	var wg sync.WaitGroup
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				err := b.Do(func() error {
					cur := running.Add(1)
					for {
						old := peak.Load()
						if cur <= old || peak.CompareAndSwap(old, cur) {
							break
						}
					}
					running.Add(-1)
					return nil
				})
				if err == resilience.ErrBulkheadFull {
					rejected.Add(1)
				} else {
					admitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if got := peak.Load(); got > limit {
		t.Fatalf("peak concurrency = %d, want at most %d", got, limit)
	}
	if rejected.Load()+admitted.Load() != 40*200 {
		t.Fatal("some calls vanished")
	}
	if got := b.InFlight(); got != 0 {
		t.Fatalf("InFlight() = %d at the end, want 0", got)
	}
}

func TestBulkhead_DoDoesNotAllocate(t *testing.T) {
	b := newBulkhead(t, 1)
	allocs := testing.AllocsPerRun(1000, func() { _ = b.Do(ok) })
	if allocs != 0 {
		t.Fatalf("Do() allocates %v times per call, want 0", allocs)
	}
}

func TestBulkhead_WorksInAChain(t *testing.T) {
	clock := resilience.NewFakeClock()
	bulkhead := newBulkhead(t, 1)
	breaker := newBreaker(t, clock, 1, 5*sec, 1)
	policy := chain(t, bulkhead, breaker)

	// A rejection by the (outer) bulkhead never reaches the breaker.
	var inner error
	_ = policy.Do(func() error {
		inner = policy.Do(ok)
		return nil
	})
	if inner != resilience.ErrBulkheadFull {
		t.Fatalf("nested call = %v, want ErrBulkheadFull", inner)
	}
	wantState(t, breaker, resilience.Closed)
}

func TestBulkhead_ImplementsPolicy(t *testing.T) {
	var _ resilience.Policy = (*resilience.Bulkhead)(nil)
}

func TestBulkhead_InvalidArguments(t *testing.T) {
	for _, limit := range []int{0, -1} {
		b, err := resilience.NewBulkhead(limit)
		if b != nil {
			t.Errorf("limit %d: a bulkhead was returned together with an error", limit)
		}
		checkInvalid(t, "limit", err, resilience.ErrInvalidLimit)
	}
}
