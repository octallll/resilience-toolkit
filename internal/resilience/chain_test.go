package resilience_test

import (
	"reflect"
	"testing"

	"resilience/internal/resilience"
)

func recorder(log *[]string, name string) resilience.Policy {
	return resilience.PolicyFunc(func(op func() error) error {
		*log = append(*log, name+":in")
		err := op()
		*log = append(*log, name+":out")
		return err
	})
}

func TestChain_FirstPolicyIsOutermost(t *testing.T) {
	var log []string
	c := chain(t, recorder(&log, "a"), recorder(&log, "b"), recorder(&log, "c"))

	err := c.Do(func() error {
		log = append(log, "op")
		return errBoom
	})
	if err != errBoom {
		t.Fatalf("Do() = %v, want errBoom", err)
	}
	want := []string{"a:in", "b:in", "c:in", "op", "c:out", "b:out", "a:out"}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("order = %v\nwant    %v", log, want)
	}
}

func TestChain_Empty(t *testing.T) {
	c := chain(t)
	calls := 0
	if err := c.Do(counter(&calls, errBoom)); err != errBoom {
		t.Fatalf("Do() = %v, want errBoom", err)
	}
	if calls != 1 {
		t.Fatalf("op called %d times, want 1", calls)
	}
}

func TestChain_Single(t *testing.T) {
	var log []string
	c := chain(t, recorder(&log, "only"))
	if err := c.Do(ok); err != nil {
		t.Fatalf("Do() = %v", err)
	}
	if want := []string{"only:in", "only:out"}; !reflect.DeepEqual(log, want) {
		t.Fatalf("order = %v, want %v", log, want)
	}
}

func TestChain_PolicyMayShortCircuit(t *testing.T) {
	deny := resilience.PolicyFunc(func(func() error) error { return errFatal })
	var log []string
	c := chain(t, recorder(&log, "a"), deny, recorder(&log, "never"))

	calls := 0
	if err := c.Do(counter(&calls, nil)); err != errFatal {
		t.Fatalf("Do() = %v, want errFatal", err)
	}
	if calls != 0 {
		t.Fatalf("op called %d times, want 0", calls)
	}
	if want := []string{"a:in", "a:out"}; !reflect.DeepEqual(log, want) {
		t.Fatalf("order = %v, want %v", log, want)
	}
}

func TestChain_IsReusableAndNestable(t *testing.T) {
	var log []string
	inner := chain(t, recorder(&log, "b"), recorder(&log, "c"))
	outer := chain(t, recorder(&log, "a"), inner)

	for range 2 {
		log = nil
		_ = outer.Do(ok)
		want := []string{"a:in", "b:in", "c:in", "c:out", "b:out", "a:out"}
		if !reflect.DeepEqual(log, want) {
			t.Fatalf("order = %v, want %v", log, want)
		}
	}
}

func TestChain_NilPolicyIsAnError(t *testing.T) {
	var log []string
	cases := map[string][]resilience.Policy{
		"nil in the middle": {recorder(&log, "a"), nil, recorder(&log, "b")},
		"nil first":         {nil, recorder(&log, "a")},
		"only nil":          {nil},
	}
	for name, policies := range cases {
		c, err := resilience.Chain(policies...)
		if c != nil {
			t.Errorf("%s: a policy was returned together with an error", name)
		}
		checkInvalid(t, name, err, resilience.ErrNilPolicy)
	}
	if len(log) != 0 {
		t.Fatalf("Chain must not run anything, log = %v", log)
	}
}

func TestChain_DoesNotAliasTheInputSlice(t *testing.T) {
	var log []string
	policies := []resilience.Policy{recorder(&log, "a"), recorder(&log, "b")}
	c := chain(t, policies...)

	policies[0] = recorder(&log, "EVIL")
	policies[1] = nil

	_ = c.Do(ok)
	want := []string{"a:in", "b:in", "b:out", "a:out"}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("order = %v, want %v", log, want)
	}
}

// The order of policies matters: these two tests document why.

func TestChain_LimiterOutsideBreaker_RejectionsDoNotTripTheBreaker(t *testing.T) {
	clock := resilience.NewFakeClock()
	bucket := newBucket(t, clock, 1, 10*sec)
	breaker := newBreaker(t, clock, 2, 5*sec, 1)
	c := chain(t, bucket, breaker)

	if err := c.Do(func() error { return errBoom }); err != errBoom {
		t.Fatalf("first call = %v, want errBoom", err)
	}
	for range 10 {
		if err := c.Do(ok); err != resilience.ErrRateLimited {
			t.Fatalf("Do() = %v, want ErrRateLimited", err)
		}
	}
	wantState(t, breaker, resilience.Closed)
}

func TestChain_BreakerOutsideLimiter_RejectionsCountAsFailures(t *testing.T) {
	clock := resilience.NewFakeClock()
	breaker := newBreaker(t, clock, 2, 5*sec, 1)
	bucket := newBucket(t, clock, 1, 10*sec)
	c := chain(t, breaker, bucket)

	if err := c.Do(ok); err != nil {
		t.Fatalf("first call = %v, want nil", err)
	}
	for range 2 {
		if err := c.Do(ok); err != resilience.ErrRateLimited {
			t.Fatalf("Do() = %v, want ErrRateLimited", err)
		}
	}
	wantState(t, breaker, resilience.Open)
	if err := c.Do(ok); err != resilience.ErrCircuitOpen {
		t.Fatalf("Do() = %v, want ErrCircuitOpen", err)
	}
}

// A policy that rejects the call must not make the chain pay for the policies behind it.
func TestChain_RejectionDoesNotAllocatePerPolicy(t *testing.T) {
	reject := resilience.PolicyFunc(func(func() error) error { return errBoom })
	pass := resilience.PolicyFunc(func(op func() error) error { return op() })
	c, err := resilience.Chain(reject, pass, pass, pass, pass, pass)
	if err != nil {
		t.Fatal(err)
	}
	op := func() error { return nil }
	if allocs := testing.AllocsPerRun(100, func() { _ = c.Do(op) }); allocs > 1 {
		t.Errorf("rejected Do allocated %v times, want at most 1", allocs)
	}
}
