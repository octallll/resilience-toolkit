package resilience_test

import (
	"testing"
	"time"

	"resilience/internal/resilience"
)

func drain(b *resilience.TokenBucket) int {
	n := 0
	for b.Allow() {
		n++
	}
	return n
}

func TestTokenBucket_StartsFull(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 3, sec)

	if got := b.Tokens(); got != 3 {
		t.Fatalf("Tokens() = %d, want 3", got)
	}
	if got := drain(b); got != 3 {
		t.Fatalf("burst = %d, want 3", got)
	}
	if b.Allow() {
		t.Fatal("Allow() = true on an empty bucket")
	}
}

func TestTokenBucket_TokensDoesNotConsume(t *testing.T) {
	b := newBucket(t, resilience.NewFakeClock(), 2, sec)
	for range 5 {
		if got := b.Tokens(); got != 2 {
			t.Fatalf("Tokens() = %d, want 2", got)
		}
	}
	if got := drain(b); got != 2 {
		t.Fatalf("burst after Tokens() calls = %d, want 2", got)
	}
}

func TestTokenBucket_RefillsOneTokenPerInterval(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 5, 10*sec)
	drain(b)

	clock.Advance(9*sec + 999*ms)
	if got := b.Tokens(); got != 0 {
		t.Fatalf("just before the interval: Tokens() = %d, want 0", got)
	}
	clock.Advance(1 * ms)
	if got := b.Tokens(); got != 1 {
		t.Fatalf("after one interval: Tokens() = %d, want 1", got)
	}
	clock.Advance(30 * sec)
	if got := b.Tokens(); got != 4 {
		t.Fatalf("after three more intervals: Tokens() = %d, want 4", got)
	}
}

func TestTokenBucket_NeverExceedsCapacity(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 3, sec)
	drain(b)

	clock.Advance(100 * sec)
	if got := b.Tokens(); got != 3 {
		t.Fatalf("Tokens() = %d, want capacity 3", got)
	}
	if got := drain(b); got != 3 {
		t.Fatalf("burst = %d, want 3", got)
	}
}

func TestTokenBucket_HugeJumpDoesNotOverflow(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 3, time.Nanosecond)
	drain(b)

	clock.Advance(100 * 365 * 24 * time.Hour)
	if got := b.Tokens(); got != 3 {
		t.Fatalf("Tokens() = %d, want 3", got)
	}
}

func TestTokenBucket_KeepsFractionalRemainder(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 10, sec)
	drain(b)

	steps := []struct {
		advance time.Duration
		want    int
	}{
		{2500 * ms, 2}, // 2.5s elapsed: two tokens, half an interval is kept
		{500 * ms, 3},  // 3.0s elapsed
		{900 * ms, 3},  // 3.9s elapsed
		{100 * ms, 4},  // 4.0s elapsed
	}
	for i, s := range steps {
		clock.Advance(s.advance)
		if got := b.Tokens(); got != s.want {
			t.Fatalf("step %d: Tokens() = %d, want %d", i, got, s.want)
		}
	}
}

func TestTokenBucket_PartialProgressSurvivesAllowAndTokens(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 3, 10*sec)
	drain(b) // t = 0, the bucket is empty

	clock.Advance(7 * sec)
	if b.Allow() {
		t.Fatal("Allow() = true after 7s of a 10s interval")
	}
	_ = b.Tokens() // looking must not reset the progress either
	clock.Advance(3 * sec)
	if !b.Allow() {
		t.Fatal("Allow() = false 10s after the bucket became empty")
	}
}

func TestTokenBucket_FullBucketDoesNotBankTime(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 2, 10*sec)

	// The bucket sits full for 9 seconds, then one token is taken.
	clock.Advance(9 * sec)
	if !b.Allow() {
		t.Fatal("Allow() = false on a full bucket")
	}
	// The replacement token starts cooking only now, so one more second is not enough.
	clock.Advance(1 * sec)
	if got := b.Tokens(); got != 1 {
		t.Fatalf("Tokens() = %d, want 1: a full bucket must not accumulate elapsed time", got)
	}
	clock.Advance(9 * sec)
	if got := b.Tokens(); got != 2 {
		t.Fatalf("Tokens() = %d, want 2", got)
	}
}

func TestTokenBucket_FullBucketDoesNotBankTime_ViaTokens(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 2, 10*sec)

	clock.Advance(9 * sec)
	_ = b.Tokens()
	b.Allow()
	clock.Advance(1 * sec)
	if got := b.Tokens(); got != 1 {
		t.Fatalf("Tokens() = %d, want 1", got)
	}
}

func TestTokenBucket_Do(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 2, sec)

	calls := 0
	if err := b.Do(counter(&calls, nil)); err != nil {
		t.Fatalf("Do() = %v, want nil", err)
	}
	// op's own error is returned as is, and the token is still spent.
	if err := b.Do(counter(&calls, errBoom)); err != errBoom {
		t.Fatalf("Do() = %v, want errBoom", err)
	}
	// No tokens left: op must not run.
	if err := b.Do(counter(&calls, nil)); err != resilience.ErrRateLimited {
		t.Fatalf("Do() = %v, want ErrRateLimited", err)
	}
	if calls != 2 {
		t.Fatalf("op called %d times, want 2", calls)
	}

	clock.Advance(sec)
	if err := b.Do(counter(&calls, nil)); err != nil {
		t.Fatalf("Do() after refill = %v, want nil", err)
	}
}

func TestTokenBucket_ImplementsPolicy(t *testing.T) {
	var _ resilience.Policy = (*resilience.TokenBucket)(nil)
}

func TestTokenBucket_AllowDoesNotAllocate(t *testing.T) {
	clock := resilience.NewFakeClock()
	b := newBucket(t, clock, 1, sec)
	allocs := testing.AllocsPerRun(1000, func() {
		clock.Advance(sec)
		b.Allow()
	})
	if allocs != 0 {
		t.Fatalf("Allow() allocates %v times per call, want 0", allocs)
	}
}

func TestTokenBucket_InvalidArguments(t *testing.T) {
	clock := resilience.NewFakeClock()
	cases := []struct {
		name     string
		clock    resilience.Clock
		capacity int
		interval time.Duration
		want     []error
	}{
		{"nil clock", nil, 1, sec, []error{resilience.ErrNilClock}},
		{"zero capacity", clock, 0, sec, []error{resilience.ErrInvalidCapacity}},
		{"negative capacity", clock, -1, sec, []error{resilience.ErrInvalidCapacity}},
		{"zero interval", clock, 1, 0, []error{resilience.ErrInvalidInterval}},
		{"negative interval", clock, 1, -sec, []error{resilience.ErrInvalidInterval}},
		{"everything wrong", nil, 0, 0, []error{
			resilience.ErrNilClock, resilience.ErrInvalidCapacity, resilience.ErrInvalidInterval,
		}},
	}
	for _, c := range cases {
		b, err := resilience.NewTokenBucket(c.clock, c.capacity, c.interval)
		if b != nil {
			t.Errorf("%s: a bucket was returned together with an error", c.name)
		}
		checkInvalid(t, c.name, err, c.want...)
	}
}
