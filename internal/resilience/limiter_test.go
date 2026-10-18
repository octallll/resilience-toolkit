//go:build bonus

package resilience_test

import (
	"math/rand/v2"
	"sort"
	"testing"
	"time"

	"resilience/internal/resilience"
)

func idleKeys[K comparable](l *resilience.Limiter[K]) map[K]bool {
	keys := make(map[K]bool)
	for k := range l.Idle() {
		keys[k] = true
	}
	return keys
}

func TestLimiter_KeysAreIndependent(t *testing.T) {
	l := newLimiter[string](t, resilience.NewFakeClock(), 2, sec)

	if !l.Allow("alice") || !l.Allow("alice") {
		t.Fatal("alice should have a burst of 2")
	}
	if l.Allow("alice") {
		t.Fatal("alice should be limited")
	}
	if !l.Allow("bob") {
		t.Fatal("bob must not be affected by alice")
	}
}

func TestLimiter_RefillsPerKey(t *testing.T) {
	clock := resilience.NewFakeClock()
	l := newLimiter[string](t, clock, 2, 10*sec)
	l.Allow("a")
	l.Allow("a")
	clock.Advance(5 * sec)
	l.Allow("b")
	l.Allow("b")

	clock.Advance(5 * sec) // a: 10s since drained, b: 5s
	if got := l.Tokens("a"); got != 1 {
		t.Errorf("Tokens(a) = %d, want 1", got)
	}
	if got := l.Tokens("b"); got != 0 {
		t.Errorf("Tokens(b) = %d, want 0", got)
	}
}

func TestLimiter_Do(t *testing.T) {
	l := newLimiter[int](t, resilience.NewFakeClock(), 1, sec)

	calls := 0
	if err := l.Do(1, counter(&calls, errBoom)); err != errBoom {
		t.Fatalf("Do() = %v, want errBoom", err)
	}
	if err := l.Do(1, counter(&calls, nil)); err != resilience.ErrRateLimited {
		t.Fatalf("Do() = %v, want ErrRateLimited", err)
	}
	if err := l.Do(2, counter(&calls, nil)); err != nil {
		t.Fatalf("Do() for another key = %v, want nil", err)
	}
	if calls != 2 {
		t.Fatalf("op called %d times, want 2", calls)
	}
}

func TestLimiter_StructKeys(t *testing.T) {
	type key struct {
		user string
		path string
	}
	l := newLimiter[key](t, resilience.NewFakeClock(), 1, sec)
	if !l.Allow(key{"u", "/a"}) || l.Allow(key{"u", "/a"}) {
		t.Fatal("same struct key must share a bucket")
	}
	if !l.Allow(key{"u", "/b"}) {
		t.Fatal("different struct key must have its own bucket")
	}
}

func TestLimiter_TokensAndSizeDoNotCreateBuckets(t *testing.T) {
	l := newLimiter[string](t, resilience.NewFakeClock(), 4, sec)

	if got := l.Tokens("ghost"); got != 4 {
		t.Fatalf("Tokens(unknown) = %d, want capacity 4", got)
	}
	if got := l.Size(); got != 0 {
		t.Fatalf("Size() = %d after Tokens(unknown), want 0", got)
	}
	l.Allow("a")
	l.Allow("b")
	l.Allow("a")
	if got := l.Size(); got != 2 {
		t.Fatalf("Size() = %d, want 2", got)
	}
}

func TestLimiter_Sweep(t *testing.T) {
	clock := resilience.NewFakeClock()
	l := newLimiter[string](t, clock, 2, sec)
	l.Allow("a")
	l.Allow("a") // a: empty
	l.Allow("b") // b: 1 of 2

	if got := l.Sweep(); got != 0 {
		t.Fatalf("Sweep() = %d, want 0: nothing is idle", got)
	}
	if got := l.Size(); got != 2 {
		t.Fatalf("Size() = %d, want 2", got)
	}

	clock.Advance(sec) // a: 1 of 2, b: full
	if got := l.Sweep(); got != 1 {
		t.Fatalf("Sweep() = %d, want 1 (only b)", got)
	}
	if got := l.Size(); got != 1 {
		t.Fatalf("Size() = %d, want 1", got)
	}

	// Neither survivor nor victim changed its behaviour.
	if !l.Allow("a") || l.Allow("a") {
		t.Fatal("a must have had exactly one token left")
	}
	if got := l.Tokens("b"); got != 2 {
		t.Fatalf("Tokens(b) = %d, want 2", got)
	}
}

func TestLimiter_SweepDoesNotChangeObservableBehaviour(t *testing.T) {
	const (
		keys  = 6
		steps = 5000
	)
	clockA, clockB := resilience.NewFakeClock(), resilience.NewFakeClock()
	swept := newLimiter[int](t, clockA, 3, 100*ms)
	plain := newLimiter[int](t, clockB, 3, 100*ms)
	rng := rand.New(rand.NewPCG(42, 7))

	for i := range steps {
		switch rng.IntN(4) {
		case 0:
			d := time.Duration(rng.IntN(250)) * ms
			clockA.Advance(d)
			clockB.Advance(d)
		case 1:
			swept.Sweep()
		default:
			k := rng.IntN(keys)
			if a, b := swept.Allow(k), plain.Allow(k); a != b {
				t.Fatalf("step %d: Allow(%d) = %v with sweeping, %v without", i, k, a, b)
			}
		}
		k := rng.IntN(keys)
		if a, b := swept.Tokens(k), plain.Tokens(k); a != b {
			t.Fatalf("step %d: Tokens(%d) = %d with sweeping, %d without", i, k, a, b)
		}
	}
}

func TestLimiter_Idle(t *testing.T) {
	clock := resilience.NewFakeClock()
	l := newLimiter[string](t, clock, 2, sec)
	l.Allow("a")
	l.Allow("a")
	l.Allow("b")
	l.Allow("c")
	clock.Advance(sec) // a: 1, b: 2 (idle), c: 2 (idle)

	got := idleKeys(l)
	if len(got) != 2 || !got["b"] || !got["c"] {
		t.Fatalf("Idle() = %v, want {b, c}", got)
	}
	if l.Size() != 3 {
		t.Fatalf("Size() = %d: Idle must not delete anything", l.Size())
	}
}

func TestLimiter_IdleSupportsEarlyBreak(t *testing.T) {
	clock := resilience.NewFakeClock()
	l := newLimiter[int](t, clock, 1, sec)
	for k := range 5 {
		l.Allow(k)
	}
	clock.Advance(sec)

	n := 0
	for range l.Idle() {
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		t.Fatalf("visited %d keys before break, want 2", n)
	}
}

func TestLimiter_IdleLoopBodyMayCallTheLimiter(t *testing.T) {
	clock := resilience.NewFakeClock()
	l := newLimiter[int](t, clock, 1, sec)
	for k := range 4 {
		l.Allow(k)
	}
	clock.Advance(sec)

	withTimeout(t, 2*sec, func() {
		for k := range l.Idle() {
			l.Allow(k)
			l.Tokens(k)
			l.Sweep()
			_ = l.Size()
		}
	})
}

func TestLimiter_IdleOfAnEmptyLimiter(t *testing.T) {
	l := newLimiter[string](t, resilience.NewFakeClock(), 1, sec)
	var keys []string
	for k := range l.Idle() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) != 0 {
		t.Fatalf("Idle() = %v, want nothing", keys)
	}
}

func TestLimiter_AllowOnExistingKeyDoesNotAllocate(t *testing.T) {
	clock := resilience.NewFakeClock()
	l := newLimiter[string](t, clock, 1, sec)
	l.Allow("user-42")
	allocs := testing.AllocsPerRun(1000, func() {
		clock.Advance(sec)
		l.Allow("user-42")
	})
	if allocs != 0 {
		t.Fatalf("Allow() allocates %v times per call, want 0", allocs)
	}
}

func TestLimiter_ImplementsNothingByAccident(t *testing.T) {
	// Limiter is keyed, so it intentionally has no Do(op) method and is not a Policy.
	var x any = newLimiter[string](t, resilience.NewFakeClock(), 1, sec)
	if _, isPolicy := x.(resilience.Policy); isPolicy {
		t.Fatal("Limiter must not implement Policy")
	}
}

func TestLimiter_InvalidArguments(t *testing.T) {
	clock := resilience.NewFakeClock()
	// Exactly the same errors as for a single bucket.
	cases := []struct {
		name     string
		clock    resilience.Clock
		capacity int
		interval time.Duration
		want     []error
	}{
		{"nil clock", nil, 1, sec, []error{resilience.ErrNilClock}},
		{"zero capacity", clock, 0, sec, []error{resilience.ErrInvalidCapacity}},
		{"zero interval", clock, 1, 0, []error{resilience.ErrInvalidInterval}},
		{"everything wrong", nil, -1, -sec, []error{
			resilience.ErrNilClock, resilience.ErrInvalidCapacity, resilience.ErrInvalidInterval,
		}},
	}
	for _, c := range cases {
		l, err := resilience.NewLimiter[string](c.clock, c.capacity, c.interval)
		if l != nil {
			t.Errorf("%s: a limiter was returned together with an error", c.name)
		}
		checkInvalid(t, c.name, err, c.want...)
	}
}

// Keys are spread over independently locked shards; the observable behaviour must not depend on it.
func TestLimiter_ManyKeysAcrossShards(t *testing.T) {
	const n = 1000
	clock := resilience.NewFakeClock()
	l := newLimiter[int](t, clock, 2, sec)

	for k := range n {
		if !l.Allow(k) {
			t.Fatalf("Allow(%d) = false, want true", k)
		}
	}
	if got := l.Size(); got != n {
		t.Fatalf("Size() = %d, want %d", got, n)
	}
	for k := range n {
		if got := l.Tokens(k); got != 1 {
			t.Fatalf("Tokens(%d) = %d, want 1", k, got)
		}
	}
	if got := l.Sweep(); got != 0 {
		t.Fatalf("Sweep() with no full buckets = %d, want 0", got)
	}
	if got := len(idleKeys(l)); got != 0 {
		t.Fatalf("Idle() yielded %d keys, want 0", got)
	}

	clock.Advance(sec)
	if got := len(idleKeys(l)); got != n {
		t.Fatalf("Idle() yielded %d keys, want %d", got, n)
	}
	if got := l.Sweep(); got != n {
		t.Fatalf("Sweep() = %d, want %d", got, n)
	}
	if got := l.Size(); got != 0 {
		t.Fatalf("Size() after Sweep = %d, want 0", got)
	}
}
