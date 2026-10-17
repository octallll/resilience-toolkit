//go:build bonus

package resilience_test

import (
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"resilience/internal/resilience"
)

// scriptedRand returns the prepared numbers one by one and counts the calls.
type scriptedRand struct {
	values []float64
	calls  int
}

func (s *scriptedRand) Float64() float64 {
	v := s.values[s.calls%len(s.values)]
	s.calls++
	return v
}

func newFullJitter(t *testing.T, inner resilience.Backoff, rnd resilience.Rand) *resilience.FullJitter {
	t.Helper()
	j, err := resilience.NewFullJitter(inner, rnd)
	if err != nil {
		t.Fatalf("NewFullJitter: %v", err)
	}
	return j
}

func newEqualJitter(t *testing.T, inner resilience.Backoff, rnd resilience.Rand) *resilience.EqualJitter {
	t.Helper()
	j, err := resilience.NewEqualJitter(inner, rnd)
	if err != nil {
		t.Fatalf("NewEqualJitter: %v", err)
	}
	return j
}

func TestFullJitter_ScalesTheInnerDelayByTheRandomNumber(t *testing.T) {
	rnd := &scriptedRand{values: []float64{0, 0.5, 0.25, math.Nextafter(1, 0)}}
	j := newFullJitter(t, resilience.ConstantBackoff{D: 100 * ms}, rnd)

	want := []time.Duration{0, 50 * ms, 25 * ms}
	for i, w := range want {
		if got := j.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %v, want %v", i+1, got, w)
		}
	}
	if got := j.Delay(4); got < 99*ms || got >= 100*ms {
		t.Errorf("Delay(4) = %v with the largest possible random number, want just below 100ms", got)
	}
}

func TestFullJitter_UsesTheInnerDelayOfTheSameAttempt(t *testing.T) {
	inner := newExp(t, 100*ms, sec, 2)
	rnd := &scriptedRand{values: []float64{0.5}}
	j := newFullJitter(t, inner, rnd)

	want := []time.Duration{50 * ms, 100 * ms, 200 * ms, 400 * ms, 500 * ms, 500 * ms}
	for i, w := range want {
		if got := j.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestJitter_AsksRandExactlyOncePerPositiveDelay(t *testing.T) {
	for name, build := range map[string]func(resilience.Backoff, resilience.Rand) resilience.Backoff{
		"full":  func(b resilience.Backoff, r resilience.Rand) resilience.Backoff { return newFullJitter(t, b, r) },
		"equal": func(b resilience.Backoff, r resilience.Rand) resilience.Backoff { return newEqualJitter(t, b, r) },
	} {
		rnd := &scriptedRand{values: []float64{0.5}}
		j := build(resilience.ConstantBackoff{D: sec}, rnd)
		for range 5 {
			j.Delay(1)
		}
		if rnd.calls != 5 {
			t.Errorf("%s: Float64 called %d times for 5 delays, want 5", name, rnd.calls)
		}

		// A non-positive inner delay stays 0 and does not touch the random source.
		rnd = &scriptedRand{values: []float64{0.5}}
		for _, d := range []time.Duration{0, -sec} {
			j = build(resilience.ConstantBackoff{D: d}, rnd)
			if got := j.Delay(1); got != 0 {
				t.Errorf("%s: Delay() with inner %v = %v, want 0", name, d, got)
			}
		}
		if rnd.calls != 0 {
			t.Errorf("%s: Float64 called %d times for non-positive delays, want 0", name, rnd.calls)
		}
	}
}

func TestFullJitter_StaysInRangeAndSpreadsOverIt(t *testing.T) {
	const (
		d = 10 * sec
		n = 20000
	)
	rng := rand.New(rand.NewPCG(1, 2))
	j := newFullJitter(t, resilience.ConstantBackoff{D: d}, rng)

	var sum, lo, hi = time.Duration(0), d, time.Duration(0)
	for range n {
		got := j.Delay(1)
		if got < 0 || got >= d {
			t.Fatalf("Delay() = %v, want a value in [0, %v)", got, d)
		}
		sum += got
		lo, hi = min(lo, got), max(hi, got)
	}
	if mean := sum / n; mean < d*45/100 || mean > d*55/100 {
		t.Errorf("mean = %v, want about %v", mean, d/2)
	}
	if lo > d/100 || hi < d*99/100 {
		t.Errorf("values span only [%v, %v], want nearly the whole [0, %v)", lo, hi, d)
	}
}

func TestEqualJitter_KeepsAtLeastHalfOfTheDelay(t *testing.T) {
	rnd := &scriptedRand{values: []float64{0, 0.5, math.Nextafter(1, 0)}}
	j := newEqualJitter(t, resilience.ConstantBackoff{D: 100 * ms}, rnd)

	if got := j.Delay(1); got != 50*ms {
		t.Errorf("Delay() with 0 = %v, want 50ms", got)
	}
	if got := j.Delay(1); got != 75*ms {
		t.Errorf("Delay() with 0.5 = %v, want 75ms", got)
	}
	if got := j.Delay(1); got < 99*ms || got >= 100*ms {
		t.Errorf("Delay() with the largest number = %v, want just below 100ms", got)
	}
}

func TestEqualJitter_OddDelayStaysInRange(t *testing.T) {
	// d = 101ns: half = 50ns, the random part is drawn from the remaining 51ns.
	for _, f := range []float64{0, 0.3, 0.5, math.Nextafter(1, 0)} {
		j := newEqualJitter(t, resilience.ConstantBackoff{D: 101}, &scriptedRand{values: []float64{f}})
		if got := j.Delay(1); got < 50 || got >= 101 {
			t.Errorf("Delay() with %v = %v, want a value in [50ns, 101ns)", f, got)
		}
	}
	// The whole range [50, 101) must be reachable, including its last nanosecond.
	top := newEqualJitter(t, resilience.ConstantBackoff{D: 101}, &scriptedRand{values: []float64{math.Nextafter(1, 0)}})
	if got := top.Delay(1); got != 100 {
		t.Errorf("Delay() with the largest random number = %v, want 100ns", got)
	}
}

// Rand promises [0, 1), but a broken one must not make the delay negative or longer than planned.
func TestJitter_ClampsAMisbehavingRand(t *testing.T) {
	bad := &scriptedRand{values: []float64{1, 2.5, -0.5}}
	full := newFullJitter(t, resilience.ConstantBackoff{D: 100 * ms}, bad)
	want := []time.Duration{100*ms - 1, 100*ms - 1, 0}
	for i, w := range want {
		if got := full.Delay(1); got != w {
			t.Errorf("FullJitter.Delay() #%d = %v, want %v", i+1, got, w)
		}
	}

	bad = &scriptedRand{values: []float64{1, 2.5, -0.5}}
	equal := newEqualJitter(t, resilience.ConstantBackoff{D: 100 * ms}, bad)
	want = []time.Duration{100*ms - 1, 100*ms - 1, 50 * ms}
	for i, w := range want {
		if got := equal.Delay(1); got != w {
			t.Errorf("EqualJitter.Delay() #%d = %v, want %v", i+1, got, w)
		}
	}
}

func TestEqualJitter_StaysInRangeOnAverageThreeQuarters(t *testing.T) {
	const (
		d = 10 * sec
		n = 20000
	)
	j := newEqualJitter(t, resilience.ConstantBackoff{D: d}, rand.New(rand.NewPCG(3, 4)))
	var sum time.Duration
	for range n {
		got := j.Delay(1)
		if got < d/2 || got >= d {
			t.Fatalf("Delay() = %v, want a value in [%v, %v)", got, d/2, d)
		}
		sum += got
	}
	if mean := sum / n; mean < d*72/100 || mean > d*78/100 {
		t.Errorf("mean = %v, want about %v", mean, d*3/4)
	}
}

func TestJitter_NeverOverflowsOrReachesTheTop(t *testing.T) {
	huge := time.Duration(math.MaxInt64)
	top := math.Nextafter(1, 0)

	full := newFullJitter(t, resilience.ConstantBackoff{D: huge}, &scriptedRand{values: []float64{top, 0.5, 0}})
	for range 3 {
		if got := full.Delay(1); got < 0 || got >= huge {
			t.Fatalf("FullJitter.Delay() = %v, want a value in [0, MaxInt64)", got)
		}
	}
	equal := newEqualJitter(t, resilience.ConstantBackoff{D: huge}, &scriptedRand{values: []float64{top, 0.5, 0}})
	for range 3 {
		if got := equal.Delay(1); got < huge/2 || got >= huge {
			t.Fatalf("EqualJitter.Delay() = %v, want a value in [MaxInt64/2, MaxInt64)", got)
		}
	}
	// A tiny delay can never become 0 or negative by mistake.
	one := newFullJitter(t, resilience.ConstantBackoff{D: 1}, &scriptedRand{values: []float64{top}})
	if got := one.Delay(1); got != 0 {
		t.Errorf("Delay() of 1ns = %v, want 0 (the only value in [0, 1ns))", got)
	}
}

func TestJitter_ComposesWithRetry(t *testing.T) {
	clock := resilience.NewFakeClock()
	rnd := &scriptedRand{values: []float64{0.5, 0.25, 0.75}}
	r := newRetry(t, clock, 4, newFullJitter(t, newExp(t, 100*ms, sec, 2), rnd))

	calls := 0
	_ = r.Do(counter(&calls, errBoom))
	// inner delays are 100ms, 200ms, 400ms
	if want := []time.Duration{50 * ms, 50 * ms, 300 * ms}; !reflect.DeepEqual(clock.Sleeps(), want) {
		t.Fatalf("sleeps = %v, want %v", clock.Sleeps(), want)
	}
}

func TestJitter_ZeroResultMeansNoSleep(t *testing.T) {
	clock := resilience.NewFakeClock()
	r := newRetry(t, clock, 3, newFullJitter(t, resilience.ConstantBackoff{D: sec}, &scriptedRand{values: []float64{0}}))
	calls := 0
	_ = r.Do(counter(&calls, errBoom))
	if calls != 3 || len(clock.Sleeps()) != 0 {
		t.Fatalf("calls = %d, sleeps = %v; want 3 calls and no sleeps", calls, clock.Sleeps())
	}
}

func TestJitter_InvalidArguments(t *testing.T) {
	inner := resilience.ConstantBackoff{D: sec}
	rnd := rand.New(rand.NewPCG(1, 1))
	cases := []struct {
		name  string
		inner resilience.Backoff
		rnd   resilience.Rand
		want  []error
	}{
		{"nil backoff", nil, rnd, []error{resilience.ErrNilBackoff}},
		{"nil rand", inner, nil, []error{resilience.ErrNilRand}},
		{"both nil", nil, nil, []error{resilience.ErrNilBackoff, resilience.ErrNilRand}},
	}
	for _, c := range cases {
		f, err := resilience.NewFullJitter(c.inner, c.rnd)
		if f != nil {
			t.Errorf("full/%s: a jitter was returned together with an error", c.name)
		}
		checkInvalid(t, "full/"+c.name, err, c.want...)

		e, err := resilience.NewEqualJitter(c.inner, c.rnd)
		if e != nil {
			t.Errorf("equal/%s: a jitter was returned together with an error", c.name)
		}
		checkInvalid(t, "equal/"+c.name, err, c.want...)
	}
}

func TestJitter_ImplementsBackoff(t *testing.T) {
	var _ resilience.Backoff = (*resilience.FullJitter)(nil)
	var _ resilience.Backoff = (*resilience.EqualJitter)(nil)
	var _ resilience.Rand = (*rand.Rand)(nil)
}

// exclusiveRand fails if two goroutines are ever inside Float64 at the same time.
type exclusiveRand struct {
	inside     atomic.Int32
	violations atomic.Int32
}

func (r *exclusiveRand) Float64() float64 {
	if r.inside.Add(1) != 1 {
		r.violations.Add(1)
	}
	runtime.Gosched()
	r.inside.Add(-1)
	return 0.5
}

// Retry is stateless and is naturally shared between goroutines, so jitter must
// not pass its callers' concurrency on to a Rand that is not goroutine-safe.
func TestJitter_SerializesCallsToRand(t *testing.T) {
	for name, build := range map[string]func(resilience.Backoff, resilience.Rand) resilience.Backoff{
		"full":  func(b resilience.Backoff, r resilience.Rand) resilience.Backoff { return newFullJitter(t, b, r) },
		"equal": func(b resilience.Backoff, r resilience.Rand) resilience.Backoff { return newEqualJitter(t, b, r) },
	} {
		rnd := &exclusiveRand{}
		j := build(resilience.ConstantBackoff{D: sec}, rnd)
		runParallel(8, func(int) {
			for range 200 {
				j.Delay(1)
			}
		})
		if v := rnd.violations.Load(); v != 0 {
			t.Errorf("%s: Rand was entered concurrently %d times", name, v)
		}
	}
}
