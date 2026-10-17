//go:build bonus

package resilience_test

import (
	"math"
	"testing"
	"time"

	"resilience/internal/resilience"
)

func TestConstantBackoff(t *testing.T) {
	b := resilience.ConstantBackoff{D: 250 * ms}
	for _, attempt := range []int{1, 2, 10, 1 << 30} {
		if got := b.Delay(attempt); got != 250*ms {
			t.Errorf("Delay(%d) = %v, want 250ms", attempt, got)
		}
	}
}

func TestExponentialBackoff_Growth(t *testing.T) {
	b := newExp(t, 100*ms, sec, 2)
	want := []time.Duration{100 * ms, 200 * ms, 400 * ms, 800 * ms, sec, sec, sec}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestExponentialBackoff_OtherFactor(t *testing.T) {
	b := newExp(t, 10*ms, 5*sec, 3)
	want := []time.Duration{10 * ms, 30 * ms, 90 * ms, 270 * ms}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestExponentialBackoff_CapIsInclusive(t *testing.T) {
	b := newExp(t, 100*ms, 400*ms, 2)
	if got := b.Delay(3); got != 400*ms {
		t.Errorf("Delay(3) = %v, want exactly Max", got)
	}
	if got := b.Delay(4); got != 400*ms {
		t.Errorf("Delay(4) = %v, want Max", got)
	}
}

func TestExponentialBackoff_NoOverflowWithCap(t *testing.T) {
	b := newExp(t, 100*ms, time.Minute, 2)
	for _, attempt := range []int{64, 100, 1 << 20, math.MaxInt} {
		if got := b.Delay(attempt); got != time.Minute {
			t.Errorf("Delay(%d) = %v, want 1m", attempt, got)
		}
	}
}

func TestExponentialBackoff_UnlimitedSaturates(t *testing.T) {
	maxDur := time.Duration(math.MaxInt64)

	b := newExp(t, sec, 0, 10)
	if got := b.Delay(100); got != maxDur {
		t.Errorf("Delay(100) = %v, want MaxInt64 ns", got)
	}
	if got := b.Delay(math.MaxInt); got != maxDur {
		t.Errorf("Delay(MaxInt) = %v, want MaxInt64 ns", got)
	}

	// The product of Base and Factor itself overflows int64 (2^62 * 2 = 2^63).
	big := newExp(t, 1<<62, 0, 2)
	if got := big.Delay(1); got != 1<<62 {
		t.Errorf("Delay(1) = %v, want Base", got)
	}
	if got := big.Delay(2); got != maxDur {
		t.Errorf("Delay(2) = %v, want saturation at MaxInt64 ns, got a wrapped value", got)
	}
}

func TestExponentialBackoff_FactorOneIsConstantAndFast(t *testing.T) {
	b := newExp(t, 70*ms, sec, 1)
	withTimeout(t, 2*sec, func() {
		if got := b.Delay(math.MaxInt); got != 70*ms {
			t.Errorf("Delay(MaxInt) = %v, want 70ms", got)
		}
	})
}

func TestExponentialBackoff_AttemptBelowOneIsTheFirstAttempt(t *testing.T) {
	b := newExp(t, 100*ms, sec, 2)
	for _, attempt := range []int{0, -1, math.MinInt} {
		if got := b.Delay(attempt); got != 100*ms {
			t.Errorf("Delay(%d) = %v, want Base", attempt, got)
		}
	}
}

func TestExponentialBackoff_ZeroValueNeverWaits(t *testing.T) {
	var b resilience.ExponentialBackoff
	for _, attempt := range []int{0, 1, 5, math.MaxInt} {
		if got := b.Delay(attempt); got != 0 {
			t.Errorf("Delay(%d) = %v, want 0", attempt, got)
		}
	}
}

func TestExponentialBackoff_ImplementsBackoff(t *testing.T) {
	var _ resilience.Backoff = resilience.ExponentialBackoff{}
	var _ resilience.Backoff = resilience.ConstantBackoff{}
}

func TestNewExponentialBackoff_InvalidArguments(t *testing.T) {
	cases := []struct {
		name   string
		base   time.Duration
		max    time.Duration
		factor int
		want   []error
	}{
		{"zero base", 0, sec, 2, []error{resilience.ErrInvalidBase}},
		{"negative base", -ms, sec, 2, []error{resilience.ErrInvalidBase}},
		{"zero factor", ms, sec, 0, []error{resilience.ErrInvalidFactor}},
		{"negative factor", ms, sec, -2, []error{resilience.ErrInvalidFactor}},
		{"negative max", ms, -sec, 2, []error{resilience.ErrInvalidMax}},
		{"max below base", ms, ms / 2, 2, []error{resilience.ErrInvalidMax}},
		{"everything wrong", 0, -sec, 0, []error{
			resilience.ErrInvalidBase, resilience.ErrInvalidFactor, resilience.ErrInvalidMax,
		}},
	}
	for _, c := range cases {
		_, err := resilience.NewExponentialBackoff(c.base, c.max, c.factor)
		checkInvalid(t, c.name, err, c.want...)
	}
}

func TestNewExponentialBackoff_BoundaryValuesAreValid(t *testing.T) {
	for name, args := range map[string][3]int64{
		"max equals base": {int64(ms), int64(ms), 2},
		"no max":          {int64(ms), 0, 2},
		"factor one":      {int64(ms), int64(sec), 1},
	} {
		if _, err := resilience.NewExponentialBackoff(time.Duration(args[0]), time.Duration(args[1]), int(args[2])); err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
		}
	}
}
