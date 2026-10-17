package resilience

import (
	"errors"
	"sync"
	"time"
)

// Rand is the source of randomness for jitter. Float64 returns a number in [0, 1).
// *rand.Rand from math/rand/v2 satisfies it. Implementations are not required to be
// safe for concurrent use: FullJitter and EqualJitter serialize their calls to it.
type Rand interface {
	Float64() float64
}

// lockedRand serializes access to a Rand that may not be goroutine-safe.
type lockedRand struct {
	mu  sync.Mutex
	rnd Rand
}

func (l *lockedRand) Float64() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rnd.Float64()
}

func validateJitter(inner Backoff, rnd Rand) error {
	return errors.Join(
		failIf(inner == nil, ErrNilBackoff),
		failIf(rnd == nil, ErrNilRand),
	)
}

// scale returns the part f (in [0, 1)) of d. The result is always in [0, d).
func scale(d time.Duration, f float64) time.Duration {
	r := time.Duration(f * float64(d))
	if r < 0 {
		return 0
	}
	if r >= d { // float64 rounding at the very top of the range
		return d - 1
	}
	return r
}

// FullJitter replaces the delay d of another Backoff with a random one, uniformly from [0, d).
//
// If all clients use the same exponential backoff, they all come back at exactly
// the same moments (a "thundering herd"). Randomness spreads them over time.
type FullJitter struct {
	inner Backoff
	rnd   lockedRand
}

// NewFullJitter returns ErrNilBackoff and/or ErrNilRand for nil arguments.
func NewFullJitter(inner Backoff, rnd Rand) (*FullJitter, error) {
	if err := validateJitter(inner, rnd); err != nil {
		return nil, err
	}
	return &FullJitter{inner: inner, rnd: lockedRand{rnd: rnd}}, nil
}

// Delay asks the inner Backoff for the delay of the same attempt. If that delay is
// not positive, it is returned as 0 and rnd is not touched; otherwise rnd.Float64
// is called exactly once.
func (j *FullJitter) Delay(attempt int) time.Duration {
	d := j.inner.Delay(attempt)
	if d <= 0 {
		return 0
	}
	return scale(d, j.rnd.Float64())
}

// EqualJitter keeps half of the delay d of another Backoff and randomizes the other half:
// the result is uniform in [d/2, d). It never waits less than half the planned time.
type EqualJitter struct {
	inner Backoff
	rnd   lockedRand
}

// NewEqualJitter returns ErrNilBackoff and/or ErrNilRand for nil arguments.
func NewEqualJitter(inner Backoff, rnd Rand) (*EqualJitter, error) {
	if err := validateJitter(inner, rnd); err != nil {
		return nil, err
	}
	return &EqualJitter{inner: inner, rnd: lockedRand{rnd: rnd}}, nil
}

// Delay behaves like FullJitter.Delay, but returns d/2 + a random part of the other half.
func (j *EqualJitter) Delay(attempt int) time.Duration {
	d := j.inner.Delay(attempt)
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + scale(d-half, j.rnd.Float64())
}
