package resilience

import "time"

// Rand is the source of randomness for jitter. Float64 returns a number in [0, 1).
// *rand.Rand from math/rand/v2 satisfies it. Implementations are not required to be
// safe for concurrent use, so do not share one Rand between goroutines unless it is.
type Rand interface {
	Float64() float64
}

// FullJitter replaces the delay d of another Backoff with a random one, uniformly from [0, d).
//
// If all clients use the same exponential backoff, they all come back at exactly
// the same moments (a "thundering herd"). Randomness spreads them over time.
type FullJitter struct {
	// TODO: add the fields you need.
}

// NewFullJitter returns ErrNilBackoff and/or ErrNilRand for nil arguments.
func NewFullJitter(inner Backoff, rnd Rand) (*FullJitter, error) {
	panic("not implemented")
}

// Delay asks the inner Backoff for the delay of the same attempt. If that delay is
// not positive, it is returned as 0 and rnd is not touched; otherwise rnd.Float64
// is called exactly once. The result is always in [0, d), even if rnd misbehaves
// and returns a number outside [0, 1).
func (j *FullJitter) Delay(attempt int) time.Duration {
	panic("not implemented")
}

// EqualJitter keeps half of the delay d of another Backoff and randomizes the other half:
// the result is uniform in [d/2, d). It never waits less than half the planned time.
type EqualJitter struct {
	// TODO: add the fields you need.
}

// NewEqualJitter returns ErrNilBackoff and/or ErrNilRand for nil arguments.
func NewEqualJitter(inner Backoff, rnd Rand) (*EqualJitter, error) {
	panic("not implemented")
}

// Delay behaves like FullJitter.Delay, but returns d/2 plus a random part of the other half,
// so the result is in [d/2, d) and, for an odd d, the whole range is reachable.
func (j *EqualJitter) Delay(attempt int) time.Duration {
	panic("not implemented")
}
