package resilience

import (
	"sync"
	"time"
)

// Clock is the only source of time for every policy in this package.
// Policies must never call time.Now or time.Sleep directly.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
}

// RealClock is a Clock backed by the standard library.
type RealClock struct{}

func (RealClock) Now() time.Time        { return time.Now() }
func (RealClock) Sleep(d time.Duration) { time.Sleep(d) }

// FakeClock is a manually driven Clock for tests.
// Sleep does not block: it records the call and moves the clock forward.
// It is safe for concurrent use.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

// NewFakeClock returns a clock that starts at 2026-01-01 00:00:00 UTC.
func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d. A negative d panics.
func (c *FakeClock) Advance(d time.Duration) {
	if d < 0 {
		panic("resilience: FakeClock cannot go backwards")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Sleep records d (even if it is zero or negative) and, when d is positive,
// moves the clock forward by d.
func (c *FakeClock) Sleep(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, d)
	if d > 0 {
		c.now = c.now.Add(d)
	}
}

// Sleeps returns a copy of every duration passed to Sleep, in call order.
func (c *FakeClock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.sleeps))
	copy(out, c.sleeps)
	return out
}
