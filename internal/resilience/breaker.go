package resilience

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is the state of a CircuitBreaker.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// Config configures a CircuitBreaker. All numeric fields must be positive.
type Config struct {
	FailureThreshold  int           // consecutive failures that open the circuit
	OpenTimeout       time.Duration // how long the circuit stays open before a probe is allowed
	HalfOpenSuccesses int           // consecutive successful probes that close the circuit
	IsFailure         func(error) bool
}

// CircuitBreaker stops calling a failing operation for a while.
type CircuitBreaker struct {
	clock Clock
	cfg   Config

	mu        sync.Mutex
	state     State
	failures  int // consecutive failures while Closed
	successes int // consecutive successes while HalfOpen
	openedAt  time.Time
	probing   bool   // a probe is in flight (HalfOpen)
	gen       uint64 // bumped on every state change; stale results are dropped
}

// NewCircuitBreaker returns a closed breaker. For invalid arguments it returns an error
// matching ErrNilClock, ErrInvalidFailureThreshold, ErrInvalidOpenTimeout and/or
// ErrInvalidHalfOpenSuccesses.
func NewCircuitBreaker(clock Clock, cfg Config) (*CircuitBreaker, error) {
	if err := errors.Join(
		failIf(clock == nil, ErrNilClock),
		failIf(cfg.FailureThreshold <= 0, ErrInvalidFailureThreshold),
		failIf(cfg.OpenTimeout <= 0, ErrInvalidOpenTimeout),
		failIf(cfg.HalfOpenSuccesses <= 0, ErrInvalidHalfOpenSuccesses),
	); err != nil {
		return nil, err
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = func(err error) bool { return err != nil }
	}
	return &CircuitBreaker{clock: clock, cfg: cfg}, nil
}

// State returns the effective state at the current time: an Open breaker whose
// timeout has elapsed is already HalfOpen.
func (c *CircuitBreaker) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	return c.state
}

// advance performs the time-driven Open -> HalfOpen transition. The caller must hold c.mu.
func (c *CircuitBreaker) advance() {
	if c.state == Open && c.clock.Now().Sub(c.openedAt) >= c.cfg.OpenTimeout {
		c.transition(HalfOpen)
	}
}

// transition switches state and resets all counters. The caller must hold c.mu.
func (c *CircuitBreaker) transition(s State) {
	c.state = s
	c.failures = 0
	c.successes = 0
	c.probing = false
	c.gen++
	if s == Open {
		c.openedAt = c.clock.Now()
	}
}

// before decides whether the call may go through.
func (c *CircuitBreaker) before() (gen uint64, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.advance()
	switch c.state {
	case Open:
		return 0, false
	case HalfOpen:
		if c.probing {
			return 0, false
		}
		c.probing = true
	}
	return c.gen, true
}

// after records the outcome of a call that started in generation gen.
func (c *CircuitBreaker) after(gen uint64, failed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen != gen {
		return // the breaker changed state while op was running
	}
	switch c.state {
	case Closed:
		if !failed {
			c.failures = 0
			return
		}
		c.failures++
		if c.failures >= c.cfg.FailureThreshold {
			c.transition(Open)
		}
	case HalfOpen:
		c.probing = false
		if failed {
			c.transition(Open)
			return
		}
		c.successes++
		if c.successes >= c.cfg.HalfOpenSuccesses {
			c.transition(Closed)
		}
	}
}

// Do runs op unless the circuit is open. op's error is returned as is.
// A panic inside op counts as a failure and is propagated to the caller.
func (c *CircuitBreaker) Do(op func() error) error {
	gen, ok := c.before()
	if !ok {
		return ErrCircuitOpen
	}
	finished := false
	defer func() {
		if !finished {
			c.after(gen, true)
		}
	}()
	err := op()
	finished = true
	c.after(gen, c.cfg.IsFailure(err))
	return err
}
