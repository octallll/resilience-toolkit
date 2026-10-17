package resilience

import "time"

// State is the state of a CircuitBreaker.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

// String returns "closed", "open", "half-open" or, for any other value, "State(<n>)".
func (s State) String() string {
	panic("not implemented")
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
	// TODO: add the fields you need.
}

// NewCircuitBreaker returns a closed breaker. For invalid arguments it returns an error
// matching ErrNilClock, ErrInvalidFailureThreshold, ErrInvalidOpenTimeout and/or
// ErrInvalidHalfOpenSuccesses.
func NewCircuitBreaker(clock Clock, cfg Config) (*CircuitBreaker, error) {
	panic("not implemented")
}

// State returns the effective state at the current time: an Open breaker whose
// timeout has elapsed is already HalfOpen.
func (c *CircuitBreaker) State() State {
	panic("not implemented")
}

// Do runs op unless the circuit is open. op's error is returned as is.
// A panic inside op counts as a failure and is propagated to the caller.
func (c *CircuitBreaker) Do(op func() error) error {
	panic("not implemented")
}
