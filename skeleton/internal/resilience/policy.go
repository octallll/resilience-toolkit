package resilience

// Policy wraps the execution of an operation with some resilience behaviour.
type Policy interface {
	Do(op func() error) error
}

// PolicyFunc adapts an ordinary function to the Policy interface.
type PolicyFunc func(op func() error) error

func (f PolicyFunc) Do(op func() error) error { return f(op) }
