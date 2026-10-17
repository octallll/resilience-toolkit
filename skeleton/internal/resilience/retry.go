package resilience

// RetryError is returned by Retry when every attempt has failed.
type RetryError struct {
	Attempts int   // how many times op was called
	Last     error // the error of the last attempt
}

// Error returns "retries exhausted after <Attempts> attempts: <Last>".
func (e *RetryError) Error() string {
	panic("not implemented")
}

// Unwrap must make both errors.Is(err, ErrRetriesExhausted) and errors.Is(err, e.Last) work.
func (e *RetryError) Unwrap() []error {
	panic("not implemented")
}

// RetryConfig configures Retry.
type RetryConfig struct {
	MaxAttempts int              // total number of calls of op, including the first one; must be >= 1
	Backoff     Backoff          // nil means "no pause"
	Retryable   func(error) bool // nil means "retry any error"
}

// Retry repeats a failing operation.
type Retry struct {
	// TODO: add the fields you need.
}

// NewRetry returns an error matching ErrNilClock and/or ErrInvalidMaxAttempts for invalid arguments.
func NewRetry(clock Clock, cfg RetryConfig) (*Retry, error) {
	panic("not implemented")
}

// Do calls op up to MaxAttempts times.
func (r *Retry) Do(op func() error) error {
	panic("not implemented")
}
