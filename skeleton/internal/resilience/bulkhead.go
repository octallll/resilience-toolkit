package resilience

// Bulkhead limits how many operations may run at the same time.
type Bulkhead struct {
	// TODO: add the fields you need.
}

// NewBulkhead returns ErrInvalidLimit if limit <= 0.
func NewBulkhead(limit int) (*Bulkhead, error) {
	panic("not implemented")
}

// InFlight returns the number of operations that are running right now.
func (b *Bulkhead) InFlight() int {
	panic("not implemented")
}

// Do runs op if a slot is free and returns op's error as is. Otherwise it
// returns ErrBulkheadFull without calling op. The slot is released when op
// returns, whether it returned an error or panicked.
func (b *Bulkhead) Do(op func() error) error {
	panic("not implemented")
}
