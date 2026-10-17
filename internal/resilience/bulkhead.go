package resilience

import "sync"

// Bulkhead limits how many operations may run at the same time.
// Unlike TokenBucket it does not care about time: a slot is freed
// the moment its operation finishes.
type Bulkhead struct {
	limit int

	mu       sync.Mutex
	inFlight int
}

// NewBulkhead returns ErrInvalidLimit if limit <= 0.
func NewBulkhead(limit int) (*Bulkhead, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	return &Bulkhead{limit: limit}, nil
}

// InFlight returns the number of operations that are running right now.
func (b *Bulkhead) InFlight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlight
}

func (b *Bulkhead) acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inFlight >= b.limit {
		return false
	}
	b.inFlight++
	return true
}

func (b *Bulkhead) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.inFlight--
}

// Do runs op if a slot is free and returns op's error as is. Otherwise it
// returns ErrBulkheadFull without calling op. The slot is released when op
// returns, whether it returned an error or panicked.
func (b *Bulkhead) Do(op func() error) error {
	if !b.acquire() {
		return ErrBulkheadFull
	}
	defer b.release()
	return op()
}
