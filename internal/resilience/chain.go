package resilience

// Chain composes policies: Chain(a, b, c).Do(op) behaves like a(b(c(op))),
// so the first policy is the outermost one.
//
// If any of the policies is nil, Chain returns ErrNilPolicy. Later changes to
// the passed slice do not affect the returned policy.
//
// Only a nil interface value counts as nil: a typed nil pointer such as
// (*TokenBucket)(nil) is not detected and panics on the first Do.
func Chain(policies ...Policy) (Policy, error) {
	ps := make([]Policy, len(policies))
	copy(ps, policies)
	for _, p := range ps {
		if p == nil {
			return nil, ErrNilPolicy
		}
	}
	return chain(ps), nil
}

type chain []Policy

func (c chain) Do(op func() error) error { return c.run(0, op) }

// run executes policies c[i:] around op. The wrapper for the next policy is created
// only when the current one actually calls it, so a policy that rejects the call
// (a drained TokenBucket, an open CircuitBreaker) costs a single allocation
// no matter how long the chain is.
func (c chain) run(i int, op func() error) error {
	if i == len(c) {
		return op()
	}
	return c[i].Do(func() error { return c.run(i+1, op) })
}
