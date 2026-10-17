package resilience

import "slices"

// Chain composes policies: Chain(a, b, c).Do(op) behaves like a(b(c(op))),
// so the first policy is the outermost one.
//
// If any of the policies is nil, Chain returns ErrNilPolicy. Later changes to
// the passed slice do not affect the returned policy.
func Chain(policies ...Policy) (Policy, error) {
	ps := make([]Policy, len(policies))
	copy(ps, policies)
	for _, p := range ps {
		if p == nil {
			return nil, ErrNilPolicy
		}
	}
	return PolicyFunc(func(op func() error) error {
		wrapped := op
		for _, p := range slices.Backward(ps) {
			p, next := p, wrapped
			wrapped = func() error { return p.Do(next) }
		}
		return wrapped()
	}), nil
}
