package resilience

// Chain composes policies: Chain(a, b, c).Do(op) behaves like a(b(c(op))),
// so the first policy is the outermost one.
//
// If any of the policies is nil, Chain returns ErrNilPolicy. Later changes to
// the passed slice do not affect the returned policy.
func Chain(policies ...Policy) (Policy, error) {
	panic("not implemented")
}
