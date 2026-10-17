//go:build bonus

package resilience_test

import (
	"math/rand/v2"
	"testing"

	"resilience/internal/resilience"
)

// This test is a small simulation in fake time. It does not check a signature
// or a corner case, it checks the *effect* a policy is supposed to have on a service.

// A thousand clients fail at the same moment and retry with the same exponential
// backoff. Without jitter they come back all together, again and again.
func TestSimulation_ThunderingHerd(t *testing.T) {
	const (
		clients = 1000
		bucket  = 10 * ms
	)

	// peak returns the largest number of *retries* that hit the service within one bucket of time.
	peak := func(backoffFor func() resilience.Backoff) int {
		hist := map[int]int{}
		for range clients {
			clock := resilience.NewFakeClock()
			start := clock.Now()
			r := newRetry(t, clock, 5, backoffFor())
			calls := 0
			_ = r.Do(func() error {
				calls++
				if calls > 1 { // the first attempt is simultaneous by construction
					hist[int(clock.Now().Sub(start)/bucket)]++
				}
				return errBoom
			})
		}
		highest := 0
		for _, n := range hist {
			highest = max(highest, n)
		}
		return highest
	}

	exp := func() resilience.Backoff { return newExp(t, 100*ms, 5*sec, 2) }

	if got := peak(exp); got != clients {
		t.Fatalf("without jitter the peak is %d retries per %v, want all %d clients at once", got, bucket, clients)
	}

	rng := rand.New(rand.NewPCG(2026, 1))
	full := peak(func() resilience.Backoff { return newFullJitter(t, exp(), rng) })
	equal := peak(func() resilience.Backoff { return newEqualJitter(t, exp(), rng) })

	t.Logf("peak retries per %v: none=%d, full jitter=%d, equal jitter=%d (of %d clients)", bucket, clients, full, equal, clients)
	if full > clients/4 {
		t.Errorf("full jitter: peak = %d per %v, want at most %d", full, bucket, clients/4)
	}
	if equal > clients/3 {
		t.Errorf("equal jitter: peak = %d per %v, want at most %d", equal, bucket, clients/3)
	}
}
