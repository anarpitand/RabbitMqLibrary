package backoff

import (
	"math/rand"
	"time"
)

// Duration returns a wait duration for the given attempt (0-based) using
// exponential growth capped at max, with jitter up to 25% of the base interval.
func Duration(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = 30 * time.Second
	}
	if attempt < 0 {
		attempt = 0
	}

	d := base * time.Duration(1<<attempt)
	if d > max {
		d = max
	}

	jitter := time.Duration(rand.Int63n(int64(d/4) + 1))
	return d + jitter
}
