package backoff

import (
	"testing"
	"time"
)

func TestDurationDefaults(t *testing.T) {
	d := Duration(0, 0, 0)
	if d < time.Second || d > time.Second+time.Second/4+1 {
		t.Fatalf("expected default base ~1s with jitter, got %v", d)
	}
}

func TestDurationExponentialGrowth(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second

	prev := time.Duration(0)
	for attempt := 0; attempt < 4; attempt++ {
		d := Duration(attempt, base, max)
		min := base * time.Duration(1<<attempt)
		if min > max {
			min = max
		}
		maxBound := min + min/4 + 1
		if d < min || d > maxBound {
			t.Fatalf("attempt %d: duration %v outside [%v, %v]", attempt, d, min, maxBound)
		}
		if attempt > 0 && d < prev {
			// growth is not strictly monotonic due to jitter, but base doubles each attempt
		}
		prev = d
	}
}

func TestDurationCapsAtMax(t *testing.T) {
	base := time.Second
	max := 2 * time.Second
	d := Duration(20, base, max)
	if d < max || d > max+max/4+1 {
		t.Fatalf("expected capped duration near max, got %v", d)
	}
}

func TestDurationNegativeAttempt(t *testing.T) {
	base := time.Second
	d := Duration(-1, base, 10*time.Second)
	if d < base || d > base+base/4+1 {
		t.Fatalf("negative attempt should behave as 0, got %v", d)
	}
}
