package backoff

import (
	"math/rand/v2"
	"testing"
	"time"
)

func highest(n int64) int64 {
	return n - 1
}

func lowest(int64) int64 {
	return 0
}

func TestNextDoublesCeilingUpToMaximum(t *testing.T) {
	exponential := NewExponential(500*time.Millisecond, 10*time.Second, highest)
	want := []time.Duration{
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		10 * time.Second,
		10 * time.Second,
	}
	for i, ceiling := range want {
		if delay := exponential.Next(); delay != ceiling {
			t.Fatalf("delay %d = %v, want %v", i, delay, ceiling)
		}
	}
}

func TestNextCanReturnZero(t *testing.T) {
	exponential := NewExponential(time.Second, 30*time.Second, lowest)
	for range 10 {
		if delay := exponential.Next(); delay != 0 {
			t.Fatalf("delay = %v, want 0", delay)
		}
	}
}

func TestNextStaysWithinCeiling(t *testing.T) {
	exponential := NewExponential(time.Second, 30*time.Second, rand.Int64N)
	ceiling := time.Second
	for range 1000 {
		delay := exponential.Next()
		if delay < 0 || delay > ceiling {
			t.Fatalf("delay = %v, want within [0, %v]", delay, ceiling)
		}
		ceiling = min(ceiling*2, 30*time.Second)
	}
}

func TestResetRestoresMinimum(t *testing.T) {
	exponential := NewExponential(time.Second, 30*time.Second, highest)
	for range 5 {
		exponential.Next()
	}
	exponential.Reset()
	if delay := exponential.Next(); delay != time.Second {
		t.Fatalf("delay after reset = %v, want 1s", delay)
	}
}
