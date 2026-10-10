package fleet

import (
	"fmt"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

func TestSourceLimiterBlocksAfterBurst(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	limiter := NewSourceLimiter(clock, 10*time.Second, 3)

	for range 3 {
		if !limiter.Allow("10.0.0.1") {
			t.Fatal("Allow = false before the burst was spent")
		}
		limiter.Fail("10.0.0.1")
	}
	if limiter.Allow("10.0.0.1") {
		t.Fatal("Allow = true after the burst was spent")
	}
	if !limiter.Allow("10.0.0.2") {
		t.Fatal("Allow = false for a different source")
	}
	clock.Step(10 * time.Second)
	if !limiter.Allow("10.0.0.1") {
		t.Fatal("Allow = false after the interval refilled one failure")
	}
}

func TestSourceLimiterSweepsRecoveredSources(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	limiter := NewSourceLimiter(clock, time.Second, 2)

	for i := range minSourceSweep - 1 {
		limiter.Fail(fmt.Sprintf("10.0.%d.%d", i/256, i%256))
	}
	clock.Step(2 * time.Second)
	limiter.Fail("10.1.0.1")
	if len(limiter.sources) != 1 {
		t.Fatalf("limiter tracks %d sources after a sweep; want 1", len(limiter.sources))
	}
}
