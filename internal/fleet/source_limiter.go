package fleet

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
	"k8s.io/utils/clock"
)

const minSourceSweep = 1024

type SourceLimiter struct {
	clock     clock.PassiveClock
	limit     rate.Limit
	burst     int
	sweepSize int
	mu        sync.Mutex
	sources   map[string]*rate.Limiter
}

func NewSourceLimiter(passiveClock clock.PassiveClock, interval time.Duration, burst int) *SourceLimiter {
	return &SourceLimiter{
		clock:     passiveClock,
		limit:     rate.Every(interval),
		burst:     burst,
		sweepSize: minSourceSweep,
		sources:   map[string]*rate.Limiter{},
	}
}

func (l *SourceLimiter) Allow(source string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	limiter, ok := l.sources[source]
	return !ok || limiter.TokensAt(l.clock.Now()) >= 1
}

func (l *SourceLimiter) Fail(source string) {
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	limiter, ok := l.sources[source]
	if !ok {
		limiter = rate.NewLimiter(l.limit, l.burst)
		l.sources[source] = limiter
	}
	limiter.AllowN(now, 1)
	if len(l.sources) < l.sweepSize {
		return
	}
	for key, limiter := range l.sources {
		if limiter.TokensAt(now) >= float64(l.burst) {
			delete(l.sources, key)
		}
	}
	l.sweepSize = max(minSourceSweep, 2*len(l.sources))
}
