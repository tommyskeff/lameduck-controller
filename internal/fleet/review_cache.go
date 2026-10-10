package fleet

import (
	"crypto/sha256"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

type reviewCacheEntry struct {
	identity PodIdentity
	expires  time.Time
}

type ReviewCache struct {
	clock     clock.PassiveClock
	ttl       time.Duration
	mu        sync.Mutex
	entries   map[[sha256.Size]byte]reviewCacheEntry
	lastSweep time.Time
}

func NewReviewCache(passiveClock clock.PassiveClock, ttl time.Duration) *ReviewCache {
	return &ReviewCache{
		clock:     passiveClock,
		ttl:       ttl,
		entries:   map[[sha256.Size]byte]reviewCacheEntry{},
		lastSweep: passiveClock.Now(),
	}
}

func (c *ReviewCache) Get(token string) (PodIdentity, bool) {
	key := sha256.Sum256([]byte(token))
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return PodIdentity{}, false
	}
	if !c.clock.Now().Before(entry.expires) {
		delete(c.entries, key)
		return PodIdentity{}, false
	}
	return entry.identity, true
}

func (c *ReviewCache) Put(token string, identity PodIdentity, tokenExpiry time.Time) {
	now := c.clock.Now()
	expires := now.Add(c.ttl)
	if tokenExpiry.Before(expires) {
		expires = tokenExpiry
	}
	if !now.Before(expires) {
		return
	}
	key := sha256.Sum256([]byte(token))
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = reviewCacheEntry{identity: identity, expires: expires}
	if now.Sub(c.lastSweep) >= c.ttl {
		c.lastSweep = now
		for key, entry := range c.entries {
			if !now.Before(entry.expires) {
				delete(c.entries, key)
			}
		}
	}
}
