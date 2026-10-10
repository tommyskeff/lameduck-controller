package fleet

import (
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

var identity = PodIdentity{Namespace: testNamespace, Name: "bedwars-abc", UID: "uid-1"}

func TestReviewCacheReturnsIdentityUntilTTL(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	cache := NewReviewCache(clock, time.Minute)
	cache.Put("token", identity, clock.Now().Add(time.Hour))

	if got, ok := cache.Get("token"); !ok || got != identity {
		t.Fatalf("Get = %v, %v; want cached identity", got, ok)
	}
	if _, ok := cache.Get("other"); ok {
		t.Fatal("Get returned an identity for an unknown token")
	}
	clock.Step(time.Minute)
	if _, ok := cache.Get("token"); ok {
		t.Fatal("Get returned an identity after the TTL")
	}
}

func TestReviewCacheExpiresWithToken(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	cache := NewReviewCache(clock, time.Hour)
	cache.Put("token", identity, clock.Now().Add(time.Minute))

	clock.Step(59 * time.Second)
	if _, ok := cache.Get("token"); !ok {
		t.Fatal("Get missed before the token expired")
	}
	clock.Step(time.Second)
	if _, ok := cache.Get("token"); ok {
		t.Fatal("Get returned an identity for an expired token")
	}
}

func TestReviewCacheSkipsExpiredTokens(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	cache := NewReviewCache(clock, time.Hour)
	cache.Put("token", identity, clock.Now())

	if len(cache.entries) != 0 {
		t.Fatalf("cache holds %d entries; want 0", len(cache.entries))
	}
}

func TestReviewCacheSweepsExpiredEntries(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	cache := NewReviewCache(clock, time.Minute)
	cache.Put("first", identity, clock.Now().Add(time.Hour))
	cache.Put("second", identity, clock.Now().Add(time.Hour))

	clock.Step(time.Minute)
	cache.Put("third", identity, clock.Now().Add(time.Hour))
	if len(cache.entries) != 1 {
		t.Fatalf("cache holds %d entries after a sweep; want 1", len(cache.entries))
	}
}
