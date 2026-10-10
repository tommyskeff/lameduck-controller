package fleet

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/peer"
	authenticationv1 "k8s.io/api/authentication/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var limits = ReviewLimits{
	Concurrency:     1,
	Timeout:         time.Second,
	CacheTTL:        10 * time.Minute,
	FailureInterval: 10 * time.Second,
	FailureBurst:    2,
}

type reviewer struct {
	calls   atomic.Int32
	release chan struct{}
}

func (r *reviewer) client() client.Client {
	return fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
			r.calls.Add(1)
			if r.release != nil {
				<-r.release
			}
			review := obj.(*authenticationv1.TokenReview)
			if review.Spec.Token != validToken {
				return nil
			}
			review.Status.Authenticated = true
			review.Status.User = authenticationv1.UserInfo{
				Username: "system:serviceaccount:games:default",
				Extra: map[string]authenticationv1.ExtraValue{
					extraPodName: {"bedwars-abc"},
					extraPodUID:  {"uid-1"},
				},
			}
			return nil
		},
	}).Build()
}

var (
	validToken    = jwt(`{"exp":4102444800,"aud":"lameduck.tommyjs.dev"}`)
	forgedToken   = jwt(`{"exp":4102444800,"aud":"lameduck.tommyjs.dev","forged":true}`)
	audienceToken = jwt(`{"exp":4102444800,"aud":"kubernetes"}`)
)

func from(address string) context.Context {
	return peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP(address), Port: 40000},
	})
}

func TestAuthenticatorCachesSuccessfulReviews(t *testing.T) {
	r := &reviewer{}
	authenticator := NewTokenAuthenticator(r.client(), clocktesting.NewFakeClock(time.Now()), limits)

	for range 3 {
		got, err := authenticator.Authenticate(from("10.0.0.1"), validToken)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if got != identity {
			t.Fatalf("identity = %v; want %v", got, identity)
		}
	}
	if calls := r.calls.Load(); calls != 1 {
		t.Fatalf("token reviews = %d; want 1", calls)
	}
}

func TestAuthenticatorSkipsReviewForWrongAudience(t *testing.T) {
	r := &reviewer{}
	authenticator := NewTokenAuthenticator(r.client(), clocktesting.NewFakeClock(time.Now()), limits)

	if _, err := authenticator.Authenticate(from("10.0.0.1"), audienceToken); err == nil {
		t.Fatal("Authenticate accepted a token for another audience")
	}
	if _, err := authenticator.Authenticate(from("10.0.0.1"), "not-a-jwt"); err == nil {
		t.Fatal("Authenticate accepted a malformed token")
	}
	if calls := r.calls.Load(); calls != 0 {
		t.Fatalf("token reviews = %d; want 0", calls)
	}
}

func TestAuthenticatorThrottlesRejectedSources(t *testing.T) {
	r := &reviewer{}
	clock := clocktesting.NewFakeClock(time.Now())
	authenticator := NewTokenAuthenticator(r.client(), clock, limits)

	for range limits.FailureBurst {
		if _, err := authenticator.Authenticate(from("10.0.0.1"), forgedToken); err == nil {
			t.Fatal("Authenticate accepted a forged token")
		}
	}
	if _, err := authenticator.Authenticate(from("10.0.0.1"), forgedToken); !errors.Is(err, ErrSourceThrottled) {
		t.Fatalf("err = %v; want ErrSourceThrottled", err)
	}
	if _, err := authenticator.Authenticate(from("10.0.0.1"), validToken); !errors.Is(err, ErrSourceThrottled) {
		t.Fatalf("err = %v; want ErrSourceThrottled for an uncached token", err)
	}
	if _, err := authenticator.Authenticate(from("10.0.0.2"), validToken); err != nil {
		t.Fatalf("Authenticate from another source: %v", err)
	}
	if calls := r.calls.Load(); calls != int32(limits.FailureBurst)+1 {
		t.Fatalf("token reviews = %d; want %d", calls, limits.FailureBurst+1)
	}
	clock.Step(limits.FailureInterval)
	if _, err := authenticator.Authenticate(from("10.0.0.1"), validToken); err != nil {
		t.Fatalf("Authenticate after the interval: %v", err)
	}
}

func TestAuthenticatorBoundsConcurrentReviews(t *testing.T) {
	r := &reviewer{release: make(chan struct{})}
	authenticator := NewTokenAuthenticator(r.client(), clocktesting.NewFakeClock(time.Now()),
		ReviewLimits{Concurrency: 1, Timeout: 100 * time.Millisecond, CacheTTL: time.Minute, FailureInterval: time.Second, FailureBurst: 1})

	done := make(chan error, 1)
	go func() {
		_, err := authenticator.Authenticate(from("10.0.0.1"), forgedToken)
		done <- err
	}()
	for r.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if _, err := authenticator.Authenticate(from("10.0.0.2"), validToken); !errors.Is(err, ErrReviewsSaturated) {
		t.Fatalf("err = %v; want ErrReviewsSaturated", err)
	}
	close(r.release)
	<-done
}
