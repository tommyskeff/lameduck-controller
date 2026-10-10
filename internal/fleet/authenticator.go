package fleet

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc/peer"
	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const (
	extraPodName = "authentication.kubernetes.io/pod-name"
	extraPodUID  = "authentication.kubernetes.io/pod-uid"
)

var (
	ErrReviewsSaturated = errors.New("token reviews saturated")
	ErrSourceThrottled  = errors.New("too many rejected tokens from source")
	errTokenRejected    = errors.New("token rejected")
)

type PodIdentity struct {
	Namespace string
	Name      string
	UID       types.UID
}

type ReviewLimits struct {
	Concurrency     int
	Timeout         time.Duration
	CacheTTL        time.Duration
	FailureInterval time.Duration
	FailureBurst    int
}

type TokenAuthenticator struct {
	client   client.Client
	clock    clock.PassiveClock
	cache    *ReviewCache
	failures *SourceLimiter
	reviews  chan struct{}
	timeout  time.Duration
}

func NewTokenAuthenticator(c client.Client, passiveClock clock.PassiveClock, limits ReviewLimits) *TokenAuthenticator {
	return &TokenAuthenticator{
		client:   c,
		clock:    passiveClock,
		cache:    NewReviewCache(passiveClock, limits.CacheTTL),
		failures: NewSourceLimiter(passiveClock, limits.FailureInterval, limits.FailureBurst),
		reviews:  make(chan struct{}, limits.Concurrency),
		timeout:  limits.Timeout,
	}
}

func (a *TokenAuthenticator) Authenticate(ctx context.Context, token string) (PodIdentity, error) {
	expiry, err := unverifiedExpiry(token, lameduckv1alpha1.SidecarTokenAudience, a.clock.Now())
	if err != nil {
		return PodIdentity{}, err
	}
	if identity, ok := a.cache.Get(token); ok {
		return identity, nil
	}
	source := sourceOf(ctx)
	if !a.failures.Allow(source) {
		return PodIdentity{}, ErrSourceThrottled
	}

	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	select {
	case a.reviews <- struct{}{}:
	case <-ctx.Done():
		return PodIdentity{}, ErrReviewsSaturated
	}
	defer func() { <-a.reviews }()

	identity, err := a.review(ctx, token)
	if errors.Is(err, errTokenRejected) {
		a.failures.Fail(source)
	}
	if err != nil {
		return PodIdentity{}, err
	}
	a.cache.Put(token, identity, expiry)
	return identity, nil
}

func (a *TokenAuthenticator) review(ctx context.Context, token string) (PodIdentity, error) {
	review := &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{
			Token:     token,
			Audiences: []string{lameduckv1alpha1.SidecarTokenAudience},
		},
	}
	if err := a.client.Create(ctx, review); err != nil {
		return PodIdentity{}, fmt.Errorf("review token: %w", err)
	}
	if !review.Status.Authenticated {
		return PodIdentity{}, errTokenRejected
	}
	user := review.Status.User
	parts := strings.Split(user.Username, ":")
	if len(parts) != 4 || parts[0] != "system" || parts[1] != "serviceaccount" {
		return PodIdentity{}, fmt.Errorf("token is not a service account token: %w", errTokenRejected)
	}
	name := user.Extra[extraPodName]
	uid := user.Extra[extraPodUID]
	if len(name) != 1 || len(uid) != 1 {
		return PodIdentity{}, fmt.Errorf("token is not bound to a pod: %w", errTokenRejected)
	}
	return PodIdentity{
		Namespace: parts[2],
		Name:      name[0],
		UID:       types.UID(uid[0]),
	}, nil
}

func sourceOf(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}
	if address, ok := p.Addr.(*net.TCPAddr); ok {
		return address.IP.String()
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}
