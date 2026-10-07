package fleet

import (
	"context"
	"fmt"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const (
	extraPodName = "authentication.kubernetes.io/pod-name"
	extraPodUID  = "authentication.kubernetes.io/pod-uid"
)

type PodIdentity struct {
	Namespace string
	Name      string
	UID       types.UID
}

type TokenAuthenticator struct {
	Client client.Client
}

func (a TokenAuthenticator) Authenticate(ctx context.Context, token string) (PodIdentity, error) {
	review := &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{
			Token:     token,
			Audiences: []string{lameduckv1alpha1.SidecarTokenAudience},
		},
	}
	if err := a.Client.Create(ctx, review); err != nil {
		return PodIdentity{}, fmt.Errorf("review token: %w", err)
	}
	if !review.Status.Authenticated {
		return PodIdentity{}, fmt.Errorf("token rejected")
	}
	user := review.Status.User
	parts := strings.Split(user.Username, ":")
	if len(parts) != 4 || parts[0] != "system" || parts[1] != "serviceaccount" {
		return PodIdentity{}, fmt.Errorf("token is not a service account token")
	}
	name := user.Extra[extraPodName]
	uid := user.Extra[extraPodUID]
	if len(name) != 1 || len(uid) != 1 {
		return PodIdentity{}, fmt.Errorf("token is not bound to a pod")
	}
	return PodIdentity{
		Namespace: parts[2],
		Name:      name[0],
		UID:       types.UID(uid[0]),
	}, nil
}
