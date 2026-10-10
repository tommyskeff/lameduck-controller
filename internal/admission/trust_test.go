package admission

import "testing"

func TestTrustedUsers(t *testing.T) {
	const controller = "system:serviceaccount:lameduck-system:lameduck"
	cases := []struct {
		username string
		eviction bool
		want     bool
	}{
		{controller, true, true},
		{controller, false, true},
		{"system:serviceaccount:kube-system:cluster-autoscaler", true, false},
		{"system:serviceaccount:kube-system:cluster-autoscaler", false, true},
		{nodeUsername, true, false},
		{nodeUsername, false, true},
		{"kubernetes-admin", true, false},
		{"kubernetes-admin", false, false},
	}
	for _, c := range cases {
		handler := &DisruptionHandler{ControllerUsername: controller, Eviction: c.eviction}
		if got := handler.trusted(c.username); got != c.want {
			t.Errorf("trusted(%q) with eviction=%v = %v, want %v", c.username, c.eviction, got, c.want)
		}
	}
}
