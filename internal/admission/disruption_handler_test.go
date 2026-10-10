package admission

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/metrics"
)

const (
	testNamespace      = "games"
	testSet            = "bedwars"
	podName            = "bedwars-abc"
	controllerUsername = "system:serviceaccount:lameduck-system:lameduck"
	userUsername       = "alice"
	nodeUsername       = "system:node:worker-1"
	setUID             = "set-uid"
)

func TestEvictionBecomesDrain(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := lameduckv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	set := &lameduckv1alpha1.DrainSet{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSet, UID: setUID}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: testNamespace,
		Name:      "bedwars-abc",
		Labels:    map[string]string{lameduckv1alpha1.LabelSet: testSet},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: lameduckv1alpha1.GroupVersion.String(),
			Kind:       "DrainSet",
			Name:       testSet,
			UID:        setUID,
			Controller: new(true),
		}},
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(set, pod).Build()
	recorder := events.NewFakeRecorder(1)
	drainMetrics := metrics.NewDrainMetrics()
	registry := prometheus.NewRegistry()
	if err := drainMetrics.Register(registry); err != nil {
		t.Fatal(err)
	}
	handler := &DisruptionHandler{Client: kube, ControllerUsername: "system:serviceaccount:lameduck:controller", Eviction: true, Recorder: recorder, Metrics: drainMetrics}

	response := handler.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Namespace: testNamespace,
		Name:      "bedwars-abc",
		UserInfo:  authenticationv1.UserInfo{Username: "system:serviceaccount:ops:drainer"},
	}})
	if response.Allowed || response.Result.Code != 429 {
		t.Fatalf("response = %+v", response.Result)
	}

	var patched corev1.Pod
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(pod), &patched); err != nil {
		t.Fatal(err)
	}
	if patched.Annotations[lameduckv1alpha1.AnnotationDrain] != string(lameduckv1alpha1.DrainReasonEviction) {
		t.Fatalf("annotations = %v", patched.Annotations)
	}
	if event := <-recorder.Events; !strings.HasPrefix(event, "Normal DrainRequested Converted eviction into a drain with reason Eviction") {
		t.Fatalf("event = %q", event)
	}
	expected := `
# HELP lameduck_evictions_intercepted_total Pod evictions turned into drains, by DrainSet.
# TYPE lameduck_evictions_intercepted_total counter
lameduck_evictions_intercepted_total{drainset="bedwars",namespace="games"} 1
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected), "lameduck_evictions_intercepted_total"); err != nil {
		t.Fatal(err)
	}
}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(lameduckv1alpha1.AddToScheme(scheme))
	return scheme
}

func drainSet(protection lameduckv1alpha1.Protection) *lameduckv1alpha1.DrainSet {
	return &lameduckv1alpha1.DrainSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSet, UID: setUID},
		Spec:       lameduckv1alpha1.DrainSetSpec{Protection: protection},
	}
}

func ownedPod(t *testing.T, scheme *runtime.Scheme, set *lameduckv1alpha1.DrainSet) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      podName,
			Labels:    map[string]string{lameduckv1alpha1.LabelSet: set.Name},
		},
	}
	if err := controllerutil.SetControllerReference(set, pod, scheme); err != nil {
		t.Fatalf("own pod: %v", err)
	}
	return pod
}

type fixture struct {
	client   client.Client
	eviction *DisruptionHandler
	deletion *DisruptionHandler
}

func newFixture(t *testing.T, set *lameduckv1alpha1.DrainSet, pod *corev1.Pod) fixture {
	t.Helper()
	builder := fake.NewClientBuilder().WithScheme(testScheme())
	if set != nil {
		builder = builder.WithObjects(set)
	}
	if pod != nil {
		builder = builder.WithObjects(pod)
	}
	fakeClient := builder.Build()
	recorder := events.NewFakeRecorder(10)
	drainMetrics := metrics.NewDrainMetrics()
	return fixture{
		client:   fakeClient,
		eviction: &DisruptionHandler{Client: fakeClient, ControllerUsername: controllerUsername, Eviction: true, Recorder: recorder, Metrics: drainMetrics},
		deletion: &DisruptionHandler{Client: fakeClient, ControllerUsername: controllerUsername, Recorder: recorder, Metrics: drainMetrics},
	}
}

func evictionRequest(username string) admission.Request {
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation:   admissionv1.Create,
		Namespace:   testNamespace,
		Name:        podName,
		SubResource: "eviction",
		UserInfo:    authenticationv1.UserInfo{Username: username},
	}}
}

func deleteRequest(t *testing.T, username string, pod *corev1.Pod) admission.Request {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("encode pod: %v", err)
	}
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Delete,
		Namespace: testNamespace,
		Name:      podName,
		UserInfo:  authenticationv1.UserInfo{Username: username},
		OldObject: runtime.RawExtension{Raw: raw},
	}}
}

func (f fixture) drainAnnotation(t *testing.T) (string, bool) {
	t.Helper()
	var pod corev1.Pod
	if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: podName}, &pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	value, ok := pod.Annotations[lameduckv1alpha1.AnnotationDrain]
	return value, ok
}

func expectAllowed(t *testing.T, response admission.Response) {
	t.Helper()
	if !response.Allowed {
		t.Fatalf("denied: %+v", response.Result)
	}
}

func expectDenied(t *testing.T, response admission.Response, code int32) {
	t.Helper()
	if response.Allowed {
		t.Fatal("allowed, want denied")
	}
	if response.Result.Code != code {
		t.Fatalf("code = %d, want %d", response.Result.Code, code)
	}
}

func expectDrainRequested(t *testing.T, f fixture, reason lameduckv1alpha1.DrainReason) {
	t.Helper()
	value, ok := f.drainAnnotation(t)
	if !ok || value != string(reason) {
		t.Fatalf("drain annotation = %q (present %v), want %q", value, ok, reason)
	}
}

func expectNoDrainRequested(t *testing.T, f fixture) {
	t.Helper()
	if value, ok := f.drainAnnotation(t); ok {
		t.Fatalf("drain annotation = %q, want none", value)
	}
}

func TestEvictionAllowedWhenPolicyAllows(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{Evictions: lameduckv1alpha1.EvictionPolicyAllow})
	f := newFixture(t, set, ownedPod(t, testScheme(), set))
	expectAllowed(t, f.eviction.Handle(context.Background(), evictionRequest(userUsername)))
	expectNoDrainRequested(t, f)
}

func TestEvictionAllowedWhenPodMissing(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{})
	f := newFixture(t, set, nil)
	expectAllowed(t, f.eviction.Handle(context.Background(), evictionRequest(userUsername)))
}

func TestEvictionSkipsPatchWhenAlreadyDraining(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{})
	pod := ownedPod(t, testScheme(), set)
	pod.Annotations = map[string]string{lameduckv1alpha1.AnnotationDrainReason: string(lameduckv1alpha1.DrainReasonEviction)}
	f := newFixture(t, set, pod)
	expectDenied(t, f.eviction.Handle(context.Background(), evictionRequest(userUsername)), http.StatusTooManyRequests)
	expectNoDrainRequested(t, f)
}

func TestDeletePolicies(t *testing.T) {
	cases := []struct {
		policy  lameduckv1alpha1.DeletePolicy
		allowed bool
		drains  bool
	}{
		{policy: lameduckv1alpha1.DeletePolicyAllow, allowed: true},
		{policy: lameduckv1alpha1.DeletePolicyDrain, drains: true},
		{policy: lameduckv1alpha1.DeletePolicyDeny},
	}
	for _, c := range cases {
		t.Run(string(c.policy), func(t *testing.T) {
			set := drainSet(lameduckv1alpha1.Protection{Deletes: c.policy})
			pod := ownedPod(t, testScheme(), set)
			f := newFixture(t, set, pod)
			response := f.deletion.Handle(context.Background(), deleteRequest(t, userUsername, pod))
			if c.allowed {
				expectAllowed(t, response)
			} else {
				expectDenied(t, response, http.StatusForbidden)
			}
			if c.drains {
				expectDrainRequested(t, f, lameduckv1alpha1.DrainReasonManual)
			} else {
				expectNoDrainRequested(t, f)
			}
		})
	}
}

func TestTerminateAnnotationBypassesProtection(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{
		Evictions: lameduckv1alpha1.EvictionPolicyDrain,
		Deletes:   lameduckv1alpha1.DeletePolicyDeny,
	})
	pod := ownedPod(t, testScheme(), set)
	pod.Annotations = map[string]string{lameduckv1alpha1.AnnotationTerminate: ""}
	f := newFixture(t, set, pod)
	expectAllowed(t, f.eviction.Handle(context.Background(), evictionRequest(userUsername)))
	expectAllowed(t, f.deletion.Handle(context.Background(), deleteRequest(t, userUsername, pod)))
	expectNoDrainRequested(t, f)
}

func TestControllerBypassesProtection(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{
		Evictions: lameduckv1alpha1.EvictionPolicyDrain,
		Deletes:   lameduckv1alpha1.DeletePolicyDeny,
	})
	pod := ownedPod(t, testScheme(), set)
	f := newFixture(t, set, pod)
	expectAllowed(t, f.eviction.Handle(context.Background(), evictionRequest(controllerUsername)))
	expectAllowed(t, f.deletion.Handle(context.Background(), deleteRequest(t, controllerUsername, pod)))
	expectNoDrainRequested(t, f)
}

func TestSystemUsersBypassDeletesButNotEvictions(t *testing.T) {
	for _, username := range []string{
		"system:serviceaccount:kube-system:generic-garbage-collector",
		nodeUsername,
	} {
		t.Run(username, func(t *testing.T) {
			set := drainSet(lameduckv1alpha1.Protection{
				Evictions: lameduckv1alpha1.EvictionPolicyDrain,
				Deletes:   lameduckv1alpha1.DeletePolicyDeny,
			})
			pod := ownedPod(t, testScheme(), set)
			f := newFixture(t, set, pod)
			expectAllowed(t, f.deletion.Handle(context.Background(), deleteRequest(t, username, pod)))
			expectNoDrainRequested(t, f)
			expectDenied(t, f.eviction.Handle(context.Background(), evictionRequest(username)), http.StatusTooManyRequests)
			expectDrainRequested(t, f, lameduckv1alpha1.DrainReasonEviction)
		})
	}
}

func TestOtherServiceAccountsAreNotTrusted(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{Deletes: lameduckv1alpha1.DeletePolicyDeny})
	pod := ownedPod(t, testScheme(), set)
	f := newFixture(t, set, pod)
	request := deleteRequest(t, "system:serviceaccount:games:deployer", pod)
	expectDenied(t, f.deletion.Handle(context.Background(), request), http.StatusForbidden)
}

func TestPodNotOwnedByDrainSetIsAllowed(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{Deletes: lameduckv1alpha1.DeletePolicyDeny})
	cases := map[string]func(pod *corev1.Pod){
		"no owner": func(pod *corev1.Pod) {
			pod.OwnerReferences = nil
		},
		"no set label": func(pod *corev1.Pod) {
			pod.Labels = nil
		},
		"other kind": func(pod *corev1.Pod) {
			pod.OwnerReferences[0].Kind = "ReplicaSet"
		},
		"label names another set": func(pod *corev1.Pod) {
			pod.Labels[lameduckv1alpha1.LabelSet] = "skywars"
		},
		"stale owner uid": func(pod *corev1.Pod) {
			pod.OwnerReferences[0].UID = "previous-set-uid"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			pod := ownedPod(t, testScheme(), set)
			mutate(pod)
			f := newFixture(t, set, pod)
			expectAllowed(t, f.eviction.Handle(context.Background(), evictionRequest(userUsername)))
			expectAllowed(t, f.deletion.Handle(context.Background(), deleteRequest(t, userUsername, pod)))
			expectNoDrainRequested(t, f)
		})
	}
}

func TestDryRunMakesNoPatch(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{
		Evictions: lameduckv1alpha1.EvictionPolicyDrain,
		Deletes:   lameduckv1alpha1.DeletePolicyDrain,
	})
	pod := ownedPod(t, testScheme(), set)
	f := newFixture(t, set, pod)

	eviction := evictionRequest(userUsername)
	eviction.DryRun = new(true)
	expectDenied(t, f.eviction.Handle(context.Background(), eviction), http.StatusTooManyRequests)

	deletion := deleteRequest(t, userUsername, pod)
	deletion.DryRun = new(true)
	expectDenied(t, f.deletion.Handle(context.Background(), deletion), http.StatusForbidden)

	expectNoDrainRequested(t, f)
}

func TestSetBeingDeletedAllowsDisruption(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{
		Evictions: lameduckv1alpha1.EvictionPolicyDrain,
		Deletes:   lameduckv1alpha1.DeletePolicyDeny,
	})
	set.Finalizers = []string{"lameduck.tommyjs.dev/test"}
	set.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	pod := ownedPod(t, testScheme(), set)
	f := newFixture(t, set, pod)
	expectAllowed(t, f.eviction.Handle(context.Background(), evictionRequest(userUsername)))
	expectAllowed(t, f.deletion.Handle(context.Background(), deleteRequest(t, userUsername, pod)))
	expectNoDrainRequested(t, f)
}

func TestSetMissingAllowsDisruption(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{Deletes: lameduckv1alpha1.DeletePolicyDeny})
	pod := ownedPod(t, testScheme(), set)
	f := newFixture(t, nil, pod)
	expectAllowed(t, f.deletion.Handle(context.Background(), deleteRequest(t, userUsername, pod)))
}

func TestPodAlreadyTerminatingIsAllowed(t *testing.T) {
	set := drainSet(lameduckv1alpha1.Protection{Deletes: lameduckv1alpha1.DeletePolicyDeny})
	pod := ownedPod(t, testScheme(), set)
	f := newFixture(t, set, pod)
	pod.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	expectAllowed(t, f.deletion.Handle(context.Background(), deleteRequest(t, userUsername, pod)))
}
