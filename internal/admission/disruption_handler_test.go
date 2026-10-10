package admission

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/metrics"
)

const (
	testNamespace = "games"
	testSet       = "bedwars"
)

func TestEvictionBecomesDrain(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := lameduckv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	set := &lameduckv1alpha1.DrainSet{ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace, Name: testSet, UID: "set-uid"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: testNamespace,
		Name:      "bedwars-abc",
		Labels:    map[string]string{lameduckv1alpha1.LabelSet: testSet},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: lameduckv1alpha1.GroupVersion.String(),
			Kind:       "DrainSet",
			Name:       testSet,
			UID:        "set-uid",
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
