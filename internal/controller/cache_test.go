package controller

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

func TestCacheOptionsSelectsManagedObjects(t *testing.T) {
	options, err := CacheOptions()
	if err != nil {
		t.Fatalf("CacheOptions: %v", err)
	}
	selected := map[string]bool{}
	for object, byObject := range options.ByObject {
		switch object.(type) {
		case *corev1.Pod:
			selected["pod"] = true
		case *appsv1.ControllerRevision:
			selected["revision"] = true
		default:
			t.Fatalf("unexpected cached object %T", object)
		}
		if byObject.Label == nil {
			t.Fatalf("%T has no label selector", object)
		}
		if !byObject.Label.Matches(labels.Set{lameduckv1alpha1.LabelSet: "game"}) {
			t.Errorf("%T selector rejects a managed object", object)
		}
		if byObject.Label.Matches(labels.Set{"app": "game"}) {
			t.Errorf("%T selector accepts an unmanaged object", object)
		}
	}
	if !selected["pod"] || !selected["revision"] {
		t.Fatalf("cache options select %v, want pods and revisions", selected)
	}
}
