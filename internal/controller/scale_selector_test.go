package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/planner"
)

const (
	appLabel = "app"
	appName  = "game"
)

func TestScaleSelectorExcludesDrainingPods(t *testing.T) {
	set := &lameduckv1alpha1.DrainSet{}
	set.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: appName}}
	selector, err := scaleSelector(set)
	if err != nil {
		t.Fatalf("scaleSelector() error = %v", err)
	}
	cases := map[lameduckv1alpha1.PodPhase]bool{
		lameduckv1alpha1.PodPhaseStarting: true,
		lameduckv1alpha1.PodPhaseActive:   true,
		lameduckv1alpha1.PodPhaseDraining: false,
	}
	for phase, want := range cases {
		pod := labels.Set{appLabel: appName, lameduckv1alpha1.LabelPhase: string(phase)}
		if got := selector.Matches(pod); got != want {
			t.Errorf("Matches(%s) = %v, want %v", phase, got, want)
		}
	}
	if selector.Matches(labels.Set{appLabel: "other"}) {
		t.Error("Matches(other app) = true, want false")
	}
}

func TestScaleSelectorFollowsDrainAndCancel(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	set := &lameduckv1alpha1.DrainSet{}
	set.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: appName}}
	selector, err := scaleSelector(set)
	if err != nil {
		t.Fatalf("scaleSelector() error = %v", err)
	}
	reconciler := &DrainSetReconciler{}
	state := &reconcileState{set: set, now: now}
	pod := &corev1.Pod{}
	pod.Labels = map[string]string{appLabel: appName}
	if !selector.Matches(labels.Set(pod.Labels)) {
		t.Fatal("Matches(unlabelled pod) = false, want true")
	}

	writeDrain(pod, &planner.Drain{
		Reason:   lameduckv1alpha1.DrainReasonScaleDown,
		Started:  now,
		Deadline: now.Add(time.Hour),
	})
	reconciler.writeLabels(state, pod, false)
	if selector.Matches(labels.Set(pod.Labels)) {
		t.Fatal("Matches(draining pod) = true, want false")
	}

	writeDrain(pod, nil)
	reconciler.writeLabels(state, pod, false)
	if !selector.Matches(labels.Set(pod.Labels)) {
		t.Fatal("Matches(cancelled pod) = false, want true")
	}
}
