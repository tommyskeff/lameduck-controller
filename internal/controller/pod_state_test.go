package controller

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const garbled = "garbled"

func TestReadRepairedDrain(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	started := now.Add(-10 * time.Minute)
	timeouts := map[lameduckv1alpha1.DrainReason]time.Duration{
		lameduckv1alpha1.DrainReasonScaleDown: time.Hour,
		lameduckv1alpha1.DrainReasonManual:    15 * time.Minute,
	}
	cases := map[string]struct {
		reason   lameduckv1alpha1.DrainReason
		started  string
		deadline string
		want     time.Time
		repaired bool
	}{
		"valid": {
			reason:   lameduckv1alpha1.DrainReasonScaleDown,
			started:  started.Format(time.RFC3339),
			deadline: now.Add(time.Minute).Format(time.RFC3339),
			want:     now.Add(time.Minute),
		},
		"garbled deadline": {
			reason:   lameduckv1alpha1.DrainReasonScaleDown,
			started:  started.Format(time.RFC3339),
			deadline: garbled,
			want:     started.Add(time.Hour),
			repaired: true,
		},
		"missing deadline": {
			reason:   lameduckv1alpha1.DrainReasonScaleDown,
			started:  started.Format(time.RFC3339),
			want:     started.Add(time.Hour),
			repaired: true,
		},
		"garbled start and deadline": {
			reason:   lameduckv1alpha1.DrainReasonScaleDown,
			started:  garbled,
			deadline: garbled,
			want:     now.Add(time.Hour),
			repaired: true,
		},
		"unknown reason": {
			reason:   "Unknown",
			started:  started.Format(time.RFC3339),
			deadline: garbled,
			want:     started.Add(15 * time.Minute),
			repaired: true,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			pod := &corev1.Pod{}
			pod.Annotations = map[string]string{
				lameduckv1alpha1.AnnotationDrainReason:  string(c.reason),
				lameduckv1alpha1.AnnotationDrainStarted: c.started,
			}
			if c.deadline != "" {
				pod.Annotations[lameduckv1alpha1.AnnotationDrainDeadline] = c.deadline
			}
			drain, repaired := readRepairedDrain(pod, timeouts, now)
			if drain == nil {
				t.Fatal("readRepairedDrain() = nil, want drain")
			}
			if !drain.Deadline.Equal(c.want) || repaired != c.repaired {
				t.Fatalf("readRepairedDrain() deadline = %v repaired = %v, want %v %v",
					drain.Deadline, repaired, c.want, c.repaired)
			}
			if !now.Before(drain.Deadline) {
				t.Fatalf("deadline %v is not after now %v", drain.Deadline, now)
			}
		})
	}
}

func TestReadRepairedDrainWithoutDrain(t *testing.T) {
	if drain, _ := readRepairedDrain(&corev1.Pod{}, nil, time.Now()); drain != nil {
		t.Fatalf("readRepairedDrain() = %v, want nil", drain)
	}
}

func TestWriteDrainPersistsRepair(t *testing.T) {
	now := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	timeouts := map[lameduckv1alpha1.DrainReason]time.Duration{lameduckv1alpha1.DrainReasonManual: time.Hour}
	pod := &corev1.Pod{}
	pod.Annotations = map[string]string{
		lameduckv1alpha1.AnnotationDrainReason:   string(lameduckv1alpha1.DrainReasonManual),
		lameduckv1alpha1.AnnotationDrainStarted:  garbled,
		lameduckv1alpha1.AnnotationDrainDeadline: garbled,
	}
	drain, _ := readRepairedDrain(pod, timeouts, now)
	writeDrain(pod, drain)
	again, repaired := readRepairedDrain(pod, timeouts, now.Add(time.Minute))
	if repaired || !again.Deadline.Equal(now.Add(time.Hour)) {
		t.Fatalf("readRepairedDrain() after write = %v repaired = %v", again.Deadline, repaired)
	}
}
