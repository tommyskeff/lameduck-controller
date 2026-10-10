package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const leaderLabelReleaseTimeout = 5 * time.Second

type LeaderLabeler struct {
	Reader client.Reader
	Writer client.Writer
	Pod    types.NamespacedName
	Log    logr.Logger
}

func (l *LeaderLabeler) NeedLeaderElection() bool {
	return true
}

func (l *LeaderLabeler) Start(ctx context.Context) error {
	if err := l.Acquire(ctx); err != nil {
		return err
	}
	l.Log.Info("Labelled pod as leader", "pod", l.Pod)
	<-ctx.Done()

	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaderLabelReleaseTimeout)
	defer cancel()
	if err := l.Release(releaseCtx); err != nil {
		l.Log.Error(err, "Failed to remove leader label", "pod", l.Pod)
	}
	return nil
}

func (l *LeaderLabeler) Acquire(ctx context.Context) error {
	var pods corev1.PodList
	err := l.Reader.List(ctx, &pods,
		client.InNamespace(l.Pod.Namespace),
		client.MatchingLabels{lameduckv1alpha1.LabelLeader: "true"})
	if err != nil {
		return fmt.Errorf("list leader pods: %w", err)
	}
	for _, pod := range pods.Items {
		if pod.Name == l.Pod.Name {
			continue
		}
		if err := client.IgnoreNotFound(l.patch(ctx, pod.Name, false)); err != nil {
			return fmt.Errorf("remove stale leader label: %w", err)
		}
		l.Log.Info("Removed stale leader label", "pod", types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name})
	}
	if err := l.patch(ctx, l.Pod.Name, true); err != nil {
		return fmt.Errorf("add leader label: %w", err)
	}
	return nil
}

func (l *LeaderLabeler) Release(ctx context.Context) error {
	if err := client.IgnoreNotFound(l.patch(ctx, l.Pod.Name, false)); err != nil {
		return fmt.Errorf("remove leader label: %w", err)
	}
	return nil
}

func (l *LeaderLabeler) patch(ctx context.Context, name string, leader bool) error {
	var value *string
	if leader {
		label := "true"
		value = &label
	}
	data, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"labels": map[string]*string{lameduckv1alpha1.LabelLeader: value},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal leader label patch: %w", err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: l.Pod.Namespace, Name: name}}
	if err := l.Writer.Patch(ctx, pod, client.RawPatch(types.MergePatchType, data)); err != nil {
		return fmt.Errorf("patch pod %s/%s: %w", l.Pod.Namespace, name, err)
	}
	return nil
}
