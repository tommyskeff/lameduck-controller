//go:build e2e

package e2e

import (
	"context"
	"os/exec"
	"testing"
	"time"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

func drainNode(ctx context.Context, node string) <-chan error {
	result := make(chan error, 1)
	go func() {
		output, err := exec.CommandContext(ctx, "kubectl", "drain", node,
			"--pod-selector="+lameduckv1alpha1.LabelSet+"="+setName,
			"--ignore-daemonsets",
			"--delete-emptydir-data",
			"--timeout="+waitTimeout.String(),
		).CombinedOutput()
		if err != nil {
			err = &drainError{err: err, output: string(output)}
		}
		result <- err
	}()
	return result
}

type drainError struct {
	err    error
	output string
}

func (e *drainError) Error() string {
	return e.err.Error() + ": " + e.output
}

func uncordon(t *testing.T, node string) {
	t.Helper()
	if output, err := exec.Command("kubectl", "uncordon", node).CombinedOutput(); err != nil {
		t.Logf("uncordon %s: %v: %s", node, err, output)
	}
}

func TestNodeDrainDrainsPodBeforeDeletingIt(t *testing.T) {
	f := newFleet(t, 1)
	pod := f.waitForRoutable(1)[0]
	t.Logf("draining node %s running pod %s", pod.Spec.NodeName, pod.Name)

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout+time.Minute)
	defer cancel()
	t.Cleanup(func() { uncordon(t, pod.Spec.NodeName) })
	drained := drainNode(ctx, pod.Spec.NodeName)

	f.until("pod "+pod.Name+" to be deleted after draining for the eviction", func() bool {
		select {
		case err := <-drained:
			t.Fatalf("kubectl drain finished before the pod was deleted: %v", err)
		default:
		}
		_, ok := f.deleted[pod.Name]
		return ok
	})
	f.assertDrainedBeforeDeadline(pod.Name, lameduckv1alpha1.DrainReasonEviction)

	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("kubectl drain: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("kubectl drain did not finish")
	}
}
