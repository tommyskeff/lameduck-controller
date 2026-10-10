//go:build !windows

package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/fleet"
	"github.com/tommyskeff/lameduck-controller/internal/metrics"
	"github.com/tommyskeff/lameduck-controller/internal/revision"
)

const (
	envtestTimeout = 30 * time.Second
	pollInterval   = 100 * time.Millisecond
	setName        = "bedwars"
	fleetCA        = "test fleet certificate authority"
)

type staticCertificateAuthority string

func (c staticCertificateAuthority) PEM() ([]byte, error) {
	return []byte(c), nil
}

var envtestConfig *rest.Config

func TestMain(m *testing.M) {
	os.Exit(runWithEnvtest(m))
}

func runWithEnvtest(m *testing.M) int {
	ctrl.SetLogger(logr.Discard())
	assets, found := envtestAssets()
	if !found {
		return m.Run()
	}
	environment := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: assets,
	}
	restConfig, err := environment.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start envtest: %v\n", err)
		return 1
	}
	envtestConfig = restConfig
	code := m.Run()
	if err := environment.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "stop envtest: %v\n", err)
	}
	return code
}

func envtestAssets() (string, bool) {
	if assets := os.Getenv("KUBEBUILDER_ASSETS"); assets != "" {
		return assets, true
	}
	pattern := filepath.Join("..", "..", "bin", "k8s", "*-"+runtime.GOOS+"-"+runtime.GOARCH)
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		return "", false
	}
	slices.Sort(matches)
	return matches[len(matches)-1], true
}

func envtestScheme() *k8sruntime.Scheme {
	scheme := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(lameduckv1alpha1.AddToScheme(scheme))
	return scheme
}

type envtestFixture struct {
	client    client.Client
	scheme    *k8sruntime.Scheme
	registry  *fleet.Registry
	metrics   *metrics.DrainMetrics
	gatherer  *prometheus.Registry
	namespace string
}

func newEnvtestFixture(t *testing.T) envtestFixture {
	t.Helper()
	if envtestConfig == nil {
		t.Skip("envtest assets not found; run make setup-envtest or set KUBEBUILDER_ASSETS")
	}
	scheme := envtestScheme()
	directClient, err := client.New(envtestConfig, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "drainset-"}}
	if err := directClient.Create(context.Background(), namespace); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	drainMetrics := metrics.NewDrainMetrics()
	gatherer := prometheus.NewRegistry()
	if err := drainMetrics.Register(gatherer); err != nil {
		t.Fatalf("register metrics: %v", err)
	}
	return envtestFixture{
		client:    directClient,
		scheme:    scheme,
		registry:  fleet.NewRegistry(clock.RealClock{}),
		metrics:   drainMetrics,
		gatherer:  gatherer,
		namespace: namespace.Name,
	}
}

func (f envtestFixture) reconciler(c client.Client, recorder events.EventRecorder) *DrainSetReconciler {
	return &DrainSetReconciler{
		Client:   c,
		Scheme:   f.scheme,
		Clock:    clock.RealClock{},
		Registry: f.registry,
		History:  revision.History{Client: c, Scheme: f.scheme},
		Pods: PodFactory{
			Scheme:         f.scheme,
			SidecarImage:   "lameduck:test",
			SidecarAddress: "127.0.0.1:9450",
			FleetAddress:   "lameduck-fleet.lameduck-system.svc:9500",
			FleetCA:        staticCertificateAuthority(fleetCA),
		},
		Expectations: NewExpectations(clock.RealClock{}),
		Recorder:     recorder,
		Metrics:      f.metrics,
	}
}

func (f envtestFixture) startController(t *testing.T) {
	t.Helper()
	cacheOptions, err := CacheOptions()
	if err != nil {
		t.Fatalf("cache options: %v", err)
	}
	cacheOptions.DefaultNamespaces = map[string]cache.Config{f.namespace: {}}
	mgr, err := ctrl.NewManager(envtestConfig, ctrl.Options{
		Scheme:                 f.scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache:                  cacheOptions,
		Controller:             config.Controller{SkipNameValidation: new(true)},
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	if err := f.reconciler(mgr.GetClient(), mgr.GetEventRecorder("lameduck-controller")).SetupWithManager(mgr); err != nil {
		t.Fatalf("set up reconciler: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() {
		stopped <- mgr.Start(ctx)
	}()
	go f.markPodsReady(ctx)
	t.Cleanup(func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Errorf("manager: %v", err)
		}
	})
}

func (f envtestFixture) markPodsReady(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		var list corev1.PodList
		if err := f.client.List(ctx, &list, client.InNamespace(f.namespace)); err != nil {
			continue
		}
		for i := range list.Items {
			pod := &list.Items[i]
			if pod.DeletionTimestamp != nil || podReady(pod) {
				continue
			}
			original := pod.DeepCopy()
			pod.Status.Phase = corev1.PodRunning
			pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
			})
			_ = f.client.Status().Patch(ctx, pod, client.MergeFrom(original))
		}
	}
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (f envtestFixture) createSet(t *testing.T, replicas int32, mutate func(set *lameduckv1alpha1.DrainSet)) *lameduckv1alpha1.DrainSet {
	t.Helper()
	set := &lameduckv1alpha1.DrainSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: f.namespace, Name: setName},
		Spec: lameduckv1alpha1.DrainSetSpec{
			Replicas: new(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: appName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{appLabel: appName}},
				Spec: corev1.PodSpec{
					ServiceAccountName: "default",
					Containers:         []corev1.Container{{Name: appName, Image: "game:v1"}},
				},
			},
		},
	}
	if mutate != nil {
		mutate(set)
	}
	if err := f.client.Create(context.Background(), set); err != nil {
		t.Fatalf("create drain set: %v", err)
	}
	return set
}

func (f envtestFixture) updateSet(t *testing.T, mutate func(set *lameduckv1alpha1.DrainSet)) {
	t.Helper()
	var set lameduckv1alpha1.DrainSet
	key := types.NamespacedName{Namespace: f.namespace, Name: setName}
	if err := f.client.Get(context.Background(), key, &set); err != nil {
		t.Fatalf("get drain set: %v", err)
	}
	original := set.DeepCopy()
	mutate(&set)
	if err := f.client.Patch(context.Background(), &set, client.MergeFrom(original)); err != nil {
		t.Fatalf("patch drain set: %v", err)
	}
}

func (f envtestFixture) set(t *testing.T) lameduckv1alpha1.DrainSet {
	t.Helper()
	var set lameduckv1alpha1.DrainSet
	key := types.NamespacedName{Namespace: f.namespace, Name: setName}
	if err := f.client.Get(context.Background(), key, &set); err != nil {
		t.Fatalf("get drain set: %v", err)
	}
	return set
}

func (f envtestFixture) pods(t *testing.T) []corev1.Pod {
	t.Helper()
	var list corev1.PodList
	err := f.client.List(context.Background(), &list,
		client.InNamespace(f.namespace),
		client.MatchingLabels{lameduckv1alpha1.LabelSet: setName},
	)
	if err != nil {
		t.Fatalf("list pods: %v", err)
	}
	return list.Items
}

func draining(pods []corev1.Pod) []corev1.Pod {
	var result []corev1.Pod
	for _, pod := range pods {
		if _, ok := pod.Annotations[lameduckv1alpha1.AnnotationDrainReason]; ok {
			result = append(result, pod)
		}
	}
	return result
}

func eventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(envtestTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", description)
		}
		time.Sleep(pollInterval)
	}
}

func (f envtestFixture) waitAvailable(t *testing.T, replicas int32) {
	t.Helper()
	eventually(t, fmt.Sprintf("%d replicas are available", replicas), func() bool {
		set := f.set(t)
		return set.Status.AvailableReplicas == replicas && set.Status.Replicas == replicas &&
			len(f.pods(t)) == int(replicas)
	})
}

func (f envtestFixture) waitCompletedDrains(t *testing.T, outcome metrics.Outcome, count int) {
	t.Helper()
	expected := fmt.Sprintf(`
# HELP lameduck_drains_completed_total Drains that ended, by DrainSet and outcome.
# TYPE lameduck_drains_completed_total counter
lameduck_drains_completed_total{drainset=%q,namespace=%q,outcome=%q} %d
`, setName, f.namespace, outcome, count)
	eventually(t, fmt.Sprintf("%d drains complete with outcome %s", count, outcome), func() bool {
		return testutil.GatherAndCompare(f.gatherer, strings.NewReader(expected), "lameduck_drains_completed_total") == nil
	})
}

func (f envtestFixture) waitEvent(t *testing.T, eventType, reason, regarding string) {
	t.Helper()
	eventually(t, fmt.Sprintf("a %s %s event is recorded for %s", eventType, reason, regarding), func() bool {
		var list eventsv1.EventList
		if err := f.client.List(context.Background(), &list, client.InNamespace(f.namespace)); err != nil {
			t.Fatalf("list events: %v", err)
		}
		return slices.ContainsFunc(list.Items, func(event eventsv1.Event) bool {
			return event.Type == eventType && event.Reason == reason && event.Regarding.Name == regarding
		})
	})
}

func TestReconcileCreatesSidecarPods(t *testing.T) {
	f := newEnvtestFixture(t)
	f.startController(t)
	set := f.createSet(t, 3, nil)
	f.waitAvailable(t, 3)

	current := f.set(t)
	for _, pod := range f.pods(t) {
		if !metav1.IsControlledBy(&pod, set) {
			t.Fatalf("pod %s is not controlled by the set", pod.Name)
		}
		if pod.Labels[lameduckv1alpha1.LabelRevision] != current.Status.UpdateRevision {
			t.Fatalf("pod %s revision = %q, want %q", pod.Name, pod.Labels[lameduckv1alpha1.LabelRevision], current.Status.UpdateRevision)
		}
		if len(pod.Spec.InitContainers) == 0 || pod.Spec.InitContainers[0].Name != lameduckv1alpha1.SidecarContainerName {
			t.Fatalf("pod %s has no sidecar", pod.Name)
		}
		if !slices.Contains(pod.Spec.InitContainers[0].Env, corev1.EnvVar{Name: "LAMEDUCK_FLEET_CA", Value: fleetCA}) {
			t.Fatalf("pod %s sidecar does not trust the fleet certificate authority", pod.Name)
		}
	}
	eventually(t, "every pod is active and routable", func() bool {
		for _, pod := range f.pods(t) {
			if pod.Labels[lameduckv1alpha1.LabelPhase] != string(lameduckv1alpha1.PodPhaseActive) ||
				pod.Labels[lameduckv1alpha1.LabelRoutable] != "true" {
				return false
			}
		}
		return true
	})
	if !meta.IsStatusConditionTrue(f.set(t).Status.Conditions, lameduckv1alpha1.ConditionAvailable) {
		t.Fatal("Available condition is not true")
	}
}

func TestScaleDownDrainsBeforeDeleting(t *testing.T) {
	f := newEnvtestFixture(t)
	f.startController(t)
	f.createSet(t, 3, nil)
	f.waitAvailable(t, 3)

	f.updateSet(t, func(set *lameduckv1alpha1.DrainSet) {
		set.Spec.Replicas = ptr.To[int32](1)
	})
	for remaining := 3; remaining > 1; remaining-- {
		var victim corev1.Pod
		eventually(t, "one pod drains for scale down", func() bool {
			pods := f.pods(t)
			drains := draining(pods)
			if len(pods) != remaining || len(drains) != 1 {
				return false
			}
			victim = drains[0]
			return true
		})
		if reason := victim.Annotations[lameduckv1alpha1.AnnotationDrainReason]; reason != string(lameduckv1alpha1.DrainReasonScaleDown) {
			t.Fatalf("drain reason = %q", reason)
		}
		if phase := victim.Labels[lameduckv1alpha1.LabelPhase]; phase != string(lameduckv1alpha1.PodPhaseDraining) {
			t.Fatalf("draining pod phase = %q", phase)
		}
		time.Sleep(time.Second)
		if len(f.pods(t)) != remaining {
			t.Fatal("pod deleted before it reported drained")
		}
		f.registry.RecordDrained(victim.UID)
		eventually(t, "the drained pod is deleted", func() bool {
			return !slices.ContainsFunc(f.pods(t), func(pod corev1.Pod) bool { return pod.UID == victim.UID })
		})
		f.waitEvent(t, corev1.EventTypeNormal, eventReasonDrained, victim.Name)
	}
	f.waitAvailable(t, 1)
	f.waitCompletedDrains(t, metrics.OutcomeDrained, 2)
}

func TestTemplateChangeRollsThroughDrains(t *testing.T) {
	f := newEnvtestFixture(t)
	f.startController(t)
	f.createSet(t, 2, nil)
	f.waitAvailable(t, 2)
	previous := f.set(t).Status.UpdateRevision

	f.updateSet(t, func(set *lameduckv1alpha1.DrainSet) {
		set.Spec.Template.Spec.Containers[0].Image = "game:v2"
	})
	var update string
	eventually(t, "a new revision is observed", func() bool {
		update = f.set(t).Status.UpdateRevision
		return update != previous
	})

	sawRolloutDrain := false
	eventually(t, "every pod runs the new revision", func() bool {
		pods := f.pods(t)
		for _, pod := range draining(pods) {
			if pod.Labels[lameduckv1alpha1.LabelRevision] != previous ||
				pod.Annotations[lameduckv1alpha1.AnnotationDrainReason] != string(lameduckv1alpha1.DrainReasonRollout) {
				t.Fatalf("pod %s drains with revision %q reason %q", pod.Name,
					pod.Labels[lameduckv1alpha1.LabelRevision], pod.Annotations[lameduckv1alpha1.AnnotationDrainReason])
			}
			sawRolloutDrain = true
			f.registry.RecordDrained(pod.UID)
		}
		if len(pods) != 2 {
			return false
		}
		for _, pod := range pods {
			if pod.Labels[lameduckv1alpha1.LabelRevision] != update {
				return false
			}
		}
		return true
	})
	if !sawRolloutDrain {
		t.Fatal("outdated pods were removed without draining")
	}
	eventually(t, "the rollout completes", func() bool {
		status := f.set(t).Status
		return status.CurrentRevision == update && status.UpdatedAvailableReplicas == 2 && status.DrainingReplicas == 0
	})
}

func TestDrainDeadlineForcesDelete(t *testing.T) {
	f := newEnvtestFixture(t)
	f.startController(t)
	f.createSet(t, 2, func(set *lameduckv1alpha1.DrainSet) {
		set.Spec.Drain.Timeouts.ScaleDown = &metav1.Duration{Duration: 2 * time.Second}
	})
	f.waitAvailable(t, 2)

	f.updateSet(t, func(set *lameduckv1alpha1.DrainSet) {
		set.Spec.Replicas = ptr.To[int32](1)
	})
	var victim corev1.Pod
	eventually(t, "one pod drains for scale down", func() bool {
		drains := draining(f.pods(t))
		if len(drains) != 1 {
			return false
		}
		victim = drains[0]
		return true
	})
	deadline, err := time.Parse(time.RFC3339, victim.Annotations[lameduckv1alpha1.AnnotationDrainDeadline])
	if err != nil {
		t.Fatalf("parse drain deadline: %v", err)
	}
	eventually(t, "the undrained pod is deleted at its deadline", func() bool {
		return !slices.ContainsFunc(f.pods(t), func(pod corev1.Pod) bool { return pod.UID == victim.UID })
	})
	if time.Now().Before(deadline) {
		t.Fatalf("pod deleted before its drain deadline %s", deadline)
	}
	f.waitEvent(t, corev1.EventTypeWarning, eventReasonDrainTimeout, victim.Name)
	f.waitAvailable(t, 1)
	f.waitCompletedDrains(t, metrics.OutcomeTimeout, 1)
}

type staleCacheClient struct {
	client.Client
}

func (c staleCacheClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

func TestExpectationsGuardStaleCache(t *testing.T) {
	f := newEnvtestFixture(t)
	set := f.createSet(t, 2, nil)
	reconciler := f.reconciler(staleCacheClient{Client: f.client}, events.NewFakeRecorder(100))
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(set)}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if created := len(f.pods(t)); created != 2 {
		t.Fatalf("first reconcile created %d pods, want 2", created)
	}

	result, err := reconciler.Reconcile(context.Background(), request)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("reconcile on a stale cache did not requeue")
	}
	if created := len(f.pods(t)); created != 2 {
		t.Fatalf("reconcile on a stale cache left %d pods, want 2", created)
	}
}
