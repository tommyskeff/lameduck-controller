package revision

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

const setName = "game"

func template(image string, ports ...corev1.ContainerPort) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": setName}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: image, Ports: ports}},
		},
	}
}

func applyServerDefaults(template *corev1.PodTemplateSpec) {
	for i := range template.Spec.Containers {
		for j := range template.Spec.Containers[i].Ports {
			if template.Spec.Containers[i].Ports[j].Protocol == "" {
				template.Spec.Containers[i].Ports[j].Protocol = corev1.ProtocolTCP
			}
		}
	}
}

func dryRunDefaulting(
	ctx context.Context,
	inner client.WithWatch,
	object client.Object,
	opts ...client.UpdateOption,
) error {
	options := &client.UpdateOptions{}
	options.ApplyOptions(opts)
	set, ok := object.(*lameduckv1alpha1.DrainSet)
	if !ok {
		return inner.Update(ctx, object, opts...)
	}
	if len(options.DryRun) == 0 {
		return errors.New("drain set updated without dry run")
	}
	applyServerDefaults(&set.Spec.Template)
	return nil
}

func drainSet(uid types.UID, image string) *lameduckv1alpha1.DrainSet {
	return &lameduckv1alpha1.DrainSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: lameduckv1alpha1.GroupVersion.String(),
			Kind:       "DrainSet",
		},
		ObjectMeta: metav1.ObjectMeta{Name: setName, Namespace: "default", UID: uid},
		Spec:       lameduckv1alpha1.DrainSetSpec{Template: template(image)},
	}
}

func encode(t *testing.T, image string, ports ...corev1.ContainerPort) []byte {
	t.Helper()
	data, err := json.Marshal(template(image, ports...))
	if err != nil {
		t.Fatalf("encode template: %v", err)
	}
	return data
}

func stored(owner *lameduckv1alpha1.DrainSet, label string, number int64, data []byte) *appsv1.ControllerRevision {
	controller := true
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      setName + "-" + label,
			Namespace: "default",
			Labels: map[string]string{
				lameduckv1alpha1.LabelSet:      setName,
				lameduckv1alpha1.LabelRevision: label,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: lameduckv1alpha1.GroupVersion.String(),
				Kind:       "DrainSet",
				Name:       owner.Name,
				UID:        owner.UID,
				Controller: &controller,
			}},
		},
		Data:     runtime.RawExtension{Raw: data},
		Revision: number,
	}
}

func TestHistorySync(t *testing.T) {
	set := drainSet("set", "app:1")
	foreign := drainSet("deleted", "app:2")
	defaulted := drainSet("set", "app:1")
	defaulted.Spec.Template = template("app:1", corev1.ContainerPort{ContainerPort: 25565, Protocol: corev1.ProtocolTCP})
	undefaulted := encode(t, "app:1", corev1.ContainerPort{ContainerPort: 25565})
	reordered := []byte(`{"spec":{"containers":[{"resources":{},"image":"app:1","name":"app","env":[]}]},"metadata":{"labels":{"app":"game"},"creationTimestamp":null}}`)

	tests := []struct {
		name       string
		set        *lameduckv1alpha1.DrainSet
		existing   []*appsv1.ControllerRevision
		wantHash   string
		wantNumber int64
		wantCount  int
	}{
		{
			name:       "identical template reuses the revision",
			existing:   []*appsv1.ControllerRevision{stored(set, hash(encode(t, "app:1"), 0), 1, encode(t, "app:1"))},
			wantHash:   hash(encode(t, "app:1"), 0),
			wantNumber: 1,
			wantCount:  1,
		},
		{
			name:       "semantically equal template with different encoding reuses the revision",
			existing:   []*appsv1.ControllerRevision{stored(set, "legacy", 1, reordered)},
			wantHash:   "legacy",
			wantNumber: 1,
			wantCount:  1,
		},
		{
			name:       "template the API server has since defaulted reuses the revision",
			set:        defaulted,
			existing:   []*appsv1.ControllerRevision{stored(defaulted, "legacy", 1, undefaulted)},
			wantHash:   "legacy",
			wantNumber: 1,
			wantCount:  1,
		},
		{
			name: "defaulted rollback promotes the old revision",
			set:  defaulted,
			existing: []*appsv1.ControllerRevision{
				stored(defaulted, "first", 1, undefaulted),
				stored(defaulted, "second", 2, encode(t, "app:2")),
			},
			wantHash:   "first",
			wantNumber: 3,
			wantCount:  2,
		},
		{
			name: "rollback promotes the old revision",
			existing: []*appsv1.ControllerRevision{
				stored(set, "first", 1, encode(t, "app:1")),
				stored(set, "second", 2, encode(t, "app:2")),
			},
			wantHash:   "first",
			wantNumber: 3,
			wantCount:  2,
		},
		{
			name:       "changed template creates a revision",
			existing:   []*appsv1.ControllerRevision{stored(set, "old", 1, encode(t, "app:0"))},
			wantHash:   hash(encode(t, "app:1"), 0),
			wantNumber: 2,
			wantCount:  2,
		},
		{
			name:       "name taken by a foreign revision creates a distinct revision",
			existing:   []*appsv1.ControllerRevision{stored(foreign, hash(encode(t, "app:1"), 0), 1, encode(t, "app:1"))},
			wantHash:   hash(encode(t, "app:1"), 1),
			wantNumber: 1,
			wantCount:  1,
		},
		{
			name: "name taken by an owned revision with another template creates a distinct revision",
			existing: []*appsv1.ControllerRevision{
				stored(set, hash(encode(t, "app:1"), 0), 1, encode(t, "app:2")),
			},
			wantHash:   hash(encode(t, "app:1"), 1),
			wantNumber: 2,
			wantCount:  2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := clientgoscheme.AddToScheme(scheme); err != nil {
				t.Fatalf("add client-go scheme: %v", err)
			}
			if err := lameduckv1alpha1.AddToScheme(scheme); err != nil {
				t.Fatalf("add lameduck scheme: %v", err)
			}
			desired := set
			if test.set != nil {
				desired = test.set
			}
			builder := fake.NewClientBuilder().
				WithScheme(scheme).
				WithInterceptorFuncs(interceptor.Funcs{Update: dryRunDefaulting})
			for _, object := range test.existing {
				builder = builder.WithObjects(object.DeepCopy())
			}
			history := History{Client: builder.Build(), Scheme: scheme}

			revision, revisions, err := history.Sync(context.Background(), desired.DeepCopy())
			if err != nil {
				t.Fatalf("sync: %v", err)
			}
			if revision.Hash != test.wantHash {
				t.Errorf("hash = %q, want %q", revision.Hash, test.wantHash)
			}
			if revision.Number != test.wantNumber {
				t.Errorf("number = %d, want %d", revision.Number, test.wantNumber)
			}
			if len(revisions) != test.wantCount {
				t.Errorf("revisions = %d, want %d", len(revisions), test.wantCount)
			}

			var object appsv1.ControllerRevision
			if err := history.Client.Get(context.Background(), client.ObjectKeyFromObject(revision.Object), &object); err != nil {
				t.Fatalf("get revision: %v", err)
			}
			if !metav1.IsControlledBy(&object, desired) {
				t.Errorf("revision %s is not controlled by the set", object.Name)
			}
			if object.Revision != test.wantNumber {
				t.Errorf("stored number = %d, want %d", object.Revision, test.wantNumber)
			}
			if object.Labels[lameduckv1alpha1.LabelRevision] != test.wantHash {
				t.Errorf("stored label = %q, want %q", object.Labels[lameduckv1alpha1.LabelRevision], test.wantHash)
			}

			listed, err := history.List(context.Background(), desired)
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if len(listed) != test.wantCount {
				t.Errorf("stored revisions = %d, want %d", len(listed), test.wantCount)
			}
		})
	}
}
