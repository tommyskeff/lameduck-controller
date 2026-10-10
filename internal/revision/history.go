package revision

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

type History struct {
	Client client.Client
	Scheme *runtime.Scheme
}

type Revision struct {
	Hash     string
	Number   int64
	Template corev1.PodTemplateSpec
	Object   *appsv1.ControllerRevision
}

const maxNameCollisions = 16

func hash(data []byte, collisions int) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write(data)
	if collisions > 0 {
		_, _ = hasher.Write([]byte(strconv.Itoa(collisions)))
	}
	return rand.SafeEncodeString(strconv.FormatUint(uint64(hasher.Sum32()), 10))
}

func (h History) List(ctx context.Context, set *lameduckv1alpha1.DrainSet) ([]Revision, error) {
	var list appsv1.ControllerRevisionList
	err := h.Client.List(ctx, &list,
		client.InNamespace(set.Namespace),
		client.MatchingLabels{lameduckv1alpha1.LabelSet: set.Name},
	)
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	var revisions []Revision
	for i := range list.Items {
		object := &list.Items[i]
		if !metav1.IsControlledBy(object, set) {
			continue
		}
		var template corev1.PodTemplateSpec
		if err := json.Unmarshal(object.Data.Raw, &template); err != nil {
			return nil, fmt.Errorf("decode revision %s: %w", object.Name, err)
		}
		revisions = append(revisions, Revision{
			Hash:     object.Labels[lameduckv1alpha1.LabelRevision],
			Number:   object.Revision,
			Template: template,
			Object:   object,
		})
	}
	slices.SortFunc(revisions, func(a, b Revision) int {
		return cmp.Compare(a.Number, b.Number)
	})
	return revisions, nil
}

func (h History) Sync(ctx context.Context, set *lameduckv1alpha1.DrainSet) (Revision, []Revision, error) {
	revisions, err := h.List(ctx, set)
	if err != nil {
		return Revision{}, nil, err
	}

	if set.Spec.Paused && set.Status.UpdateRevision != "" {
		for _, revision := range revisions {
			if revision.Hash == set.Status.UpdateRevision {
				return revision, revisions, nil
			}
		}
	}

	var latest int64
	for _, revision := range revisions {
		latest = max(latest, revision.Number)
	}

	index, err := h.match(ctx, set, revisions)
	if err != nil {
		return Revision{}, nil, err
	}
	if index >= 0 {
		revision, err := h.promote(ctx, revisions[index], latest)
		if err != nil {
			return Revision{}, nil, err
		}
		revisions[index] = revision
		return revision, revisions, nil
	}

	revision, err := h.create(ctx, set, latest)
	if err != nil {
		return Revision{}, nil, err
	}
	return revision, append(revisions, revision), nil
}

func (h History) match(ctx context.Context, set *lameduckv1alpha1.DrainSet, revisions []Revision) (int, error) {
	for i, revision := range slices.Backward(revisions) {
		if equality.Semantic.DeepEqual(revision.Template, set.Spec.Template) {
			return i, nil
		}
	}
	for i, revision := range slices.Backward(revisions) {
		template, admitted, err := h.defaulted(ctx, set, revision.Template)
		if err != nil {
			return -1, err
		}
		if admitted && equality.Semantic.DeepEqual(template, set.Spec.Template) {
			return i, nil
		}
	}
	return -1, nil
}

func (h History) defaulted(
	ctx context.Context,
	set *lameduckv1alpha1.DrainSet,
	template corev1.PodTemplateSpec,
) (corev1.PodTemplateSpec, bool, error) {
	probe := set.DeepCopy()
	probe.Spec.Template = *template.DeepCopy()
	err := h.Client.Update(ctx, probe, client.DryRunAll)
	switch {
	case err == nil:
		return probe.Spec.Template, true, nil
	case apierrors.IsInvalid(err), apierrors.IsForbidden(err), apierrors.IsBadRequest(err):
		return corev1.PodTemplateSpec{}, false, nil
	default:
		return corev1.PodTemplateSpec{}, false, fmt.Errorf("default revision template: %w", err)
	}
}

func (h History) promote(ctx context.Context, revision Revision, latest int64) (Revision, error) {
	if revision.Number >= latest {
		return revision, nil
	}
	revision.Object.Revision = latest + 1
	if err := h.Client.Update(ctx, revision.Object); err != nil {
		return Revision{}, fmt.Errorf("promote revision %s: %w", revision.Object.Name, err)
	}
	revision.Number = latest + 1
	return revision, nil
}

func (h History) create(ctx context.Context, set *lameduckv1alpha1.DrainSet, latest int64) (Revision, error) {
	data, err := json.Marshal(&set.Spec.Template)
	if err != nil {
		return Revision{}, fmt.Errorf("encode template: %w", err)
	}
	for collisions := range maxNameCollisions {
		name := hash(data, collisions)
		object := &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      set.Name + "-" + name,
				Namespace: set.Namespace,
				Labels: map[string]string{
					lameduckv1alpha1.LabelSet:      set.Name,
					lameduckv1alpha1.LabelRevision: name,
				},
			},
			Data:     runtime.RawExtension{Raw: data},
			Revision: latest + 1,
		}
		if err := controllerutil.SetControllerReference(set, object, h.Scheme); err != nil {
			return Revision{}, fmt.Errorf("own revision: %w", err)
		}
		err := h.Client.Create(ctx, object)
		if err == nil {
			return Revision{Hash: name, Number: object.Revision, Template: set.Spec.Template, Object: object}, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return Revision{}, fmt.Errorf("create revision: %w", err)
		}
		existing, matches, err := h.existing(ctx, set, client.ObjectKeyFromObject(object))
		if err != nil {
			return Revision{}, err
		}
		if matches {
			return h.promote(ctx, existing, latest)
		}
	}
	return Revision{}, fmt.Errorf("create revision: %d name collisions for set %s", maxNameCollisions, set.Name)
}

func (h History) existing(ctx context.Context, set *lameduckv1alpha1.DrainSet, key client.ObjectKey) (Revision, bool, error) {
	var object appsv1.ControllerRevision
	if err := h.Client.Get(ctx, key, &object); err != nil {
		return Revision{}, false, fmt.Errorf("get existing revision %s: %w", key.Name, err)
	}
	if !metav1.IsControlledBy(&object, set) {
		return Revision{}, false, nil
	}
	var template corev1.PodTemplateSpec
	if err := json.Unmarshal(object.Data.Raw, &template); err != nil {
		return Revision{}, false, nil
	}
	if !equality.Semantic.DeepEqual(template, set.Spec.Template) {
		return Revision{}, false, nil
	}
	revision := Revision{
		Hash:     object.Labels[lameduckv1alpha1.LabelRevision],
		Number:   object.Revision,
		Template: template,
		Object:   &object,
	}
	return revision, true, nil
}

func (h History) Prune(ctx context.Context, revisions []Revision, inUse map[string]bool, limit int) error {
	var unused []Revision
	for _, revision := range revisions {
		if !inUse[revision.Hash] {
			unused = append(unused, revision)
		}
	}
	excess := len(unused) - limit
	for _, revision := range unused[:max(excess, 0)] {
		if err := h.Client.Delete(ctx, revision.Object); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete revision %s: %w", revision.Object.Name, err)
		}
	}
	return nil
}
