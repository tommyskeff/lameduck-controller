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

func Hash(template *corev1.PodTemplateSpec) (string, error) {
	data, err := json.Marshal(template)
	if err != nil {
		return "", fmt.Errorf("encode template: %w", err)
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write(data)
	return rand.SafeEncodeString(strconv.FormatUint(uint64(hasher.Sum32()), 10)), nil
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

	hash, err := Hash(&set.Spec.Template)
	if err != nil {
		return Revision{}, nil, err
	}
	var latest int64
	for _, revision := range revisions {
		latest = max(latest, revision.Number)
	}

	for i, revision := range revisions {
		if revision.Hash != hash {
			continue
		}
		if revision.Number == latest {
			return revision, revisions, nil
		}
		revision.Object.Revision = latest + 1
		if err := h.Client.Update(ctx, revision.Object); err != nil {
			return Revision{}, nil, fmt.Errorf("promote revision %s: %w", revision.Object.Name, err)
		}
		revision.Number = latest + 1
		revisions[i] = revision
		return revision, revisions, nil
	}

	data, err := json.Marshal(&set.Spec.Template)
	if err != nil {
		return Revision{}, nil, fmt.Errorf("encode template: %w", err)
	}
	object := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      set.Name + "-" + hash,
			Namespace: set.Namespace,
			Labels: map[string]string{
				lameduckv1alpha1.LabelSet:      set.Name,
				lameduckv1alpha1.LabelRevision: hash,
			},
		},
		Data:     runtime.RawExtension{Raw: data},
		Revision: latest + 1,
	}
	if err := controllerutil.SetControllerReference(set, object, h.Scheme); err != nil {
		return Revision{}, nil, fmt.Errorf("own revision: %w", err)
	}
	if err := h.Client.Create(ctx, object); err != nil && !apierrors.IsAlreadyExists(err) {
		return Revision{}, nil, fmt.Errorf("create revision: %w", err)
	}
	revision := Revision{
		Hash:     hash,
		Number:   object.Revision,
		Template: set.Spec.Template,
		Object:   object,
	}
	return revision, append(revisions, revision), nil
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
