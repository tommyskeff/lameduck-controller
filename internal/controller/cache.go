package controller

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

func CacheOptions() (cache.Options, error) {
	managed, err := labels.NewRequirement(lameduckv1alpha1.LabelSet, selection.Exists, nil)
	if err != nil {
		return cache.Options{}, fmt.Errorf("build managed selector: %w", err)
	}
	selector := labels.NewSelector().Add(*managed)
	return cache.Options{
		ByObject: map[client.Object]cache.ByObject{
			&corev1.Pod{}:                {Label: selector},
			&appsv1.ControllerRevision{}: {Label: selector},
		},
	}, nil
}
