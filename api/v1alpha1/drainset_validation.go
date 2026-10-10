package v1alpha1

import (
	"errors"
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (s *DrainSetSpec) ValidateSelector() error {
	if s.Selector == nil {
		return errors.New("spec.selector is required")
	}
	selector, err := metav1.LabelSelectorAsSelector(s.Selector)
	if err != nil {
		return fmt.Errorf("spec.selector is invalid: %w", err)
	}
	if selector.Empty() {
		return errors.New("spec.selector must not be empty")
	}
	if !selector.Matches(labels.Set(s.Template.Labels)) {
		return errors.New("spec.selector does not match spec.template.metadata.labels")
	}
	return nil
}

func (s *DrainSetSpec) ValidateTemplate(sidecarPort int32) error {
	spec := &s.Template.Spec
	var errs []error
	for _, container := range slices.Concat(spec.InitContainers, spec.Containers) {
		if container.Name == SidecarContainerName {
			errs = append(errs, fmt.Errorf("spec.template container name %q is reserved for the lameduck sidecar", container.Name))
		}
		for _, port := range container.Ports {
			if port.ContainerPort == sidecarPort {
				errs = append(errs, fmt.Errorf(
					"spec.template container %q port %d is reserved for the lameduck sidecar", container.Name, sidecarPort))
			}
		}
	}
	for _, volume := range spec.Volumes {
		if volume.Name == SidecarTokenVolume {
			errs = append(errs, fmt.Errorf("spec.template volume name %q is reserved for the lameduck sidecar", volume.Name))
		}
	}
	return errors.Join(errs...)
}
