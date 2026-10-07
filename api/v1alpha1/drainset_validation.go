package v1alpha1

import (
	"errors"
	"fmt"

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
