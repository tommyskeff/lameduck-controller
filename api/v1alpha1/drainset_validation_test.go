package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const appLabel = "app"

func TestValidateSelector(t *testing.T) {
	cases := map[string]struct {
		selector *metav1.LabelSelector
		labels   map[string]string
		valid    bool
	}{
		"missing":   {selector: nil, labels: map[string]string{appLabel: "a"}},
		"empty":     {selector: &metav1.LabelSelector{}, labels: map[string]string{appLabel: "a"}},
		"mismatch":  {selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: "b"}}, labels: map[string]string{appLabel: "a"}},
		"no labels": {selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: "a"}}},
		"expression": {
			selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: "tier", Operator: metav1.LabelSelectorOpIn, Values: []string{"game"},
			}}},
			labels: map[string]string{"tier": "game"},
			valid:  true,
		},
		"match": {
			selector: &metav1.LabelSelector{MatchLabels: map[string]string{appLabel: "a"}},
			labels:   map[string]string{appLabel: "a", "extra": "x"},
			valid:    true,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			spec := DrainSetSpec{Selector: c.selector}
			spec.Template.Labels = c.labels
			if err := spec.ValidateSelector(); (err == nil) != c.valid {
				t.Fatalf("ValidateSelector() = %v, want valid=%v", err, c.valid)
			}
		})
	}
}
