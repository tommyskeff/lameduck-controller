package v1alpha1

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
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

func TestValidateTemplate(t *testing.T) {
	const port int32 = 9460
	app := corev1.Container{Name: "server", Ports: []corev1.ContainerPort{{ContainerPort: 25565}}}
	cases := map[string]struct {
		spec  corev1.PodSpec
		valid bool
	}{
		"clean": {
			spec: corev1.PodSpec{
				Containers: []corev1.Container{app},
				Volumes:    []corev1.Volume{{Name: "data"}},
			},
			valid: true,
		},
		"default port on custom sidecar port": {
			spec:  corev1.PodSpec{Containers: []corev1.Container{{Name: app.Name, Ports: []corev1.ContainerPort{{ContainerPort: DefaultSidecarPort}}}}},
			valid: true,
		},
		"container name": {
			spec: corev1.PodSpec{Containers: []corev1.Container{app, {Name: SidecarContainerName}}},
		},
		"init container name": {
			spec: corev1.PodSpec{Containers: []corev1.Container{app}, InitContainers: []corev1.Container{{Name: SidecarContainerName}}},
		},
		"volume name": {
			spec: corev1.PodSpec{Containers: []corev1.Container{app}, Volumes: []corev1.Volume{{Name: SidecarTokenVolume}}},
		},
		"container port": {
			spec: corev1.PodSpec{Containers: []corev1.Container{{Name: app.Name, Ports: []corev1.ContainerPort{{ContainerPort: port}}}}},
		},
		"init container port": {
			spec: corev1.PodSpec{
				Containers:     []corev1.Container{app},
				InitContainers: []corev1.Container{{Name: "proxy", Ports: []corev1.ContainerPort{{ContainerPort: port}}}},
			},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			spec := DrainSetSpec{}
			spec.Template.Spec = c.spec
			if err := spec.ValidateTemplate(port); (err == nil) != c.valid {
				t.Fatalf("ValidateTemplate() = %v, want valid=%v", err, c.valid)
			}
		})
	}
}
