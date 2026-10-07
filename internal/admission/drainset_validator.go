package admission

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

// +kubebuilder:webhook:path=/validate-lameduck-tommyjs-dev-v1alpha1-drainset,mutating=false,failurePolicy=fail,sideEffects=None,groups=lameduck.tommyjs.dev,resources=drainsets,verbs=create;update,versions=v1alpha1,name=drainset.lameduck.tommyjs.dev,admissionReviewVersions=v1

type DrainSetValidator struct{}

func SetupDrainSetValidator(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &lameduckv1alpha1.DrainSet{}).
		WithValidator(&DrainSetValidator{}).
		Complete()
}

func (v *DrainSetValidator) ValidateCreate(
	_ context.Context,
	obj *lameduckv1alpha1.DrainSet,
) (admission.Warnings, error) {
	return nil, obj.Spec.ValidateSelector()
}

func (v *DrainSetValidator) ValidateUpdate(
	_ context.Context,
	_ *lameduckv1alpha1.DrainSet,
	obj *lameduckv1alpha1.DrainSet,
) (admission.Warnings, error) {
	return nil, obj.Spec.ValidateSelector()
}

func (v *DrainSetValidator) ValidateDelete(
	_ context.Context,
	_ *lameduckv1alpha1.DrainSet,
) (admission.Warnings, error) {
	return nil, nil
}
