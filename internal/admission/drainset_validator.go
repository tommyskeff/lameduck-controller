package admission

import (
	"context"
	"errors"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
)

// +kubebuilder:webhook:path=/validate-lameduck-tommyjs-dev-v1alpha1-drainset,mutating=false,failurePolicy=fail,sideEffects=None,groups=lameduck.tommyjs.dev,resources=drainsets,verbs=create;update,versions=v1alpha1,name=drainset.lameduck.tommyjs.dev,admissionReviewVersions=v1,timeoutSeconds=5

type DrainSetValidator struct {
	SidecarPort int32
}

func SetupDrainSetValidator(mgr ctrl.Manager, validator *DrainSetValidator) error {
	return ctrl.NewWebhookManagedBy(mgr, &lameduckv1alpha1.DrainSet{}).
		WithValidator(validator).
		Complete()
}

func (v *DrainSetValidator) ValidateCreate(
	_ context.Context,
	obj *lameduckv1alpha1.DrainSet,
) (admission.Warnings, error) {
	return nil, v.validate(obj)
}

func (v *DrainSetValidator) ValidateUpdate(
	_ context.Context,
	_ *lameduckv1alpha1.DrainSet,
	obj *lameduckv1alpha1.DrainSet,
) (admission.Warnings, error) {
	return nil, v.validate(obj)
}

func (v *DrainSetValidator) ValidateDelete(
	_ context.Context,
	_ *lameduckv1alpha1.DrainSet,
) (admission.Warnings, error) {
	return nil, nil
}

func (v *DrainSetValidator) validate(set *lameduckv1alpha1.DrainSet) error {
	return errors.Join(set.Spec.ValidateSelector(), set.Spec.ValidateTemplate(v.SidecarPort))
}
