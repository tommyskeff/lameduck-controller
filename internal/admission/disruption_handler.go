package admission

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/metrics"
)

const (
	EvictionPath = "/validate-pod-eviction"
	DeletePath   = "/validate-pod-delete"

	eventReasonDrainRequested = "DrainRequested"
	actionDrain               = "Drain"
)

// +kubebuilder:webhook:path=/validate-pod-eviction,mutating=false,failurePolicy=fail,sideEffects=NoneOnDryRun,groups="",resources=pods/eviction,verbs=create,versions=v1,name=pod-eviction.lameduck.tommyjs.dev,admissionReviewVersions=v1,timeoutSeconds=5
// +kubebuilder:webhook:path=/validate-pod-delete,mutating=false,failurePolicy=ignore,sideEffects=NoneOnDryRun,groups="",resources=pods,verbs=delete,versions=v1,name=pod-delete.lameduck.tommyjs.dev,admissionReviewVersions=v1,timeoutSeconds=5

type DisruptionHandler struct {
	Client             client.Client
	ControllerUsername string
	Eviction           bool
	Recorder           events.EventRecorder
	Metrics            *metrics.DrainMetrics
}

func (h *DisruptionHandler) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod, err := h.pod(ctx, req)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if pod == nil || pod.DeletionTimestamp != nil || h.trusted(req.UserInfo.Username) {
		return admission.Allowed("")
	}
	if _, ok := pod.Annotations[lameduckv1alpha1.AnnotationTerminate]; ok {
		return admission.Allowed("")
	}
	set, err := h.owner(ctx, pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if set == nil {
		return admission.Allowed("")
	}

	if h.Eviction {
		if set.Spec.Protection.Evictions == lameduckv1alpha1.EvictionPolicyAllow {
			return admission.Allowed("")
		}
		if err := h.requestDrain(ctx, req, pod, set, lameduckv1alpha1.DrainReasonEviction); err != nil {
			return admission.Errored(http.StatusInternalServerError, err)
		}
		return tooManyRequests("pod is draining before eviction")
	}

	switch set.Spec.Protection.Deletes {
	case lameduckv1alpha1.DeletePolicyAllow:
		return admission.Allowed("")
	case lameduckv1alpha1.DeletePolicyDeny:
		return admission.Denied("pod is protected; annotate " + lameduckv1alpha1.AnnotationTerminate + " to delete it")
	default:
		if err := h.requestDrain(ctx, req, pod, set, lameduckv1alpha1.DrainReasonManual); err != nil {
			return admission.Errored(http.StatusInternalServerError, err)
		}
		return admission.Denied("pod is draining instead; annotate " + lameduckv1alpha1.AnnotationTerminate + " to delete it now")
	}
}

func (h *DisruptionHandler) pod(ctx context.Context, req admission.Request) (*corev1.Pod, error) {
	if !h.Eviction {
		if len(req.OldObject.Raw) == 0 {
			return nil, nil
		}
		var pod corev1.Pod
		if err := json.Unmarshal(req.OldObject.Raw, &pod); err != nil {
			return nil, fmt.Errorf("decode pod: %w", err)
		}
		return &pod, nil
	}
	var pod corev1.Pod
	err := h.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, &pod)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	return &pod, nil
}

func (h *DisruptionHandler) owner(ctx context.Context, pod *corev1.Pod) (*lameduckv1alpha1.DrainSet, error) {
	name := pod.Labels[lameduckv1alpha1.LabelSet]
	owner := metav1.GetControllerOf(pod)
	if name == "" || owner == nil || owner.Kind != "DrainSet" || owner.Name != name {
		return nil, nil
	}
	var set lameduckv1alpha1.DrainSet
	err := h.Client.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: name}, &set)
	if err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if set.UID != owner.UID || set.DeletionTimestamp != nil {
		return nil, nil
	}
	return &set, nil
}

func (h *DisruptionHandler) trusted(username string) bool {
	if username == h.ControllerUsername {
		return true
	}
	return !h.Eviction && (strings.HasPrefix(username, "system:serviceaccount:kube-system:") ||
		strings.HasPrefix(username, "system:node:"))
}

func (h *DisruptionHandler) requestDrain(
	ctx context.Context,
	req admission.Request,
	pod *corev1.Pod,
	set *lameduckv1alpha1.DrainSet,
	reason lameduckv1alpha1.DrainReason,
) error {
	if req.DryRun != nil && *req.DryRun {
		return nil
	}
	if pod.Annotations[lameduckv1alpha1.AnnotationDrain] == string(reason) ||
		pod.Annotations[lameduckv1alpha1.AnnotationDrainReason] == string(reason) {
		return nil
	}
	patch := client.RawPatch(types.MergePatchType, fmt.Appendf(nil,
		`{"metadata":{"annotations":{%q:%q}}}`, lameduckv1alpha1.AnnotationDrain, reason))
	err := h.Client.Patch(ctx, pod, patch)
	if client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("request drain: %w", err)
	}
	if err != nil {
		return nil
	}
	logf.FromContext(ctx).Info("Requested drain", "pod", pod.Name, "reason", reason)
	operation := "delete"
	if h.Eviction {
		operation = "eviction"
		h.Metrics.EvictionIntercepted(client.ObjectKeyFromObject(set))
	}
	h.Recorder.Eventf(pod, set, corev1.EventTypeNormal, eventReasonDrainRequested, actionDrain,
		"Converted %s into a drain with reason %s", operation, reason)
	return nil
}

func tooManyRequests(message string) admission.Response {
	response := admission.Denied(message)
	response.Result.Code = http.StatusTooManyRequests
	response.Result.Reason = metav1.StatusReasonTooManyRequests
	return response
}
