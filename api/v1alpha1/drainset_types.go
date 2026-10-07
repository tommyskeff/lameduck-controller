package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// +kubebuilder:validation:Enum=Rollout;ScaleDown;Eviction;Manual
type DrainReason string

const (
	DrainReasonRollout   DrainReason = "Rollout"
	DrainReasonScaleDown DrainReason = "ScaleDown"
	DrainReasonEviction  DrainReason = "Eviction"
	DrainReasonManual    DrainReason = "Manual"
)

// +kubebuilder:validation:Enum=Starting;Active;Draining
type PodPhase string

const (
	PodPhaseStarting PodPhase = "Starting"
	PodPhaseActive   PodPhase = "Active"
	PodPhaseDraining PodPhase = "Draining"
)

// +kubebuilder:validation:Enum=UntilDraining;UntilCurrentAvailable
type OutdatedRouting string

const (
	OutdatedRoutingUntilDraining         OutdatedRouting = "UntilDraining"
	OutdatedRoutingUntilCurrentAvailable OutdatedRouting = "UntilCurrentAvailable"
)

// +kubebuilder:validation:Enum=NotReady;Outdated;Load;Age
type VictimCriterion string

const (
	VictimCriterionNotReady VictimCriterion = "NotReady"
	VictimCriterionOutdated VictimCriterion = "Outdated"
	VictimCriterionLoad     VictimCriterion = "Load"
	VictimCriterionAge      VictimCriterion = "Age"
)

// +kubebuilder:validation:Enum=Never;Hysteresis
type ReselectionPolicy string

const (
	ReselectionPolicyNever      ReselectionPolicy = "Never"
	ReselectionPolicyHysteresis ReselectionPolicy = "Hysteresis"
)

// +kubebuilder:validation:Enum=Drain;Allow
type EvictionPolicy string

const (
	EvictionPolicyDrain EvictionPolicy = "Drain"
	EvictionPolicyAllow EvictionPolicy = "Allow"
)

// +kubebuilder:validation:Enum=Allow;Drain;Deny
type DeletePolicy string

const (
	DeletePolicyAllow DeletePolicy = "Allow"
	DeletePolicyDrain DeletePolicy = "Drain"
	DeletePolicyDeny  DeletePolicy = "Deny"
)

// DrainSetSpec describes a fleet of interchangeable pods that each hold
// state worth draining before they are removed.
type DrainSetSpec struct {
	// Desired number of pods that are not draining.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Selects the pods for Services and autoscalers. Must match the template's labels.
	// +required
	Selector *metav1.LabelSelector `json:"selector"`

	// +required
	Template corev1.PodTemplateSpec `json:"template"`

	// Seconds a pod must be Ready before it counts as available.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinReadySeconds int32 `json:"minReadySeconds,omitempty"`

	// Old revisions kept for rollback and drain cancellation.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	// +optional
	RevisionHistoryLimit *int32 `json:"revisionHistoryLimit,omitempty"`

	// Stops a template change from rolling out. Scaling still happens on the revision already rolling.
	// +optional
	Paused bool `json:"paused,omitempty"`

	// +kubebuilder:default={}
	// +optional
	Rollout RolloutStrategy `json:"rollout,omitempty"`

	// +kubebuilder:default={}
	// +optional
	Drain DrainPolicy `json:"drain,omitempty"`

	// +kubebuilder:default={}
	// +optional
	Routing RoutingPolicy `json:"routing,omitempty"`

	// +kubebuilder:default={}
	// +optional
	VictimSelection VictimSelection `json:"victimSelection,omitempty"`

	// +kubebuilder:default={}
	// +optional
	Protection Protection `json:"protection,omitempty"`
}

type RolloutStrategy struct {
	// Pods allowed above replicas, not counting draining pods.
	// +kubebuilder:default=1
	// +kubebuilder:validation:XIntOrString
	// +optional
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`

	// Pods allowed below replicas that are not available, not counting draining pods.
	// +kubebuilder:default=0
	// +kubebuilder:validation:XIntOrString
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// Pods draining at once for Rollout or ScaleDown. Eviction and Manual drains are not limited.
	// Total pods never exceed replicas + maxSurge + maxDraining, plus eviction and manual drains.
	// +kubebuilder:default=1
	// +kubebuilder:validation:XIntOrString
	// +optional
	MaxDraining *intstr.IntOrString `json:"maxDraining,omitempty"`

	// Seconds the newest revision may go without a new pod becoming available before the rollout is marked stalled.
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=1
	// +optional
	ProgressDeadlineSeconds *int32 `json:"progressDeadlineSeconds,omitempty"`
}

type DrainPolicy struct {
	// How long a pod may drain, per reason, before it is deleted anyway.
	// +kubebuilder:default={}
	// +optional
	Timeouts DrainTimeouts `json:"timeouts,omitempty"`

	// Grace period used when a drained or timed-out pod is deleted.
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=0
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`

	// +kubebuilder:default={}
	// +optional
	CancelOn DrainCancellation `json:"cancelOn,omitempty"`
}

type DrainTimeouts struct {
	// +kubebuilder:default="1h"
	// +optional
	Rollout *metav1.Duration `json:"rollout,omitempty"`

	// +kubebuilder:default="1h"
	// +optional
	ScaleDown *metav1.Duration `json:"scaleDown,omitempty"`

	// +kubebuilder:default="10m"
	// +optional
	Eviction *metav1.Duration `json:"eviction,omitempty"`

	// +kubebuilder:default="15m"
	// +optional
	Manual *metav1.Duration `json:"manual,omitempty"`
}

type DrainCancellation struct {
	// Un-drain a Rollout drain when its pod's revision becomes the newest again.
	// +kubebuilder:default=true
	// +optional
	Rollback *bool `json:"rollback,omitempty"`

	// Un-drain a ScaleDown drain on the newest revision when replicas go back up, before creating pods.
	// +kubebuilder:default=true
	// +optional
	ScaleUp *bool `json:"scaleUp,omitempty"`
}

type RoutingPolicy struct {
	// When outdated pods stop receiving new traffic.
	// UntilDraining keeps them routable until they drain.
	// UntilCurrentAvailable removes them once currentThreshold of the newest revision is available.
	// +kubebuilder:default=UntilDraining
	// +optional
	OutdatedPods OutdatedRouting `json:"outdatedPods,omitempty"`

	// Available pods on the newest revision, absolute or a percentage of replicas.
	// +kubebuilder:default="50%"
	// +kubebuilder:validation:XIntOrString
	// +optional
	CurrentThreshold *intstr.IntOrString `json:"currentThreshold,omitempty"`
}

type VictimSelection struct {
	// Tiebreakers in sequence when choosing which pod to drain. Age prefers the oldest.
	// +kubebuilder:default={NotReady,Outdated,Load,Age}
	// +optional
	Order []VictimCriterion `json:"order,omitempty"`

	// +kubebuilder:default={}
	// +optional
	Load LoadPolicy `json:"load,omitempty"`

	// +kubebuilder:default={}
	// +optional
	Reselection Reselection `json:"reselection,omitempty"`
}

type LoadPolicy struct {
	// A pod whose sidecar has not reported for this long has unknown load, which sorts as the heaviest.
	// +kubebuilder:default="30s"
	// +optional
	StaleAfter *metav1.Duration `json:"staleAfter,omitempty"`
}

// Reselection decides whether a drain moves to a lighter pod that becomes eligible later.
type Reselection struct {
	// +kubebuilder:default=Hysteresis
	// +optional
	Policy ReselectionPolicy `json:"policy,omitempty"`

	// Always swap to a candidate reporting zero load while the draining pod has load.
	// +kubebuilder:default=true
	// +optional
	SwapWhenIdle *bool `json:"swapWhenIdle,omitempty"`

	// The candidate must be at least this much lighter than the draining pod.
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinLoadDelta int64 `json:"minLoadDelta,omitempty"`

	// The candidate's load must be at most this percentage of the draining pod's.
	// +kubebuilder:default=50
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	MaxLoadPercent int32 `json:"maxLoadPercent,omitempty"`

	// Both conditions must hold continuously for this long.
	// +kubebuilder:default="60s"
	// +optional
	SustainFor *metav1.Duration `json:"sustainFor,omitempty"`

	// Minimum time between swaps across the whole set.
	// +kubebuilder:default="5m"
	// +optional
	Cooldown *metav1.Duration `json:"cooldown,omitempty"`

	// Drain reasons that may be swapped. Eviction and Manual drains never are.
	// +kubebuilder:default={Rollout,ScaleDown}
	// +optional
	ApplyTo []DrainReason `json:"applyTo,omitempty"`
}

type Protection struct {
	// Drain turns an eviction into a drain and answers 429, so kubectl drain and autoscalers retry.
	// +kubebuilder:default=Drain
	// +optional
	Evictions EvictionPolicy `json:"evictions,omitempty"`

	// What a direct pod delete does. Annotating lameduck.tommyjs.dev/terminate always deletes immediately.
	// +kubebuilder:default=Drain
	// +optional
	Deletes DeletePolicy `json:"deletes,omitempty"`
}

type DrainSetStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Pods that are not draining.
	// +optional
	Replicas int32 `json:"replicas"`

	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`

	// +optional
	AvailableReplicas int32 `json:"availableReplicas"`

	// Pods on the newest revision that are not draining.
	// +optional
	UpdatedReplicas int32 `json:"updatedReplicas"`

	// +optional
	DrainingReplicas int32 `json:"drainingReplicas"`

	// +optional
	RoutableReplicas int32 `json:"routableReplicas"`

	// +optional
	CurrentRevision string `json:"currentRevision,omitempty"`

	// +optional
	UpdateRevision string `json:"updateRevision,omitempty"`

	// +optional
	Selector string `json:"selector,omitempty"`

	// +optional
	LastSwapTime *metav1.Time `json:"lastSwapTime,omitempty"`

	// +optional
	UpdatedAvailableReplicas int32 `json:"updatedAvailableReplicas"`

	// +optional
	LastProgressTime *metav1.Time `json:"lastProgressTime,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:resource:shortName=dset
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Active",type=integer,JSONPath=`.status.replicas`
// +kubebuilder:printcolumn:name="Available",type=integer,JSONPath=`.status.availableReplicas`
// +kubebuilder:printcolumn:name="Updated",type=integer,JSONPath=`.status.updatedReplicas`
// +kubebuilder:printcolumn:name="Draining",type=integer,JSONPath=`.status.drainingReplicas`
// +kubebuilder:printcolumn:name="Routable",type=integer,JSONPath=`.status.routableReplicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

type DrainSet struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec DrainSetSpec `json:"spec"`

	// +optional
	Status DrainSetStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

type DrainSetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DrainSet `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &DrainSet{}, &DrainSetList{})
		return nil
	})
}
