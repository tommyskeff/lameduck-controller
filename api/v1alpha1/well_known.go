package v1alpha1

const (
	LabelSet      = "lameduck.tommyjs.dev/set"
	LabelRevision = "lameduck.tommyjs.dev/revision"
	LabelPhase    = "lameduck.tommyjs.dev/phase"
	LabelRoutable = "lameduck.tommyjs.dev/routable"
	LabelLeader   = "lameduck.tommyjs.dev/leader"

	AnnotationDrainReason   = "lameduck.tommyjs.dev/drain-reason"
	AnnotationDrainStarted  = "lameduck.tommyjs.dev/drain-started"
	AnnotationDrainDeadline = "lameduck.tommyjs.dev/drain-deadline"
	AnnotationDrain         = "lameduck.tommyjs.dev/drain"
	AnnotationTerminate     = "lameduck.tommyjs.dev/terminate"

	SidecarTokenAudience = "lameduck.tommyjs.dev"

	SidecarContainerName       = "lameduck-sidecar"
	SidecarTokenVolume         = "lameduck-token"
	DefaultSidecarPort   int32 = 9450

	ConditionAvailable   = "Available"
	ConditionProgressing = "Progressing"
)
