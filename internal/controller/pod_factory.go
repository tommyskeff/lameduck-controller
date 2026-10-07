package controller

import (
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	lameduckv1alpha1 "github.com/tommyskeff/lameduck-controller/api/v1alpha1"
	"github.com/tommyskeff/lameduck-controller/internal/revision"
)

const (
	sidecarContainerName = "lameduck-sidecar"
	sidecarTokenVolume   = "lameduck-token"
	sidecarTokenDir      = "/var/run/secrets/lameduck.tommyjs.dev"
	SidecarAddressEnv    = "LAMEDUCK_SIDECAR_ADDRESS"
)

type PodFactory struct {
	Scheme         *runtime.Scheme
	SidecarImage   string
	SidecarAddress string
	FleetAddress   string
}

func (f PodFactory) Build(set *lameduckv1alpha1.DrainSet, rev revision.Revision) (*corev1.Pod, error) {
	template := rev.Template.DeepCopy()
	pod := &corev1.Pod{}
	pod.GenerateName = set.Name + "-"
	pod.Namespace = set.Namespace
	pod.Labels = maps.Clone(template.Labels)
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Labels[lameduckv1alpha1.LabelSet] = set.Name
	pod.Labels[lameduckv1alpha1.LabelRevision] = rev.Hash
	pod.Labels[lameduckv1alpha1.LabelPhase] = string(lameduckv1alpha1.PodPhaseStarting)
	pod.Labels[lameduckv1alpha1.LabelRoutable] = "false"
	pod.Annotations = maps.Clone(template.Annotations)
	pod.Spec = template.Spec

	for i := range pod.Spec.Containers {
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, corev1.EnvVar{
			Name:  SidecarAddressEnv,
			Value: f.SidecarAddress,
		})
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name: sidecarTokenVolume,
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{
				DefaultMode: new(int32(0o444)),
				Sources: []corev1.VolumeProjection{{
					ServiceAccountToken: &corev1.ServiceAccountTokenProjection{
						Audience:          lameduckv1alpha1.SidecarTokenAudience,
						ExpirationSeconds: ptr.To[int64](3600),
						Path:              "token",
					},
				}},
			},
		},
	})
	pod.Spec.InitContainers = append([]corev1.Container{f.sidecar()}, pod.Spec.InitContainers...)

	if err := controllerutil.SetControllerReference(set, pod, f.Scheme); err != nil {
		return nil, fmt.Errorf("own pod: %w", err)
	}
	return pod, nil
}

func (f PodFactory) sidecar() corev1.Container {
	return corev1.Container{
		Name:          sidecarContainerName,
		Image:         f.SidecarImage,
		Command:       []string{"/sidecar"},
		RestartPolicy: ptr.To(corev1.ContainerRestartPolicyAlways),
		Env: []corev1.EnvVar{
			{Name: "LAMEDUCK_SIDECAR_LISTEN_ADDRESS", Value: f.SidecarAddress},
			{Name: "LAMEDUCK_FLEET_ADDRESS", Value: f.FleetAddress},
			{Name: "LAMEDUCK_TOKEN_PATH", Value: sidecarTokenDir + "/token"},
		},
		VolumeMounts: []corev1.VolumeMount{{
			Name:      sidecarTokenVolume,
			MountPath: sidecarTokenDir,
			ReadOnly:  true,
		}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("5m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: new(false),
			ReadOnlyRootFilesystem:   new(true),
			RunAsNonRoot:             new(true),
			RunAsUser:                ptr.To[int64](65532),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
			SeccompProfile: &corev1.SeccompProfile{
				Type: corev1.SeccompProfileTypeRuntimeDefault,
			},
		},
	}
}
