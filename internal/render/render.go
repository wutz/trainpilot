/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package render turns a TrainTask into the Kubeflow Trainer v2 objects that run
// it: one namespaced TrainingRuntime per round (so the worker pod spec — host
// networking, RDMA, /dev/shm, mounts — is fully controlled) and one TrainJob
// referencing that runtime.
package render

import (
	"fmt"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

const (
	// TrainerNodeLabel marks the replicated job whose pods receive PET_* variables.
	TrainerNodeLabel = "trainer.kubeflow.org/trainjob-ancestor-step"

	// NodeContainerName is the container name Trainer v2 renders for training pods.
	NodeContainerName = "node"

	// SharedMemoryVolumeName is the /dev/shm volume injected into every worker.
	SharedMemoryVolumeName = "dshm"

	// DefaultSharedMemoryRatio is the fraction of the memory limit used for /dev/shm.
	DefaultSharedMemoryRatio = 0.5

	// RDMAKey is the extended resource exposed by the RDMA device plugin.
	RDMAKey corev1.ResourceName = "rdma/hca"

	// GPUKey is the extended GPU resource name.
	GPUKey corev1.ResourceName = "nvidia.com/gpu"

	// GPULabel is the default node label carrying the GPU type.
	GPULabel = "scclabs.dev/gpu"

	// HostnameLabel is the standard hostname label used for anti-affinity and exclusion.
	HostnameLabel = "kubernetes.io/hostname"

	// MASTERPort is the default torch rendezvous port.
	MASTERPort = "29500"

	// InstanceLabel groups all pods of one task across rounds.
	InstanceLabel = "app.kubernetes.io/instance"
)

// Options carries operator-wide defaults injected into every rendered worker.
type Options struct {
	// GPULabel is the node label holding the GPU type (e.g. scclabs.dev/gpu).
	GPULabel string
	// GPUTolerationKeys are tolerated taint keys of GPU nodes.
	GPUTolerationKeys []string
	// SharedMemoryRatio is the /dev/shm fraction of the memory limit.
	SharedMemoryRatio float64
	// ServiceAccountName is set on worker pods when non-empty.
	ServiceAccountName string
}

// Round carries the values that vary per attempt.
type Round struct {
	// RunID is the user-visible run identifier.
	RunID int32
	// Round is the 1-based attempt index inside the run.
	Round int32
	// RestartCount is injected as RESTART_COUNT.
	RestartCount int32
	// ExcludedNodes are hostnames that must not host the new workers.
	ExcludedNodes []string
}

// Names returns the name shared by the TrainingRuntime and TrainJob of a round.
//
// <task>-r<run>-<round>, truncated so that the result stays within the
// 63-character DNS label that Trainer and JobSet require.
func Names(taskName string, r Round) string {
	suffix := fmt.Sprintf("-r%d-%d", r.RunID, r.Round)
	base := taskName
	if max := 63 - len(suffix); len(base) > max {
		base = strings.TrimRight(base[:max], "-")
	}
	return base + suffix
}

// MasterAddr is the rendezvous address of worker 0 for a TrainJob.
// JobSet creates a headless Service named after the JobSet, and Trainer pods are
// named <jobset>-node-0-<index>.
func MasterAddr(trainJobName string) string {
	return fmt.Sprintf("%s-node-0-0.%s", trainJobName, trainJobName)
}

// RDMAEnabled reports whether the task gets host networking and an RDMA device
// (PRD 7.3.5: multi-node tasks enable RDMA automatically when GPUs per worker >= 8;
// for multi-node with fewer GPUs, RDMA and hostNetwork are still enabled to support
// cross-node communication, though RDMA hardware may not be utilized).
func RDMAEnabled(spec *trainpilotv1alpha1.TrainTaskSpec) bool {
	return spec.Workers > 1
}

// ObjectLabels are applied to every object the operator creates for a task.
func ObjectLabels(taskName string, r Round) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":         "trainpilot",
		"app.kubernetes.io/component":    "training",
		InstanceLabel:                    taskName,
		"trainpilot.wutz.dev/task":       taskName,
		"trainpilot.wutz.dev/run-id":     strconv.Itoa(int(r.RunID)),
		"trainpilot.wutz.dev/round":      strconv.Itoa(int(r.Round)),
		"trainpilot.wutz.dev/managed-by": "trainpilot-controller",
	}
}

// TrainingRuntime renders the per-round runtime that fully describes the workers.
func TrainingRuntime(task *trainpilotv1alpha1.TrainTask, r Round, opts Options) *trainerv1alpha1.TrainingRuntime {
	name := Names(task.Name, r)

	podLabels := ObjectLabels(task.Name, r)
	podLabels[TrainerNodeLabel] = "trainer"

	return &trainerv1alpha1.TrainingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: task.Namespace,
			Labels:    ObjectLabels(task.Name, r),
		},
		Spec: trainerv1alpha1.TrainingRuntimeSpec{
			MLPolicy: &trainerv1alpha1.MLPolicy{
				NumNodes: ptr(int32(task.Spec.Workers)),
				MLPolicySource: trainerv1alpha1.MLPolicySource{
					Torch: &trainerv1alpha1.TorchMLPolicySource{},
				},
			},
			Template: trainerv1alpha1.JobSetTemplateSpec{
				Spec: jobsetv1alpha2.JobSetSpec{
					// A headless Service per JobSet gives every worker a stable
					// DNS name, which is what MASTER_ADDR points at (PRD 7.2).
					Network: &jobsetv1alpha2.Network{
						EnableDNSHostnames:       ptr(true),
						PublishNotReadyAddresses: ptr(true),
					},
					// JobSet must never restart a failed Job: the operator owns
					// restart accounting (PRD 7.7) and starts a new round instead.
					FailurePolicy: &jobsetv1alpha2.FailurePolicy{MaxRestarts: 0},
					SuccessPolicy: &jobsetv1alpha2.SuccessPolicy{
						Operator:             jobsetv1alpha2.OperatorAll,
						TargetReplicatedJobs: []string{"node"},
					},
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name:      "node",
							GroupName: "default",
							Replicas:  1,
							Template: batchv1.JobTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
								Spec: batchv1.JobSpec{
									BackoffLimit: ptr(int32(0)),
									Template: corev1.PodTemplateSpec{
										ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
										Spec:       WorkerPodSpec(task, r, opts),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// WorkerPodSpec builds the pod template: resources, /dev/shm, storage mounts,
// RDMA, platform env and the anti-affinity / node-exclusion rules
// (PRD 7.3.4-7.3.7, 7.7.7).
func WorkerPodSpec(task *trainpilotv1alpha1.TrainTask, r Round, opts Options) corev1.PodSpec {
	spec := &task.Spec
	cpu := parseQuantity(spec.Resources.CPU, "8")
	memory := parseQuantity(spec.Resources.Memory, "32Gi")

	limits := corev1.ResourceList{
		corev1.ResourceCPU:    cpu.DeepCopy(),
		corev1.ResourceMemory: memory.DeepCopy(),
	}
	requests := corev1.ResourceList{
		corev1.ResourceCPU:    cpu.DeepCopy(),
		corev1.ResourceMemory: memory.DeepCopy(),
	}
	if g := spec.Resources.GPUsPerWorker; g > 0 {
		gpu := *resource.NewQuantity(int64(g), resource.DecimalSI)
		limits[GPUKey] = gpu
		requests[GPUKey] = gpu.DeepCopy()
	}
	if RDMAEnabled(spec) {
		one := *resource.NewQuantity(1, resource.DecimalSI)
		limits[RDMAKey] = one
		requests[RDMAKey] = one.DeepCopy()
	}

	volumes := []corev1.Volume{sharedMemoryVolume(memory, opts.SharedMemoryRatio)}
	mounts := []corev1.VolumeMount{{
		Name:      SharedMemoryVolumeName,
		MountPath: "/dev/shm",
	}}
	volumes, mounts = appendStorage(volumes, mounts, spec.SharedStorage, "shared")
	if spec.PublicData != nil {
		volumes, mounts = appendStorage(volumes, mounts,
			[]trainpilotv1alpha1.StorageMount{*spec.PublicData}, "public")
	}

	env := platformEnv(task, r)
	env = mergeEnv(env, spec.Env)

	command := containerCommand(strings.Join(spec.Command, "\n"))

	podSpec := corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		TerminationGracePeriodSeconds: ptr(int64(30)),
		Containers: []corev1.Container{{
			Name:         NodeContainerName,
			Image:        spec.Image,
			Command:      command,
			Env:          env,
			Resources:    corev1.ResourceRequirements{Limits: limits, Requests: requests},
			VolumeMounts: mounts,
		}},
		Volumes:      volumes,
		Affinity:     affinity(task, r, opts),
		NodeSelector: nodeSelector(spec, opts),
		Tolerations:  tolerations(spec, opts),
	}
	if opts.ServiceAccountName != "" {
		podSpec.ServiceAccountName = opts.ServiceAccountName
	}
	if RDMAEnabled(spec) {
		podSpec.HostNetwork = true
		podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
	return podSpec
}

// containerCommand turns the TrainTask command into the worker container's
// argv.
//
// The entry point is /bin/sh, which every image that can run a script has:
// busybox, alpine and the slim CUDA variants ship no bash, and pointing argv[0]
// straight at bash fails those images at container start with "bash:
// executable file not found in $PATH". sh then probes for bash and hands over
// when it exists, so images that have it keep the previous `bash -lc`
// behaviour (profile files, bash syntax) while the rest fall back to plain sh.
//
// The script travels as $0 rather than inline text: a fixed wrapper avoids
// having to quote an arbitrary script into a nested -c argument.
func containerCommand(script string) []string {
	return []string{
		"/bin/sh",
		"-c",
		`if command -v bash >/dev/null 2>&1; then exec bash -lc "$0"; fi; exec sh -c "$0"`,
		script,
	}
}

// platformEnv is PRD 7.3.1: the variables every worker receives. PET_* is added
// by Trainer itself and deliberately not duplicated here.
func platformEnv(task *trainpilotv1alpha1.TrainTask, r Round) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "TASK_ID", Value: task.Name},
		{Name: "RUN_ID", Value: strconv.Itoa(int(r.RunID))},
		{Name: "RESTART_COUNT", Value: strconv.Itoa(int(r.RestartCount))},
		{Name: "NNODES", Value: strconv.Itoa(int(task.Spec.Workers))},
		{Name: "MASTER_ADDR", Value: MasterAddr(Names(task.Name, r))},
		{Name: "MASTER_PORT", Value: MASTERPort},
		{
			// Same source as PET_NODE_RANK, offered under the PRD name too.
			Name: "NODE_RANK",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					APIVersion: "v1",
					FieldPath:  "metadata.annotations['batch.kubernetes.io/job-completion-index']",
				},
			},
		},
	}
}

func sharedMemoryVolume(memory resource.Quantity, ratio float64) corev1.Volume {
	if ratio <= 0 || ratio > 1 {
		ratio = DefaultSharedMemoryRatio
	}
	shm := resource.NewQuantity(int64(float64(memory.Value())*ratio), resource.BinarySI)
	return corev1.Volume{
		Name: SharedMemoryVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: shm,
			},
		},
	}
}

func appendStorage(volumes []corev1.Volume, mounts []corev1.VolumeMount,
	items []trainpilotv1alpha1.StorageMount, prefix string) ([]corev1.Volume, []corev1.VolumeMount) {
	for i, s := range items {
		name := fmt.Sprintf("%s-%d", prefix, i)
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: s.ClaimName,
					ReadOnly:  s.ReadOnly,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name:      name,
			MountPath: s.MountPath,
			SubPath:   s.SubPath,
			ReadOnly:  s.ReadOnly,
		})
	}
	return volumes, mounts
}

// mergeEnv lets user variables override platform defaults for non-reserved
// names; reserved and PET_* names are rejected earlier by admission.
func mergeEnv(base, user []corev1.EnvVar) []corev1.EnvVar {
	reserved := map[string]bool{
		"TASK_ID": true, "RUN_ID": true, "RESTART_COUNT": true,
		"NNODES": true, "NODE_RANK": true, "MASTER_ADDR": true, "MASTER_PORT": true,
	}
	out := append([]corev1.EnvVar{}, base...)
	for _, e := range user {
		if reserved[e.Name] || strings.HasPrefix(e.Name, "PET_") {
			continue
		}
		replaced := false
		for i := range out {
			if out[i].Name == e.Name {
				out[i] = e
				replaced = true
				break
			}
		}
		if !replaced {
			out = append(out, e)
		}
	}
	return out
}

func nodeSelector(spec *trainpilotv1alpha1.TrainTaskSpec, opts Options) map[string]string {
	out := map[string]string{}
	if spec.GPUType != "" {
		out[gpuLabel(opts)] = spec.GPUType
	}
	for k, v := range spec.NodeSelector {
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func tolerations(spec *trainpilotv1alpha1.TrainTaskSpec, opts Options) []corev1.Toleration {
	keys := opts.GPUTolerationKeys
	if len(keys) == 0 {
		keys = []string{"nvidia.com/gpu", "scclabs.dev/vgpu"}
	}
	out := make([]corev1.Toleration, 0, len(keys)+len(spec.Tolerations))
	for _, k := range keys {
		out = append(out, corev1.Toleration{
			Key:      k,
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		})
	}
	return append(out, spec.Tolerations...)
}

// affinity spreads workers over distinct nodes and keeps them off nodes excluded
// earlier in the same run (PRD 7.7.7). Hard anti-affinity guarantees the gang
// occupies `workers` different nodes, which multi-node RDMA runs need.
func affinity(task *trainpilotv1alpha1.TrainTask, r Round, opts Options) *corev1.Affinity {
	a := &corev1.Affinity{}
	if task.Spec.Workers > 1 {
		a.PodAntiAffinity = &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{InstanceLabel: task.Name},
				},
				TopologyKey: HostnameLabel,
			}},
		}
	}

	terms := []corev1.NodeSelectorRequirement{}
	if task.Spec.Resources.GPUsPerWorker > 0 {
		terms = append(terms, corev1.NodeSelectorRequirement{
			Key:      gpuLabel(opts),
			Operator: corev1.NodeSelectorOpExists,
		})
	}
	if len(r.ExcludedNodes) > 0 {
		terms = append(terms, corev1.NodeSelectorRequirement{
			Key:      HostnameLabel,
			Operator: corev1.NodeSelectorOpNotIn,
			Values:   r.ExcludedNodes,
		})
	}
	if len(terms) > 0 {
		a.NodeAffinity = &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: terms}},
			},
		}
	}
	if a.PodAntiAffinity == nil && a.NodeAffinity == nil {
		return nil
	}
	return a
}

// TrainJob renders the TrainJob that hands the round to the Trainer controller.
// The node count comes from the runtime's mlPolicy.numNodes; Trainer keeps the
// Job's parallelism/completions in sync with it.
func TrainJob(task *trainpilotv1alpha1.TrainTask, r Round) *trainerv1alpha1.TrainJob {
	name := Names(task.Name, r)
	numNodes := int32(task.Spec.Workers)
	return &trainerv1alpha1.TrainJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: task.Namespace,
			Labels:    ObjectLabels(task.Name, r),
		},
		Spec: trainerv1alpha1.TrainJobSpec{
			RuntimeRef: trainerv1alpha1.RuntimeRef{
				Name:     name,
				APIGroup: ptr(trainerv1alpha1.GroupVersion.Group),
				Kind:     ptr(trainerv1alpha1.TrainingRuntimeKind),
			},
			Trainer: &trainerv1alpha1.Trainer{
				Image:    ptr(task.Spec.Image),
				NumNodes: &numNodes,
			},
		},
	}
}

func gpuLabel(opts Options) string {
	if opts.GPULabel == "" {
		return GPULabel
	}
	return opts.GPULabel
}

func parseQuantity(s, fallback string) resource.Quantity {
	if s == "" {
		s = fallback
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		q = resource.MustParse(fallback)
	}
	return q
}

func ptr[T any](v T) *T { return &v }
