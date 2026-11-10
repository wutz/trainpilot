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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TrainTaskPhase is the lifecycle phase of a TrainTask:
// Queued, Starting, Running, Recovering, Succeeded, Failed, Stopped, Interrupted.
type TrainTaskPhase string

const (
	// PhaseQueued means the task is waiting for resources (排队中).
	PhaseQueued TrainTaskPhase = "Queued"
	// PhaseStarting means resources are allocated and workers are being created (启动中).
	PhaseStarting TrainTaskPhase = "Starting"
	// PhaseRunning means all workers execute the startup command (运行中).
	PhaseRunning TrainTaskPhase = "Running"
	// PhaseRecovering means the current round ended and the operator is preparing the next one (恢复中).
	PhaseRecovering TrainTaskPhase = "Recovering"
	// PhaseSucceeded means every worker exited with code 0 (成功).
	PhaseSucceeded TrainTaskPhase = "Succeeded"
	// PhaseFailed means the task will not be restarted, or max runtime was reached (失败).
	PhaseFailed TrainTaskPhase = "Failed"
	// PhaseStopped means the user stopped the task (已停止).
	PhaseStopped TrainTaskPhase = "Stopped"
	// PhaseInterrupted means the task was preempted or queue-timed-out and will not restart (已中断).
	PhaseInterrupted TrainTaskPhase = "Interrupted"
)

// DesiredState lets users stop and continue a task without deleting it.
type DesiredState string

const (
	// DesiredStateRunning means the task should run (创建、继续运行).
	DesiredStateRunning DesiredState = "Running"
	// DesiredStateStopped means the task should stop and release its workers (停止).
	DesiredStateStopped DesiredState = "Stopped"
)

// End reasons recorded on a run segment and in status.message.
const (
	ReasonSucceeded            = "Succeeded"
	ReasonExitCode             = "ExitCode"
	ReasonPreempted            = "Preempted"
	ReasonNodeFailure          = "NodeFailure"
	ReasonStartupCheckFailed   = "StartupCheckFailed"
	ReasonMaxRuntimeExceeded   = "MaxRuntimeExceeded"
	ReasonRestartsExhausted    = "RestartsExhausted"
	ReasonPreemptionsExhausted = "PreemptionsExhausted"
	ReasonQueueTimeout         = "QueueTimeout"
	ReasonUserStopped          = "UserStopped"
	ReasonConfigChanged        = "ConfigChanged"
)

// Starting steps shown next to the Starting phase.
const (
	StepAllocatingResources = "AllocatingResources"
	StepPullingImage        = "PullingImage"
	StepMountingStorage     = "MountingStorage"
	StepStartingContainers  = "StartingContainers"
)

// WorkerResources describes the per-worker compute shape.
type WorkerResources struct {
	// GPUsPerWorker is the number of GPUs attached to every worker.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	GPUsPerWorker int32 `json:"gpusPerWorker,omitempty"`

	// CPU is the CPU request/limit per worker, e.g. "16".
	// +kubebuilder:default="8"
	CPU string `json:"cpu,omitempty"`

	// Memory is the memory request/limit per worker, e.g. "64Gi".
	// +kubebuilder:default="32Gi"
	Memory string `json:"memory,omitempty"`
}

// StorageMount mounts a PVC into every worker.
type StorageMount struct {
	// ClaimName is the PVC to mount.
	// +kubebuilder:validation:MinLength=1
	ClaimName string `json:"claimName"`

	// MountPath must be an absolute path.
	// +kubebuilder:validation:Pattern=`^/[^ ]*$`
	MountPath string `json:"mountPath"`

	// ReadOnly mounts the volume read-only.
	// +kubebuilder:default=false
	ReadOnly bool `json:"readOnly,omitempty"`

	// SubPath is an optional sub-directory of the volume.
	SubPath string `json:"subPath,omitempty"`
}

// FaultTolerance captures the automatic restart configuration.
type FaultTolerance struct {
	// AutoRestart restarts the task after process failures, startup check failures
	// and node failures.
	// +kubebuilder:default=true
	AutoRestart bool `json:"autoRestart,omitempty"`

	// MaxRestarts bounds failure-class automatic restarts (1-100, default 10).
	// Preemptions do not consume this budget.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	MaxRestarts int32 `json:"maxRestarts,omitempty"`

	// MaxPreemptions is the platform-side re-queue budget consumed by preemptions
	// (default 40). It is not part of the user-facing create form.
	// +kubebuilder:default=40
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	MaxPreemptions int32 `json:"maxPreemptions,omitempty"`

	// QueueTimeoutSeconds bounds a single wait in the queue (default 24h).
	// +kubebuilder:default=86400
	// +kubebuilder:validation:Minimum=60
	QueueTimeoutSeconds int64 `json:"queueTimeoutSeconds,omitempty"`

	// MaxRuntimeSeconds bounds the accumulated metered time of one run
	// (1-720 hours, default 168h).
	// +kubebuilder:default=604800
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=2592000
	MaxRuntimeSeconds int64 `json:"maxRuntimeSeconds,omitempty"`
}

// TrainTaskSpec defines the desired state of TrainTask.
type TrainTaskSpec struct {
	// Description is a free-form note, at most 200 characters.
	// +kubebuilder:validation:MaxLength=200
	Description string `json:"description,omitempty"`

	// Image is the training container image.
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// Command is the startup command. The lines are joined and run by the
	// image's POSIX shell (`/bin/sh -c`), which re-executes it under `bash -lc`
	// when the image provides bash. Images without any shell (distroless) are
	// not supported. Exit code 0 means success, anything else means failure.
	// +kubebuilder:validation:MinItems=1
	Command []string `json:"command"`

	// Workers is the number of workers (>= 1). All workers are scheduled as a gang.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=256
	Workers int32 `json:"workers,omitempty"`

	// Resources is the per-worker compute shape.
	// +kubebuilder:default={}
	Resources WorkerResources `json:"resources,omitempty"`

	// GPUType pins workers to nodes exposing this GPU type, matched against the
	// type label configured with --gpu-node-label (e.g. a100, h100).
	GPUType string `json:"gpuType,omitempty"`

	// NodeSelector is merged into the worker node selector.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations are applied to every worker pod.
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// Env is injected into every worker. Values matching the sensitive pattern
	// (KEY / TOKEN / SECRET / PASS) are masked in the UI, not in the pod.
	Env []corev1.EnvVar `json:"env,omitempty"`

	// SharedStorage is mounted read-write into every worker.
	SharedStorage []StorageMount `json:"sharedStorage,omitempty"`

	// PublicData is an optional read-only platform dataset mount.
	PublicData *StorageMount `json:"publicData,omitempty"`

	// FaultTolerance configures automatic restarts and runtime limits.
	// +kubebuilder:default={}
	FaultTolerance FaultTolerance `json:"faultTolerance,omitempty"`

	// DesiredState stops or continues the task. Stopping releases workers and
	// keeps RUN_ID; continuing resets both restart counters.
	// +kubebuilder:default=Running
	// +kubebuilder:validation:Enum=Running;Stopped
	DesiredState DesiredState `json:"desiredState,omitempty"`

	// RunID is the user-facing run identifier. It starts at 1, stays unchanged
	// across automatic restarts and continue operations, and increments on re-run.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	RunID int32 `json:"runID,omitempty"`

	// Suspend stops the underlying TrainJob without ending the task
	// (equivalent to DesiredState=Stopped, kept for Jobs-style UX).
	Suspend bool `json:"suspend,omitempty"`

	// QueueName is the Kueue LocalQueue in this namespace that admits the task.
	// It overrides the operator's default queue (--default-queue-name /
	// Helm defaultQueueName). When neither is set the task is not queued
	// through Kueue and its TrainJob starts unsuspended. Set it to the literal
	// "none" to opt one task out while a default queue is configured.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^(none|[a-z0-9]([-a-z0-9]*[a-z0-9])?)$`
	QueueName string `json:"queueName,omitempty"`
}

// RunSegment is one metered run segment: from the moment every worker is running
// to the moment the round ends.
type RunSegment struct {
	// RunID is the run this segment belongs to.
	RunID int32 `json:"runID"`
	// Round is the 1-based attempt index within the run.
	Round int32 `json:"round"`
	// StartedAt is when all workers started running.
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
	// EndedAt is when the round ended.
	EndedAt *metav1.Time `json:"endedAt,omitempty"`
	// EndReason is one of the Reason* constants.
	EndReason string `json:"endReason,omitempty"`
	// MeteredSeconds is the metered duration of the segment.
	MeteredSeconds int64 `json:"meteredSeconds"`
	// Workers is the worker count the segment was metered at.
	Workers int32 `json:"workers,omitempty"`
}

// WorkerPod reports one worker's placement.
type WorkerPod struct {
	// Name is the pod name.
	Name string `json:"name"`
	// WorkerIndex is PET_NODE_RANK.
	WorkerIndex int32 `json:"workerIndex,omitempty"`
	// Node is the node the worker landed on.
	Node string `json:"node,omitempty"`
	// Phase is the pod phase.
	Phase string `json:"phase,omitempty"`
}

// TrainTaskStatus defines the observed state of TrainTask.
type TrainTaskStatus struct {
	// Phase is the current lifecycle phase.
	Phase TrainTaskPhase `json:"phase,omitempty"`

	// Message explains the current phase or the terminal reason.
	Message string `json:"message,omitempty"`

	// Reason is a machine-readable terminal / recovery reason.
	Reason string `json:"reason,omitempty"`

	// StartingStep is the current step while Phase=Starting.
	StartingStep string `json:"startingStep,omitempty"`

	// RunID is the RUN_ID currently executing.
	RunID int32 `json:"runID,omitempty"`

	// Round is the 1-based attempt counter within the current run.
	Round int32 `json:"round,omitempty"`

	// StartedRound is the round whose TrainJob exists right now (0 when none).
	// It lets the operator tell its own deletions from platform preemption.
	StartedRound int32 `json:"startedRound,omitempty"`

	// RestartCount is the failure-class restart budget already consumed.
	RestartCount int32 `json:"restartCount,omitempty"`

	// PreemptionCount is the preemption re-queue budget already consumed.
	PreemptionCount int32 `json:"preemptionCount,omitempty"`

	// MaxRestarts and MaxPreemptions mirror the effective budgets for display.
	MaxRestarts    int32 `json:"maxRestarts,omitempty"`
	MaxPreemptions int32 `json:"maxPreemptions,omitempty"`

	// ExcludedNodes lists nodes excluded for the rest of this run.
	ExcludedNodes []string `json:"excludedNodes,omitempty"`

	// AppliedAction records the last action id whose status-side effects were
	// applied, so a repeated request is honoured once.
	AppliedAction string `json:"appliedAction,omitempty"`

	// QueuedSince is when the current queue wait started.
	QueuedSince *metav1.Time `json:"queuedSince,omitempty"`

	// StartingSince is when Kueue admitted the round and its workers began
	// starting. It starts the startup timeout; time spent in the queue is not
	// counted against it.
	StartingSince *metav1.Time `json:"startingSince,omitempty"`

	// SegmentStartedAt is when the current metered segment started.
	SegmentStartedAt *metav1.Time `json:"segmentStartedAt,omitempty"`

	// MeteredSeconds is the accumulated metered time of the current run.
	MeteredSeconds int64 `json:"meteredSeconds,omitempty"`

	// Segments is the per-round metering history of the current run.
	Segments []RunSegment `json:"segments,omitempty"`

	// Workers reports the current worker placement.
	Workers []WorkerPod `json:"workers,omitempty"`

	// StartedAt is when all workers of the first round started running.
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletionTime is when the task entered a terminal phase.
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// ObservedGeneration is the last spec generation reconciled.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions are standard Kubernetes conditions.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Workers",type=integer,JSONPath=`.spec.workers`
// +kubebuilder:printcolumn:name="GPU/Worker",type=integer,JSONPath=`.spec.resources.gpusPerWorker`
// +kubebuilder:printcolumn:name="Restarts",type=string,JSONPath=`.status.restartCount`
// +kubebuilder:printcolumn:name="Preemptions",type=string,JSONPath=`.status.preemptionCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TrainTask is the Schema for the traintasks API.
// It wraps a Kubeflow Trainer v2 TrainJob with queueing, automatic restart,
// node exclusion and per-run-segment metering.
type TrainTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TrainTaskSpec   `json:"spec,omitempty"`
	Status TrainTaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TrainTaskList contains a list of TrainTask.
type TrainTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TrainTask `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(SchemeGroupVersion, &TrainTask{}, &TrainTaskList{})
		return nil
	})
}

// IsTerminal reports whether the phase is final.
func (p TrainTaskPhase) IsTerminal() bool {
	switch p {
	case PhaseSucceeded, PhaseFailed, PhaseStopped, PhaseInterrupted:
		return true
	}
	return false
}

// EffectiveMaxRestarts returns the failure-class budget, honouring AutoRestart.
func (s *TrainTaskSpec) EffectiveMaxRestarts() int32 {
	if !s.FaultTolerance.AutoRestart {
		return 0
	}
	return s.FaultTolerance.MaxRestarts
}

// EffectiveMaxPreemptions returns the preemption budget, honouring AutoRestart.
func (s *TrainTaskSpec) EffectiveMaxPreemptions() int32 {
	if !s.FaultTolerance.AutoRestart {
		return 0
	}
	if s.FaultTolerance.MaxPreemptions == 0 {
		return 40
	}
	return s.FaultTolerance.MaxPreemptions
}

// WantsRunning reports whether the task should currently hold resources.
func (s *TrainTaskSpec) WantsRunning() bool {
	return s.DesiredState != DesiredStateStopped && !s.Suspend
}
