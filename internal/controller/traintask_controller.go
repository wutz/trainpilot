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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/recorder"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
	"github.com/wutz/trainpilot/internal/render"
)

const (
	// ConditionQueued reports admission state.
	ConditionQueued = "Queued"
	// ConditionWorkersReady reports whether all workers of the round are running.
	ConditionWorkersReady = "WorkersReady"
	// ConditionStopped reports the user-requested stop.
	ConditionStopped = "Stopped"

	// ConditionReasonAdmitted / NotAdmitted qualify ConditionQueued.
	ConditionReasonAdmitted    = "Admitted"
	ConditionReasonNotAdmitted = "NotAdmitted"

	// AnnotationAction carries an idempotent user action request
	// (continue=<id>, rerun=<id>, stop=<id>). The id is compared against
	// status.lastAction so a repeated request is applied once.
	AnnotationAction = "trainpilot.wutz.dev/action"

	// AnnotationActionApplied mirrors the last applied action id.
	AnnotationActionApplied = "trainpilot.wutz.dev/applied-action"

	// taskLabel names the owning TrainTask on every object of a round.
	taskLabel = "trainpilot.wutz.dev/task"

	// roundLabel selects the TrainJob of the current round.
	roundLabel = "trainpilot.wutz.dev/round"

	// queueRequeue is how often a queued task re-evaluates admission.
	queueRequeue = 10 * time.Second
	// runningRequeue is how often a running task refreshes metered seconds.
	runningRequeue = 30 * time.Second
	// startupTimeout bounds Starting before the round counts as a failed startup.
	startupTimeout = 15 * time.Minute
	// maxExcludedNodes is the cap on per-run node exclusion.
	maxExcludedNodes = 8
)

// TrainTaskReconciler reconciles a TrainTask object.
type TrainTaskReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder recorder.EventRecorder

	// GPULabel, GPUTolerationKeys, SharedMemoryRatio and ServiceAccountName are
	// operator-wide render options (flags in main.go).
	GPULabel           string
	GPUTolerationKeys  []string
	SharedMemoryRatio  float64
	ServiceAccountName string

	// DefaultQueueName is the Kueue LocalQueue used when a TrainTask does not
	// set spec.queueName. Empty means tasks are not queued through Kueue unless
	// they name a queue themselves.
	DefaultQueueName string

	// Now is overridable for tests.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=trainpilot.wutz.dev,resources=traintasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=trainpilot.wutz.dev,resources=traintasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=trainpilot.wutz.dev,resources=traintasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=trainjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=trainjobs/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=trainingruntimes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=trainer.kubeflow.org,resources=clustertrainingruntimes,verbs=get;list;watch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile drives one TrainTask through its lifecycle.
func (r *TrainTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var task trainpilotv1alpha1.TrainTask
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !task.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	now := r.now()
	origStatus := task.Status.DeepCopy()

	// Apply a pending user action (continue / rerun) before anything else.
	// The change lives in spec/metadata, so it must be persisted with Update and
	// reconciled again — status-only writes would silently drop it.
	if r.applyAction(&task) {
		// spec and metadata go through Update, status through the subresource;
		// continue resets counters, so both writes can be needed at once.
		if err := r.Update(ctx, &task); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// A continue request resets per-run budgets, which is a status change and is
	// therefore applied here rather than in applyAction.
	r.applyStatusAction(&task)

	// A new spec.runID starts a fresh run: counters, exclusions, segments reset.
	if task.Spec.RunID != task.Status.RunID {
		if err := r.beginRun(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
	}

	result, err := r.reconcile(ctx, &task, now)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !equalStatus(origStatus, &task.Status) {
		if err := r.Status().Update(ctx, &task); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err
		}
		logger.V(1).Info("status updated", "phase", task.Status.Phase,
			"run", task.Status.RunID, "round", task.Status.Round)
	}
	return result, nil
}

// reconcile is the phase machine; it mutates task.Status in memory and returns
// the requeue interval.
func (r *TrainTaskReconciler) reconcile(ctx context.Context, task *trainpilotv1alpha1.TrainTask, now time.Time) (ctrl.Result, error) {
	// User stopped: close the segment, drop the round, release everything.
	if !task.Spec.WantsRunning() {
		if task.Status.Phase != trainpilotv1alpha1.PhaseStopped {
			r.stopRound(ctx, task, now, trainpilotv1alpha1.ReasonUserStopped)
			task.Status.Phase = trainpilotv1alpha1.PhaseStopped
			task.Status.Reason = trainpilotv1alpha1.ReasonUserStopped
			task.Status.Message = "已停止：用户停止运行，worker 已释放"
			task.Status.CompletionTime = timePtr(now)
			r.setCondition(task, ConditionStopped, metav1.ConditionTrue, "UserStopped", "stopped by the user")
			r.event(task, corev1.EventTypeNormal, "Stopped", "task stopped by user")
		}
		return ctrl.Result{}, nil
	}

	// Terminal states are final until a new RUN_ID or a continue action.
	if task.Status.Phase.IsTerminal() {
		return ctrl.Result{}, nil
	}

	round := render.Round{
		RunID:         task.Spec.RunID,
		Round:         max(task.Status.Round, 1),
		RestartCount:  task.Status.RestartCount,
		ExcludedNodes: task.Status.ExcludedNodes,
	}
	task.Status.Round = round.Round
	task.Status.RunID = task.Spec.RunID

	// Max runtime is measured on metered time, not wall clock.
	r.accrueMetering(task, now)
	if limit := task.Spec.FaultTolerance.MaxRuntimeSeconds; limit > 0 && task.Status.MeteredSeconds >= limit {
		r.endRun(ctx, task, now, trainpilotv1alpha1.ReasonMaxRuntimeExceeded)
		return ctrl.Result{}, nil
	}

	var tj trainerv1alpha1.TrainJob
	err := r.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: render.Names(task.Name, round)}, &tj)
	switch {
	case apierrors.IsNotFound(err):
		// A round we started whose TrainJob vanished was removed by the platform
		// (preemption / eviction), not by us: charge the preemption budget.
		if task.Status.StartedRound == round.Round && round.Round > 0 {
			// finishRound charges the preemption budget for ReasonPreempted.
			r.event(task, corev1.EventTypeWarning, "Preempted",
				fmt.Sprintf("round %d was removed by the platform", round.Round))
			return r.finishRound(ctx, task, round, now, trainpilotv1alpha1.ReasonPreempted, false)
		}
		return r.startOrQueue(ctx, task, round, now)
	case err != nil:
		return ctrl.Result{}, err
	}

	return r.observeRound(ctx, task, &tj, round, now)
}

// startOrQueue creates the round's TrainJob and leaves the task Queued until
// Kueue admits it.
func (r *TrainTaskReconciler) startOrQueue(ctx context.Context, task *trainpilotv1alpha1.TrainTask, round render.Round, now time.Time) (ctrl.Result, error) {
	if task.Status.QueuedSince == nil {
		task.Status.QueuedSince = timePtr(now)
	}
	if r.queueExpired(ctx, task, now) {
		return ctrl.Result{}, nil
	}

	// The TrainJob is created suspended. Kueue unsuspends it once the queue
	// admits the whole job, which is the gang admission step.
	if err := r.startRound(ctx, task, round); err != nil {
		return ctrl.Result{}, err
	}
	task.Status.StartedRound = round.Round

	// Without a queue there is nothing to wait for: the TrainJob is created
	// unsuspended and its workers start as soon as the scheduler places them.
	if render.QueueName(task, r.renderOptions()) == "" {
		task.Status.Phase = trainpilotv1alpha1.PhaseStarting
		task.Status.QueuedSince = nil
		task.Status.StartingSince = timePtr(now)
		task.Status.Message = "启动中：等待 worker 调度"
		r.setCondition(task, ConditionQueued, metav1.ConditionTrue, ConditionReasonAdmitted, "queueing disabled")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	task.Status.Phase = trainpilotv1alpha1.PhaseQueued
	task.Status.Message = "排队中：等待 Kueue 准入"
	r.setCondition(task, ConditionQueued, metav1.ConditionFalse, ConditionReasonNotAdmitted, "waiting for Kueue admission")
	r.event(task, corev1.EventTypeNormal, "Queued", "TrainJob created suspended, waiting for Kueue admission")
	return ctrl.Result{RequeueAfter: queueRequeue}, nil
}

// renderOptions are the operator-wide values injected into every rendered object.
func (r *TrainTaskReconciler) renderOptions() render.Options {
	return render.Options{
		GPULabel:           r.GPULabel,
		GPUTolerationKeys:  r.GPUTolerationKeys,
		SharedMemoryRatio:  r.SharedMemoryRatio,
		ServiceAccountName: r.ServiceAccountName,
		DefaultQueueName:   r.DefaultQueueName,
	}
}

// queueExpired enforces the queue timeout and reports whether the task was
// interrupted. It runs on every reconcile of a Queued task because a suspended
// TrainJob can wait for as long as Kueue keeps it out of the queue.
// A timeout can also fire mid-restart, when the previous round was already
// metered, so it closes whatever segment is open and drops stale round objects.
func (r *TrainTaskReconciler) queueExpired(ctx context.Context, task *trainpilotv1alpha1.TrainTask, now time.Time) bool {
	timeout := time.Duration(task.Spec.FaultTolerance.QueueTimeoutSeconds) * time.Second
	if timeout <= 0 || task.Status.QueuedSince == nil || now.Sub(task.Status.QueuedSince.Time) <= timeout {
		return false
	}
	r.stopRound(ctx, task, now, trainpilotv1alpha1.ReasonQueueTimeout)
	task.Status.Phase = trainpilotv1alpha1.PhaseInterrupted
	task.Status.Reason = trainpilotv1alpha1.ReasonQueueTimeout
	task.Status.Message = "已中断：排队超时，未获得资源"
	task.Status.CompletionTime = timePtr(now)
	r.setCondition(task, ConditionQueued, metav1.ConditionFalse, trainpilotv1alpha1.ReasonQueueTimeout, "queue timeout")
	r.event(task, corev1.EventTypeWarning, "QueueTimeout", "task waited longer than the queue timeout")
	return true
}

// startRound renders the runtime and TrainJob for the round and creates them.
func (r *TrainTaskReconciler) startRound(ctx context.Context, task *trainpilotv1alpha1.TrainTask, round render.Round) error {
	opts := r.renderOptions()

	rt := render.TrainingRuntime(task, round, opts)
	if err := controllerutil.SetControllerReference(task, rt, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, rt); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create training runtime: %w", err)
	}

	tj := render.TrainJob(task, round, opts)
	if err := controllerutil.SetControllerReference(task, tj, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, tj); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create train job: %w", err)
	}
	return nil
}

// observeRound reads the current round's pods and TrainJob and advances the phase.
func (r *TrainTaskReconciler) observeRound(ctx context.Context, task *trainpilotv1alpha1.TrainTask,
	tj *trainerv1alpha1.TrainJob, round render.Round, now time.Time) (ctrl.Result, error) {

	pods, err := r.roundPods(ctx, task, round)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Queued resolves into Starting only after Kueue admits the round. A
	// suspended TrainJob has no worker pods yet, so nothing is starting: keeping
	// the task Queued keeps the queue timeout running and the startup timeout
	// stopped.
	if task.Status.Phase == trainpilotv1alpha1.PhaseQueued || task.Status.Phase == "" {
		if suspended(tj) {
			if task.Status.QueuedSince == nil {
				task.Status.QueuedSince = timePtr(now)
			}
			if r.queueExpired(ctx, task, now) {
				return ctrl.Result{}, nil
			}
			task.Status.Phase = trainpilotv1alpha1.PhaseQueued
			task.Status.Message = "排队中：等待 Kueue 准入"
			r.setCondition(task, ConditionQueued, metav1.ConditionFalse, ConditionReasonNotAdmitted, "waiting for Kueue admission")
			return ctrl.Result{RequeueAfter: queueRequeue}, nil
		}
		task.Status.Phase = trainpilotv1alpha1.PhaseStarting
		task.Status.QueuedSince = nil
		if task.Status.StartingSince == nil {
			task.Status.StartingSince = timePtr(now)
		}
		r.setCondition(task, ConditionQueued, metav1.ConditionTrue, ConditionReasonAdmitted, "admitted by Kueue")
		r.event(task, corev1.EventTypeNormal, "Admitted", "Kueue admitted the TrainJob")
	}

	// Kueue re-suspends an admitted TrainJob to preempt it. The round lost its
	// quota, which is a preemption, not a failure: it spends the preemption
	// budget and the next round queues again.
	if suspended(tj) {
		r.event(task, corev1.EventTypeWarning, "Preempted", "Kueue suspended the TrainJob to reclaim its quota")
		return r.finishRound(ctx, task, round, now, trainpilotv1alpha1.ReasonPreempted, false)
	}

	r.refreshWorkers(task, pods)

	if task.Status.Phase == trainpilotv1alpha1.PhaseStarting || task.Status.Phase == trainpilotv1alpha1.PhaseRecovering {
		if allRunning(pods, int(task.Spec.Workers)) {
			r.markRunning(task, pods, now)
			r.event(task, corev1.EventTypeNormal, "Running", "all workers are running")
			return ctrl.Result{RequeueAfter: runningRequeue}, nil
		}
		r.updateStartingStep(task, pods, now)
		if stepFailed := r.detectStartupFailure(task, pods, now); stepFailed != "" {
			return r.finishRound(ctx, task, round, now, stepFailed, false)
		}
		// A short round can start and finish between two polls, so the phase is
		// still Starting when the TrainJob turns terminal. Without this the
		// status would requeue forever and never reach Succeeded or Failed.
		if done, reason := trainJobOutcome(tj); done {
			r.openSegmentFromPods(task, pods, now)
			return r.finishRound(ctx, task, round, now, reason, false)
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Running: the TrainJob condition decides the round's outcome.
	if done, reason := trainJobOutcome(tj); done {
		return r.finishRound(ctx, task, round, now, reason, false)
	}
	if failed := r.detectWorkerFailure(ctx, task, pods); failed != "" {
		return r.finishRound(ctx, task, round, now, failed, false)
	}

	r.accrueMetering(task, now)
	return ctrl.Result{RequeueAfter: runningRequeue}, nil
}

// finishRound closes the segment, meters it and decides restart vs terminal.
func (r *TrainTaskReconciler) finishRound(ctx context.Context, task *trainpilotv1alpha1.TrainTask,
	round render.Round, now time.Time, reason string, _ bool) (ctrl.Result, error) {

	r.closeSegment(task, now, reason)

	success := reason == trainpilotv1alpha1.ReasonSucceeded
	if success {
		// Keep the finished round and its pods: they are the only record of the
		// workers' output, and a Succeeded pod holds no resources. Deleting the
		// task still cascades everything away through the owner reference.
		task.Status.Phase = trainpilotv1alpha1.PhaseSucceeded
		task.Status.Reason = reason
		task.Status.Message = "成功：所有 worker 正常退出"
		task.Status.CompletionTime = timePtr(now)
		r.event(task, corev1.EventTypeNormal, "Succeeded", "all workers exited successfully")
		return ctrl.Result{}, nil
	}

	// A round that failed is replaced by the next one, so drop it before the
	// gang re-queues: leaving it would keep its pods and their GPU/NIC
	// requests counted against the next round's admission.
	r.deleteRound(ctx, task, round)

	// Classify: preemption spends the operator-side budget, everything else the
	// user-facing restart budget.
	if reason == trainpilotv1alpha1.ReasonPreempted {
		task.Status.PreemptionCount++
	} else {
		task.Status.RestartCount++
	}
	task.Status.Message = fmt.Sprintf("恢复中：第 %d 轮结束（%s）", round.Round, reason)
	r.event(task, corev1.EventTypeWarning, "RoundFailed", fmt.Sprintf("round %d ended: %s", round.Round, reason))

	maxRestarts := task.Spec.EffectiveMaxRestarts()
	maxPreemptions := task.Spec.EffectiveMaxPreemptions()
	task.Status.MaxRestarts = maxRestarts
	task.Status.MaxPreemptions = maxPreemptions

	if task.Status.PreemptionCount > maxPreemptions {
		return r.terminal(task, now, trainpilotv1alpha1.PhaseInterrupted,
			trainpilotv1alpha1.ReasonPreemptionsExhausted, "已中断：抢占重排次数已达上限")
	}
	if task.Status.RestartCount > maxRestarts {
		return r.terminal(task, now, trainpilotv1alpha1.PhaseFailed,
			trainpilotv1alpha1.ReasonRestartsExhausted, "失败：自动重启次数已达上限")
	}

	// Back off a little so a crash loop cannot spin the API server.
	backoff := time.Duration(min(10*int(task.Status.RestartCount)+5, 60)) * time.Second
	task.Status.Phase = trainpilotv1alpha1.PhaseRecovering
	task.Status.Round = round.Round + 1
	task.Status.QueuedSince = timePtr(now.Add(backoff))
	task.Status.StartingStep = ""
	task.Status.StartingSince = nil
	return ctrl.Result{RequeueAfter: backoff}, nil
}

func (r *TrainTaskReconciler) terminal(task *trainpilotv1alpha1.TrainTask,
	now time.Time, phase trainpilotv1alpha1.TrainTaskPhase, reason, message string) (ctrl.Result, error) {
	task.Status.Phase = phase
	task.Status.Reason = reason
	task.Status.Message = message
	task.Status.CompletionTime = timePtr(now)
	r.event(task, corev1.EventTypeWarning, reason, message)
	return ctrl.Result{}, nil
}

// endRun closes an open segment and terminates the run.
func (r *TrainTaskReconciler) endRun(ctx context.Context, task *trainpilotv1alpha1.TrainTask, now time.Time, reason string) {
	r.closeSegment(task, now, reason)
	round := render.Round{RunID: task.Spec.RunID, Round: max(task.Status.Round, 1)}
	r.deleteRound(ctx, task, round)
	phase := trainpilotv1alpha1.PhaseFailed
	switch reason {
	case trainpilotv1alpha1.ReasonMaxRuntimeExceeded:
		phase = trainpilotv1alpha1.PhaseFailed
	case trainpilotv1alpha1.ReasonUserStopped:
		phase = trainpilotv1alpha1.PhaseStopped
	}
	task.Status.Phase = phase
	task.Status.Reason = reason
	task.Status.Message = "失败：已达最长运行时长，任务终止"
	task.Status.CompletionTime = timePtr(now)
	r.event(task, corev1.EventTypeWarning, reason, task.Status.Message)
}

// stopRound closes the segment and removes the round's objects.
func (r *TrainTaskReconciler) stopRound(ctx context.Context, task *trainpilotv1alpha1.TrainTask, now time.Time, reason string) {
	r.closeSegment(task, now, reason)
	round := render.Round{RunID: task.Spec.RunID, Round: max(task.Status.Round, 1)}
	r.deleteRound(ctx, task, round)
}

// beginRun resets per-run state when spec.runID changes (重跑).
func (r *TrainTaskReconciler) beginRun(ctx context.Context, task *trainpilotv1alpha1.TrainTask) error {
	oldRun := task.Status.RunID

	// Remove everything owned by the previous run.
	var tjs trainerv1alpha1.TrainJobList
	if err := r.List(ctx, &tjs, client.InNamespace(task.Namespace),
		client.MatchingLabels{taskLabel: task.Name}); err != nil {
		return err
	}
	for i := range tjs.Items {
		if ref := metav1.GetControllerOf(&tjs.Items[i]); ref == nil || ref.UID != task.UID {
			continue
		}
		if err := r.Delete(ctx, &tjs.Items[i]); client.IgnoreNotFound(err) != nil {
			return err
		}
	}

	var rts trainerv1alpha1.TrainingRuntimeList
	if err := r.List(ctx, &rts, client.InNamespace(task.Namespace),
		client.MatchingLabels{taskLabel: task.Name}); err != nil {
		return err
	}
	for i := range rts.Items {
		if err := r.Delete(ctx, &rts.Items[i]); client.IgnoreNotFound(err) != nil {
			return err
		}
	}

	task.Status.RunID = task.Spec.RunID
	task.Status.Round = 1
	task.Status.RestartCount = 0
	task.Status.PreemptionCount = 0
	task.Status.ExcludedNodes = nil
	task.Status.MeteredSeconds = 0
	task.Status.Segments = nil
	task.Status.Workers = nil
	task.Status.StartedAt = nil
	task.Status.CompletionTime = nil
	task.Status.SegmentStartedAt = nil
	task.Status.AppliedAction = ""
	task.Status.Phase = trainpilotv1alpha1.PhaseQueued
	task.Status.Message = "排队中"
	if oldRun != 0 {
		r.event(task, corev1.EventTypeNormal, "RunStarted", fmt.Sprintf("run %d started", task.Spec.RunID))
	}
	return nil
}

func (r *TrainTaskReconciler) deleteRound(ctx context.Context, task *trainpilotv1alpha1.TrainTask, round render.Round) {
	task.Status.StartedRound = 0
	name := render.Names(task.Name, round)
	_ = r.Delete(ctx, &trainerv1alpha1.TrainJob{Name: name, Namespace: task.Namespace})
	_ = r.Delete(ctx, &trainerv1alpha1.TrainingRuntime{Name: name, Namespace: task.Namespace})
}

// roundPods lists the worker pods of one round.
func (r *TrainTaskReconciler) roundPods(ctx context.Context, task *trainpilotv1alpha1.TrainTask, round render.Round) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(task.Namespace), client.MatchingLabels{
		taskLabel:  task.Name,
		roundLabel: strconv.Itoa(int(round.Round)),
	}); err != nil {
		return nil, err
	}
	out := make([]corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Labels[render.TrainerNodeLabel] == "trainer" || p.DeletionTimestamp.IsZero() {
			out = append(out, *p)
		}
	}
	slices.SortFunc(out, func(a, b corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

func allRunning(pods []corev1.Pod, want int) bool {
	if len(pods) < want {
		return false
	}
	for i := range pods {
		if pods[i].Status.Phase != corev1.PodRunning || !podReady(&pods[i]) {
			return false
		}
	}
	return true
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return p.Status.Phase == corev1.PodRunning
}

// markRunning opens the metered segment (metering starts when every worker is
// running).
func (r *TrainTaskReconciler) markRunning(task *trainpilotv1alpha1.TrainTask, pods []corev1.Pod, now time.Time) {
	task.Status.Phase = trainpilotv1alpha1.PhaseRunning
	task.Status.StartingStep = ""
	task.Status.QueuedSince = nil
	task.Status.StartingSince = nil
	task.Status.Message = "运行中"
	if task.Status.StartedAt == nil {
		task.Status.StartedAt = timePtr(now)
	}
	if task.Status.SegmentStartedAt == nil {
		task.Status.SegmentStartedAt = timePtr(now)
	}
	task.Status.Segments = append(task.Status.Segments, trainpilotv1alpha1.RunSegment{
		RunID:     task.Spec.RunID,
		Round:     task.Status.Round,
		StartedAt: timePtr(now),
		Workers:   task.Spec.Workers,
	})
	r.setCondition(task, ConditionWorkersReady, metav1.ConditionTrue, "AllRunning", "all workers are running")
	_ = pods
}

// openSegmentFromPods meters a round the operator never saw as Running. The
// segment is backdated to the earliest worker start so a short round still
// reports its metered time instead of zero.
func (r *TrainTaskReconciler) openSegmentFromPods(task *trainpilotv1alpha1.TrainTask, pods []corev1.Pod, now time.Time) {
	if task.Status.SegmentStartedAt != nil || len(task.Status.Segments) > 0 {
		return
	}
	start := now
	for i := range pods {
		if t := pods[i].Status.StartTime; t != nil && t.Time.Before(start) {
			start = t.Time
		}
	}
	if start.After(now) {
		start = now
	}
	if task.Status.StartedAt == nil {
		task.Status.StartedAt = timePtr(start)
	}
	task.Status.SegmentStartedAt = timePtr(start)
	task.Status.Segments = append(task.Status.Segments, trainpilotv1alpha1.RunSegment{
		RunID:     task.Spec.RunID,
		Round:     task.Status.Round,
		StartedAt: timePtr(start),
		Workers:   task.Spec.Workers,
	})
}

// updateStartingStep maps pod state onto the starting sub-steps.
func (r *TrainTaskReconciler) updateStartingStep(task *trainpilotv1alpha1.TrainTask, pods []corev1.Pod, now time.Time) {
	step := trainpilotv1alpha1.StepAllocatingResources
	for i := range pods {
		p := &pods[i]
		switch p.Status.Phase {
		case corev1.PodPending:
			for _, cs := range p.Status.ContainerStatuses {
				if cs.State.Waiting == nil {
					continue
				}
				reason := cs.State.Waiting.Reason
				switch {
				case reason == "ContainerCreating":
					step = laterStep(step, trainpilotv1alpha1.StepMountingStorage)
				case reason == "CreateContainerConfigError" || reason == "CreateContainerError":
					step = laterStep(step, trainpilotv1alpha1.StepMountingStorage)
				case strings.Contains(reason, "ImagePull") || reason == "ErrImagePull":
					step = laterStep(step, trainpilotv1alpha1.StepPullingImage)
				default:
					step = laterStep(step, trainpilotv1alpha1.StepStartingContainers)
				}
			}
			for _, cond := range p.Status.Conditions {
				if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse &&
					cond.Reason == corev1.PodReasonUnschedulable {
					step = trainpilotv1alpha1.StepAllocatingResources
				}
			}
		case corev1.PodRunning:
			step = laterStep(step, trainpilotv1alpha1.StepStartingContainers)
		}
	}
	task.Status.StartingStep = step
	task.Status.Message = fmt.Sprintf("启动中：%s", step)
	_ = now
}

func laterStep(cur, next string) string {
	order := map[string]int{
		trainpilotv1alpha1.StepAllocatingResources: 0,
		trainpilotv1alpha1.StepMountingStorage:     1,
		trainpilotv1alpha1.StepPullingImage:        2,
		trainpilotv1alpha1.StepStartingContainers:  3,
	}
	if order[next] > order[cur] {
		return next
	}
	return cur
}

// detectStartupFailure returns a reason when the round cannot start: image pull
// or config errors, or a startup that exceeds startupTimeout.
func (r *TrainTaskReconciler) detectStartupFailure(task *trainpilotv1alpha1.TrainTask,
	pods []corev1.Pod, now time.Time) string {
	for i := range pods {
		for _, cs := range pods[i].Status.ContainerStatuses {
			if cs.State.Waiting == nil {
				continue
			}
			switch cs.State.Waiting.Reason {
			case "ImagePullBackOff", "ErrImagePull", "InvalidImageName":
				task.Status.Message = "启动失败：" + cs.State.Waiting.Message
				return trainpilotv1alpha1.ReasonStartupCheckFailed
			case "CreateContainerConfigError":
				task.Status.Message = "启动失败：" + cs.State.Waiting.Message
				return trainpilotv1alpha1.ReasonStartupCheckFailed
			}
		}
	}
	// The startup clock starts at admission, not at enqueue: time spent waiting
	// for Kueue must not count as a failed startup.
	if task.Status.StartingSince != nil && now.Sub(task.Status.StartingSince.Time) > startupTimeout {
		return trainpilotv1alpha1.ReasonStartupCheckFailed
	}
	// Starting is timestamped by the earliest pod; fall back to segment start.
	for i := range pods {
		if started := pods[i].Status.StartTime; started != nil && now.Sub(started.Time) > startupTimeout {
			return trainpilotv1alpha1.ReasonStartupCheckFailed
		}
	}
	return ""
}

// detectWorkerFailure returns a reason when the round failed for infrastructure
// reasons the TrainJob condition does not describe (node flapping, eviction).
func (r *TrainTaskReconciler) detectWorkerFailure(ctx context.Context, task *trainpilotv1alpha1.TrainTask,
	pods []corev1.Pod) string {

	for i := range pods {
		p := &pods[i]
		if p.Spec.NodeName == "" {
			continue
		}
		var node corev1.Node
		if err := r.Get(ctx, types.NamespacedName{Name: p.Spec.NodeName}, &node); err != nil {
			if apierrors.IsNotFound(err) {
				r.excludeNode(task, p.Spec.NodeName)
				return trainpilotv1alpha1.ReasonNodeFailure
			}
			continue
		}
		if !nodeReady(&node) {
			r.excludeNode(task, p.Spec.NodeName)
			return trainpilotv1alpha1.ReasonNodeFailure
		}
		// A vanished pod on a healthy node means eviction or preemption.
		if p.DeletionTimestamp != nil {
			return trainpilotv1alpha1.ReasonPreempted
		}
	}
	return ""
}

// excludeNode records a bad node for the rest of the run, FIFO-evicting past the cap.
func (r *TrainTaskReconciler) excludeNode(task *trainpilotv1alpha1.TrainTask, node string) {
	if slices.Contains(task.Status.ExcludedNodes, node) {
		return
	}
	if len(task.Status.ExcludedNodes) >= maxExcludedNodes {
		task.Status.ExcludedNodes = task.Status.ExcludedNodes[1:]
	}
	task.Status.ExcludedNodes = append(task.Status.ExcludedNodes, node)
	r.event(task, corev1.EventTypeWarning, "NodeExcluded",
		fmt.Sprintf("node %s excluded for the rest of run %d", node, task.Spec.RunID))
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// refreshWorkers mirrors pod placement into status (运行记录).
func (r *TrainTaskReconciler) refreshWorkers(task *trainpilotv1alpha1.TrainTask, pods []corev1.Pod) {
	workers := make([]trainpilotv1alpha1.WorkerPod, 0, len(pods))
	for i := range pods {
		p := &pods[i]
		idx, _ := strconv.Atoi(p.Annotations["batch.kubernetes.io/job-completion-index"])
		workers = append(workers, trainpilotv1alpha1.WorkerPod{
			Name:        p.Name,
			WorkerIndex: int32(idx),
			Node:        p.Spec.NodeName,
			Phase:       string(p.Status.Phase),
		})
	}
	task.Status.Workers = workers
}

// accrueMetering updates the open segment's metered seconds.
func (r *TrainTaskReconciler) accrueMetering(task *trainpilotv1alpha1.TrainTask, now time.Time) {
	start := task.Status.SegmentStartedAt
	if start == nil {
		return
	}
	if len(task.Status.Segments) == 0 {
		return
	}
	seg := &task.Status.Segments[len(task.Status.Segments)-1]
	if seg.EndedAt != nil {
		return
	}
	seconds := max(int64(now.Sub(start.Time).Seconds()), 0)
	seg.MeteredSeconds = seconds
	r.recomputeMetered(task)
}

// closeSegment ends the open metered segment.
func (r *TrainTaskReconciler) closeSegment(task *trainpilotv1alpha1.TrainTask, now time.Time, reason string) {
	if task.Status.SegmentStartedAt == nil || len(task.Status.Segments) == 0 {
		return
	}
	seg := &task.Status.Segments[len(task.Status.Segments)-1]
	if seg.EndedAt != nil {
		return
	}
	seconds := max(int64(now.Sub(task.Status.SegmentStartedAt.Time).Seconds()), 0)
	seg.MeteredSeconds = seconds
	seg.EndedAt = timePtr(now)
	seg.EndReason = reason
	r.recomputeMetered(task)
	task.Status.SegmentStartedAt = nil
}

// recomputeMetered refreshes accumulated metered seconds from the segment history.
func (r *TrainTaskReconciler) recomputeMetered(task *trainpilotv1alpha1.TrainTask) {
	var seconds int64
	for i := range task.Status.Segments {
		seconds += task.Status.Segments[i].MeteredSeconds
	}
	task.Status.MeteredSeconds = seconds
}

// suspended reports whether the TrainJob still waits for admission. Kueue owns
// the field: it clears it on admission and sets it again on eviction.
func suspended(tj *trainerv1alpha1.TrainJob) bool {
	return tj.Spec.Suspend != nil && *tj.Spec.Suspend
}

// trainJobOutcome extracts a terminal outcome from TrainJob conditions.
func trainJobOutcome(tj *trainerv1alpha1.TrainJob) (bool, string) {
	for _, c := range tj.Status.Conditions {
		if c.Status != metav1.ConditionTrue {
			continue
		}
		switch c.Type {
		case trainerv1alpha1.TrainJobComplete:
			return true, trainpilotv1alpha1.ReasonSucceeded
		case trainerv1alpha1.TrainJobFailed:
			if c.Reason == trainerv1alpha1.TrainJobDeadlineExceededReason {
				return true, trainpilotv1alpha1.ReasonMaxRuntimeExceeded
			}
			return true, trainpilotv1alpha1.ReasonExitCode
		}
	}
	return false, ""
}

// applyAction consumes an idempotent continue/rerun request from the annotation.
// It returns true when state changed and a status write is needed.
func (r *TrainTaskReconciler) applyAction(task *trainpilotv1alpha1.TrainTask) bool {
	raw := task.Annotations[AnnotationAction]
	if raw == "" || raw == task.Annotations[AnnotationActionApplied] {
		return false
	}
	verb, id, _ := strings.Cut(raw, "=")
	switch verb {
	case "continue", "rerun":
		if task.Spec.DesiredState == trainpilotv1alpha1.DesiredStateStopped {
			task.Spec.DesiredState = trainpilotv1alpha1.DesiredStateRunning
		}
		task.Spec.Suspend = false
		if verb == "rerun" {
			// A new RUN_ID resets everything through beginRun.
			task.Spec.RunID++
		}
	}
	if task.Annotations == nil {
		task.Annotations = map[string]string{}
	}
	task.Annotations[AnnotationActionApplied] = raw
	_ = id
	return true
}

// applyStatusAction applies the status half of a continue request (继续运行):
// same RUN_ID, both budgets reset. Keyed on the action id so a
// repeated annotation is applied once.
func (r *TrainTaskReconciler) applyStatusAction(task *trainpilotv1alpha1.TrainTask) {
	raw := task.Annotations[AnnotationAction]
	if raw == "" || raw == task.Status.AppliedAction {
		return
	}
	if verb, _, _ := strings.Cut(raw, "="); verb != "continue" {
		return
	}
	task.Status.RestartCount = 0
	task.Status.PreemptionCount = 0
	task.Status.ExcludedNodes = nil
	task.Status.AppliedAction = raw
	if task.Status.Phase.IsTerminal() {
		task.Status.Phase = trainpilotv1alpha1.PhaseQueued
		task.Status.Reason = ""
		task.Status.Message = "排队中"
		task.Status.CompletionTime = nil
		task.Status.QueuedSince = nil
		task.Status.StartingSince = nil
	}
}

func (r *TrainTaskReconciler) setCondition(task *trainpilotv1alpha1.TrainTask, condType string,
	status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: task.Generation,
		LastTransitionTime: metav1.NewTime(r.now()),
	})
}

func (r *TrainTaskReconciler) event(task *trainpilotv1alpha1.TrainTask, eventType, reason, message string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(task, nil, eventType, reason, "", "%s", message)
}

func (r *TrainTaskReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func timePtr(t time.Time) *metav1.Time {
	mt := metav1.NewTime(t.UTC().Truncate(time.Second))
	return &mt
}

// equalStatus compares the fields this controller owns, ignoring LastTransitionTime
// churn caused by re-evaluation of unchanged conditions.
func equalStatus(a, b *trainpilotv1alpha1.TrainTaskStatus) bool {
	ac := append([]metav1.Condition{}, a.Conditions...)
	bc := append([]metav1.Condition{}, b.Conditions...)
	for i := range ac {
		ac[i].LastTransitionTime = metav1.Time{}
	}
	for i := range bc {
		bc[i].LastTransitionTime = metav1.Time{}
	}
	aa, _ := json.Marshal(struct {
		S    trainpilotv1alpha1.TrainTaskStatus
		Cond []metav1.Condition
	}{*a, ac})
	bb, _ := json.Marshal(struct {
		S    trainpilotv1alpha1.TrainTaskStatus
		Cond []metav1.Condition
	}{*b, bc})
	return string(aa) == string(bb)
}

// SetupWithManager sets up the controller with the Manager.
func (r *TrainTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("trainpilot")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&trainpilotv1alpha1.TrainTask{}).
		Owns(&trainerv1alpha1.TrainJob{}).
		Owns(&trainerv1alpha1.TrainingRuntime{}).
		Watches(&jobsetv1alpha2.JobSet{}, handler.EnqueueRequestsFromMapFunc(r.mapJobSetToTask)).
		Named("traintask").
		Complete(r)
}

// mapJobSetToTask maps a JobSet owned by a TrainJob back to its TrainTask.
func (r *TrainTaskReconciler) mapJobSetToTask(ctx context.Context, obj client.Object) []reconcile.Request {
	ns := obj.GetNamespace()
	name := obj.GetName()
	var tj trainerv1alpha1.TrainJob
	if err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &tj); err != nil {
		return nil
	}
	if taskName, ok := tj.Labels[taskLabel]; ok {
		return []reconcile.Request{{Namespace: ns, Name: taskName}}
	}
	return nil
}
