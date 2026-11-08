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
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
	"github.com/wutz/trainpilot/internal/render"
)

// A round whose TrainJob was started by the operator and then removed by the
// platform is a preemption, not a fresh start.
func TestVanishedTrainJobCountsAsPreemption(t *testing.T) {
	tk := task("preempt-me", 1, 0)
	tk.Status.StartedRound = 2
	r, c := newReconciler(t, tk)

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if got.Status.PreemptionCount != 1 {
		t.Fatalf("preemptionCount = %d, want 1", got.Status.PreemptionCount)
	}
	if got.Status.RestartCount != 0 {
		t.Fatalf("restartCount = %d, want 0: preemption must not spend the restart budget", got.Status.RestartCount)
	}
	if got.Status.Phase != trainpilotv1alpha1.PhaseRecovering {
		t.Fatalf("phase = %q, want Recovering", got.Status.Phase)
	}
	if got.Status.Round != 3 {
		t.Fatalf("round = %d, want 3", got.Status.Round)
	}
}

// Preemption budget exhaustion interrupts the task instead of failing it.
func TestPreemptionsExhaustedInterrupts(t *testing.T) {
	tk := task("preempt-cap", 1, 0)
	tk.Status.StartedRound = 2
	tk.Status.PreemptionCount = 40
	r, c := newReconciler(t, tk)

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if got.Status.Phase != trainpilotv1alpha1.PhaseInterrupted {
		t.Fatalf("phase = %q, want Interrupted", got.Status.Phase)
	}
	if got.Status.Reason != trainpilotv1alpha1.ReasonPreemptionsExhausted {
		t.Fatalf("reason = %q", got.Status.Reason)
	}
}

// Restart budget exhaustion fails the task.
func TestRestartsExhaustedFails(t *testing.T) {
	tk := task("crash-loop", 1, 0)
	tk.Spec.FaultTolerance.MaxRestarts = 1
	tk.Status.StartedRound = 2
	tk.Status.RestartCount = 1
	tj := &trainerv1alpha1.TrainJob{
		ObjectMeta: metav1.ObjectMeta{Name: render.Names(tk.Name, render.Round{RunID: 1, Round: 2}), Namespace: tk.Namespace},
		Status: trainerv1alpha1.TrainJobStatus{
			Conditions: []metav1.Condition{{Type: trainerv1alpha1.TrainJobFailed, Status: metav1.ConditionTrue}},
		},
	}
	r, c := newReconciler(t, tk, tj)

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if got.Status.Phase != trainpilotv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if got.Status.Reason != trainpilotv1alpha1.ReasonRestartsExhausted {
		t.Fatalf("reason = %q", got.Status.Reason)
	}
	if got.Status.RestartCount != 2 {
		t.Fatalf("restartCount = %d, want 2 (the failed round is charged)", got.Status.RestartCount)
	}
}

// A completed TrainJob succeeds the task without spending any budget.
func TestCompletedTrainJobSucceeds(t *testing.T) {
	tk := task("done", 1, 0)
	tk.Status.StartedRound = 2
	tj := &trainerv1alpha1.TrainJob{
		ObjectMeta: metav1.ObjectMeta{Name: render.Names(tk.Name, render.Round{RunID: 1, Round: 2}), Namespace: tk.Namespace},
		Status: trainerv1alpha1.TrainJobStatus{
			Conditions: []metav1.Condition{{Type: trainerv1alpha1.TrainJobComplete, Status: metav1.ConditionTrue}},
		},
	}
	r, c := newReconciler(t, tk, tj)

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if got.Status.Phase != trainpilotv1alpha1.PhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", got.Status.Phase)
	}
	if got.Status.RestartCount != 0 || got.Status.PreemptionCount != 0 {
		t.Fatal("a successful round must not spend restart or preemption budget")
	}
}

// A round that starts and finishes between two polls is still Starting when the
// TrainJob completes. It must reach Succeeded rather than requeue forever, and
// the billed segment must be backdated to the workers' start.
func TestCompletedTrainJobSucceedsFromStarting(t *testing.T) {
	tk := task("fast", 2, 0)
	tk.Status.StartedRound = 2
	tk.Status.Phase = trainpilotv1alpha1.PhaseStarting
	tk.Status.Workers = nil
	started := metav1.NewTime(fixedNow.Add(-90 * time.Second))
	tk.Status.QueuedSince = &started
	round := render.Round{RunID: 1, Round: 2}

	tj := &trainerv1alpha1.TrainJob{
		ObjectMeta: metav1.ObjectMeta{Name: render.Names(tk.Name, round), Namespace: tk.Namespace},
		Status: trainerv1alpha1.TrainJobStatus{
			Conditions: []metav1.Condition{{Type: trainerv1alpha1.TrainJobComplete, Status: metav1.ConditionTrue}},
		},
	}
	pod := workerPod(tk.Name, 2, "gn-a")
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.StartTime = &started
	r, c := newReconciler(t, tk, tj, pod)

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if got.Status.Phase != trainpilotv1alpha1.PhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", got.Status.Phase)
	}
	if got.Status.BilledSeconds != 90 {
		t.Fatalf("billedSeconds = %d, want 90 (backdated to pod start)", got.Status.BilledSeconds)
	}
	if got.Status.RestartCount != 0 {
		t.Fatal("a successful round must not spend restart budget")
	}
}

// Billing is measured on segments; exceeding max runtime ends the run.
func TestMaxRuntimeIsEnforcedOnBilledTime(t *testing.T) {
	tk := task("long-run", 2, 0)
	tk.Spec.FaultTolerance.MaxRuntimeSeconds = 1800
	tk.Status.StartedRound = 2
	tk.Status.SegmentStartedAt = timePtr(fixedNow.Add(-time.Hour))
	tk.Status.Segments = []trainpilotv1alpha1.RunSegment{{
		RunID: 1, Round: 2, StartedAt: timePtr(fixedNow.Add(-time.Hour)),
		Workers: 2, PricePerWorkerHour: "10.00",
	}}
	tj := &trainerv1alpha1.TrainJob{
		ObjectMeta: metav1.ObjectMeta{Name: render.Names(tk.Name, render.Round{RunID: 1, Round: 2}), Namespace: tk.Namespace},
	}
	r, c := newReconciler(t, tk, tj)

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if got.Status.Phase != trainpilotv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if got.Status.Reason != trainpilotv1alpha1.ReasonMaxRuntimeExceeded {
		t.Fatalf("reason = %q", got.Status.Reason)
	}
	// 1h × 2 workers × ¥10/h.
	if got.Status.BilledSeconds < 3600 {
		t.Fatalf("billedSeconds = %d, want >= 3600", got.Status.BilledSeconds)
	}
	if got.Status.AccumulatedCost.String() != "20.00" {
		t.Fatalf("cost = %s, want 20.00", got.Status.AccumulatedCost)
	}
	if got.Status.Segments[0].EndedAt == nil {
		t.Fatal("the open segment must be closed")
	}
}

// Queue timeout interrupts a task that never got resources, and must not leave
// a billed segment open.
func TestQueueTimeoutInterrupts(t *testing.T) {
	tk := task("no-capacity", 1, 8)
	tk.Spec.FaultTolerance.QueueTimeoutSeconds = 3600
	tk.Status.Phase = trainpilotv1alpha1.PhaseQueued
	tk.Status.StartedRound = 0
	tk.Status.QueuedSince = timePtr(fixedNow.Add(-2 * time.Hour))
	tk.Status.SegmentStartedAt = timePtr(fixedNow.Add(-2 * time.Hour))
	tk.Status.Segments = []trainpilotv1alpha1.RunSegment{{
		RunID: 1, Round: 1, StartedAt: timePtr(fixedNow.Add(-2 * time.Hour)),
		Workers: 1, PricePerWorkerHour: "10.00",
	}}
	r, c := newReconciler(t, tk, gpuNode("gn-a", 8))

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if got.Status.Phase != trainpilotv1alpha1.PhaseInterrupted {
		t.Fatalf("phase = %q, want Interrupted", got.Status.Phase)
	}
	if got.Status.Reason != trainpilotv1alpha1.ReasonQueueTimeout {
		t.Fatalf("reason = %q", got.Status.Reason)
	}
	if got.Status.SegmentStartedAt != nil {
		t.Fatal("queue timeout must close the open billing segment")
	}
	if got.Status.Segments[0].EndedAt == nil {
		t.Fatal("the segment must be closed")
	}
}

// An eight-GPU worker needs whole nodes; the queue admits FIFO, so the older
// task takes the only node and the newer one waits.
func TestAdmissionIsFIFO(t *testing.T) {
	older := task("older", 1, 8)
	older.CreationTimestamp = metav1.NewTime(fixedNow.Add(-2 * time.Hour))
	older.Status = trainpilotv1alpha1.TrainTaskStatus{RunID: 1, Round: 1, Phase: trainpilotv1alpha1.PhaseQueued}
	newer := task("newer", 1, 8)
	newer.CreationTimestamp = metav1.NewTime(fixedNow.Add(-time.Hour))
	newer.Status = trainpilotv1alpha1.TrainTaskStatus{RunID: 1, Round: 1, Phase: trainpilotv1alpha1.PhaseQueued}

	r, c := newReconciler(t, older, newer, gpuNode("gn-a", 8))

	reconcileOnce(t, r, newer.Name)
	if got := getTask(t, c, newer.Name); got.Status.Phase != trainpilotv1alpha1.PhaseQueued {
		t.Fatalf("newer phase = %q, want Queued: the older task owns the node", got.Status.Phase)
	}

	reconcileOnce(t, r, older.Name)
	if got := getTask(t, c, older.Name); got.Status.Phase != trainpilotv1alpha1.PhaseStarting {
		t.Fatalf("older phase = %q, want Starting", got.Status.Phase)
	}
	if got := getTask(t, c, older.Name); got.Status.StartedRound != 1 {
		t.Fatalf("startedRound = %d, want 1", got.Status.StartedRound)
	}
}

// A running round whose node disappeared excludes that node for the run.
func TestNodeFailureExcludesNode(t *testing.T) {
	tk := task("node-gone", 1, 0)
	tk.Status.StartedRound = 2
	pod := workerPod(tk.Name, 2, "gn-gone")
	tj := &trainerv1alpha1.TrainJob{
		ObjectMeta: metav1.ObjectMeta{Name: render.Names(tk.Name, render.Round{RunID: 1, Round: 2}), Namespace: tk.Namespace},
	}
	r, c := newReconciler(t, tk, pod, tj)

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if len(got.Status.ExcludedNodes) != 1 || got.Status.ExcludedNodes[0] != "gn-gone" {
		t.Fatalf("excludedNodes = %v, want [gn-gone]", got.Status.ExcludedNodes)
	}
	if got.Status.RestartCount != 1 {
		t.Fatalf("restartCount = %d, want 1", got.Status.RestartCount)
	}
	if got.Status.Phase != trainpilotv1alpha1.PhaseRecovering {
		t.Fatalf("phase = %q, want Recovering", got.Status.Phase)
	}
}

// Node exclusion is capped so a run cannot grow status without bound.
func TestExcludedNodesAreCapped(t *testing.T) {
	tk := task("many-nodes", 1, 0)
	tk.Status.ExcludedNodes = []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"}
	r, _ := newReconciler(t, tk)
	r.excludeNode(tk, "n9")
	if len(tk.Status.ExcludedNodes) != maxExcludedNodes {
		t.Fatalf("len = %d, want %d", len(tk.Status.ExcludedNodes), maxExcludedNodes)
	}
	if tk.Status.ExcludedNodes[0] != "n2" || tk.Status.ExcludedNodes[7] != "n9" {
		t.Fatalf("FIFO eviction wrong: %v", tk.Status.ExcludedNodes)
	}
	r.excludeNode(tk, "n9")
	if len(tk.Status.ExcludedNodes) != maxExcludedNodes {
		t.Fatal("a repeated node must not be appended twice")
	}
}

// Stopping releases the round and closes an open segment.
func TestStopReleasesWorkers(t *testing.T) {
	tk := task("stop-me", 1, 0)
	tk.Spec.DesiredState = trainpilotv1alpha1.DesiredStateStopped
	tk.Status.StartedRound = 2
	tk.Status.SegmentStartedAt = timePtr(fixedNow.Add(-30 * time.Minute))
	tk.Status.Segments = []trainpilotv1alpha1.RunSegment{{
		RunID: 1, Round: 2, StartedAt: timePtr(fixedNow.Add(-30 * time.Minute)),
		Workers: 1, PricePerWorkerHour: "10.00",
	}}
	tj := &trainerv1alpha1.TrainJob{
		ObjectMeta: metav1.ObjectMeta{Name: render.Names(tk.Name, render.Round{RunID: 1, Round: 2}), Namespace: tk.Namespace},
	}
	r, c := newReconciler(t, tk, tj)

	reconcileOnce(t, r, tk.Name)

	got := getTask(t, c, tk.Name)
	if got.Status.Phase != trainpilotv1alpha1.PhaseStopped {
		t.Fatalf("phase = %q, want Stopped", got.Status.Phase)
	}
	if got.Status.SegmentStartedAt != nil {
		t.Fatal("stop must close the billing segment")
	}
	if got.Status.BilledSeconds != 1800 {
		t.Fatalf("billedSeconds = %d, want 1800", got.Status.BilledSeconds)
	}
}

// A rerun bumps runID and wipes per-run state; the webhook allows new image
// and command because the runID changed.
func TestRerunResetsRunState(t *testing.T) {
	tk := task("rerun-me", 1, 0)
	tk.Annotations = map[string]string{
		AnnotationAction:        "rerun=abc",
		AnnotationActionApplied: "rerun=old",
	}
	tk.Status.RestartCount = 2
	tk.Status.ExcludedNodes = []string{"gn-a"}
	tk.Status.BilledSeconds = 100
	tk.Status.AccumulatedCost = trainpilotv1alpha1.MoneyFromFloat(5)
	tk.Status.Phase = trainpilotv1alpha1.PhaseFailed
	r, _ := newReconciler(t, tk)

	// The first pass persists the action (new runID); the second begins the run.
	reconcileOnce(t, r, tk.Name)
	reconcileOnce(t, r, tk.Name)

	got := getTask(t, r.Client, tk.Name)
	if got.Spec.RunID != 2 {
		t.Fatalf("runID = %d, want 2", got.Spec.RunID)
	}
	if got.Annotations[AnnotationActionApplied] != "rerun=abc" {
		t.Fatalf("action not marked applied: %q", got.Annotations[AnnotationActionApplied])
	}
	if got.Status.RunID != 2 || got.Status.RestartCount != 0 || len(got.Status.ExcludedNodes) != 0 ||
		got.Status.BilledSeconds != 0 || got.Status.AccumulatedCost != trainpilotv1alpha1.ZeroMoney {
		t.Fatalf("run state not reset: %+v", got.Status)
	}
}

// Continue keeps runID, resets both budgets and un-terminates the task.
func TestContinueKeepsRunAndResetsBudgets(t *testing.T) {
	tk := task("continue-me", 1, 0)
	tk.Annotations = map[string]string{AnnotationAction: "continue=xyz"}
	tk.Status.RestartCount = 3
	tk.Status.PreemptionCount = 7
	tk.Status.Phase = trainpilotv1alpha1.PhaseFailed
	tk.Status.CompletionTime = timePtr(fixedNow.Add(-time.Minute))
	r, _ := newReconciler(t, tk)

	// Persisting the action and applying its status half are separate passes.
	reconcileOnce(t, r, tk.Name)
	reconcileOnce(t, r, tk.Name)

	got := getTask(t, r.Client, tk.Name)
	if got.Spec.RunID != 1 {
		t.Fatalf("runID = %d: continue must keep the run", got.Spec.RunID)
	}
	if got.Status.RestartCount != 0 || got.Status.PreemptionCount != 0 {
		t.Fatalf("budgets not reset: %d / %d", got.Status.RestartCount, got.Status.PreemptionCount)
	}
	if got.Status.Phase == trainpilotv1alpha1.PhaseFailed {
		t.Fatal("continue must un-terminate the task")
	}
}

func workerPod(taskName string, round int32, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      taskName + "-node-0-0",
			Namespace: "trainpilot",
			Labels: map[string]string{
				"trainpilot.wutz.dev/task": taskName,
				roundLabel:                 strconv.Itoa(int(round)),
				render.TrainerNodeLabel:    "trainer",
			},
			Annotations: map[string]string{"batch.kubernetes.io/job-completion-index": "0"},
		},
		Spec:   corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}
