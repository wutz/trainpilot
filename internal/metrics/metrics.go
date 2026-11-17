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

// Package metrics registers Prometheus metrics for TrainPilot.
package metrics

import (
	"context"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

var (
	// RoundTerminationsTotal counts ended rounds by end reason.
	RoundTerminationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trainpilot_round_terminations_total",
		Help: "Total number of ended training rounds partitioned by end reason.",
	}, []string{"reason"})

	// TaskTerminalTotal counts terminal transitions by phase and reason.
	TaskTerminalTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "trainpilot_task_terminal_total",
		Help: "Total number of TrainTasks entering a terminal phase.",
	}, []string{"phase", "reason"})

	registerOnce sync.Once
)

// Register registers counters and the live TrainTask collector on the
// controller-runtime Prometheus registry.
func Register(reader client.Reader) {
	registerOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(RoundTerminationsTotal, TaskTerminalTotal)
		if reader != nil {
			ctrlmetrics.Registry.MustRegister(&TaskCollector{reader: reader})
		}
	})
}

var allPhases = []trainpilotv1alpha1.TrainTaskPhase{
	trainpilotv1alpha1.PhaseQueued,
	trainpilotv1alpha1.PhaseStarting,
	trainpilotv1alpha1.PhaseRunning,
	trainpilotv1alpha1.PhaseRecovering,
	trainpilotv1alpha1.PhaseSucceeded,
	trainpilotv1alpha1.PhaseFailed,
	trainpilotv1alpha1.PhaseStopped,
	trainpilotv1alpha1.PhaseInterrupted,
}

// TaskCollector emits point-in-time gauges from the manager's informer cache.
type TaskCollector struct {
	reader client.Reader
}

var (
	tasksDesc = prometheus.NewDesc(
		"trainpilot_tasks",
		"Current number of TrainTasks by lifecycle phase.",
		[]string{"phase"}, nil,
	)
	activeGPUsDesc = prometheus.NewDesc(
		"trainpilot_active_gpus",
		"Total GPUs allocated to currently Running TrainTasks.",
		nil, nil,
	)
	excludedNodesDesc = prometheus.NewDesc(
		"trainpilot_excluded_nodes",
		"Number of nodes currently excluded by non-terminal TrainTasks.",
		nil, nil,
	)
	meteredSecondsDesc = prometheus.NewDesc(
		"trainpilot_task_metered_seconds",
		"Accumulated metered seconds for a TrainTask run.",
		[]string{"namespace", "name", "run_id"}, nil,
	)
	meteredGPUSecondsDesc = prometheus.NewDesc(
		"trainpilot_task_metered_gpu_seconds",
		"Accumulated metered GPU-seconds for a TrainTask run.",
		[]string{"namespace", "name", "run_id"}, nil,
	)
)

// Describe implements prometheus.Collector.
func (c *TaskCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- tasksDesc
	ch <- activeGPUsDesc
	ch <- excludedNodesDesc
	ch <- meteredSecondsDesc
	ch <- meteredGPUSecondsDesc
}

// Collect implements prometheus.Collector.
func (c *TaskCollector) Collect(ch chan<- prometheus.Metric) {
	var list trainpilotv1alpha1.TrainTaskList
	if err := c.reader.List(context.Background(), &list); err != nil {
		return
	}

	phaseCounts := make(map[trainpilotv1alpha1.TrainTaskPhase]float64, len(allPhases))
	var activeGPUs float64
	var excludedNodes float64

	for i := range list.Items {
		t := &list.Items[i]
		phase := t.Status.Phase
		if phase == "" {
			phase = trainpilotv1alpha1.PhaseQueued
		}
		phaseCounts[phase]++
		if phase == trainpilotv1alpha1.PhaseRunning {
			activeGPUs += float64(t.Spec.Workers * t.Spec.Resources.GPUsPerWorker)
		}
		if !phase.IsTerminal() {
			excludedNodes += float64(len(t.Status.ExcludedNodes))
		}
		runID := strconv.Itoa(int(max(t.Status.RunID, t.Spec.RunID)))
		ch <- prometheus.MustNewConstMetric(
			meteredSecondsDesc, prometheus.GaugeValue, float64(t.Status.MeteredSeconds),
			t.Namespace, t.Name, runID,
		)
		ch <- prometheus.MustNewConstMetric(
			meteredGPUSecondsDesc, prometheus.GaugeValue, float64(t.Status.MeteredGPUSeconds),
			t.Namespace, t.Name, runID,
		)
	}

	for _, p := range allPhases {
		ch <- prometheus.MustNewConstMetric(tasksDesc, prometheus.GaugeValue, phaseCounts[p], string(p))
	}
	ch <- prometheus.MustNewConstMetric(activeGPUsDesc, prometheus.GaugeValue, activeGPUs)
	ch <- prometheus.MustNewConstMetric(excludedNodesDesc, prometheus.GaugeValue, excludedNodes)
}
