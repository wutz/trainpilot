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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

func TestTaskCollectorEmitsExpectedGauges(t *testing.T) {
	s := runtime.NewScheme()
	_ = trainpilotv1alpha1.AddToScheme(s)

	running := &trainpilotv1alpha1.TrainTask{
		Name: "run-1", Namespace: "ns-a",
		Spec: trainpilotv1alpha1.TrainTaskSpec{
			Workers:   2,
			Resources: trainpilotv1alpha1.WorkerResources{GPUsPerWorker: 8},
			RunID:     1,
		},
		Status: trainpilotv1alpha1.TrainTaskStatus{
			Phase:             trainpilotv1alpha1.PhaseRunning,
			RunID:             1,
			ExcludedNodes:     []string{"bad-1"},
			MeteredSeconds:    60,
			MeteredGPUSeconds: 960,
		},
	}
	failed := &trainpilotv1alpha1.TrainTask{
		Name: "fail-1", Namespace: "ns-a",
		Spec: trainpilotv1alpha1.TrainTaskSpec{Workers: 1, RunID: 1},
		Status: trainpilotv1alpha1.TrainTaskStatus{
			Phase:         trainpilotv1alpha1.PhaseFailed,
			RunID:         1,
			ExcludedNodes: []string{"bad-2"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(running, failed).Build()
	reg := prometheus.NewRegistry()
	reg.MustRegister(&TaskCollector{reader: c})

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			key := mf.GetName()
			for _, lp := range m.GetLabel() {
				key += "|" + lp.GetName() + "=" + lp.GetValue()
			}
			if m.GetGauge() != nil {
				got[key] = m.GetGauge().GetValue()
			} else if mf.GetType() == dto.MetricType_GAUGE {
				got[key] = m.GetGauge().GetValue()
			}
		}
	}

	if got["trainpilot_active_gpus"] != 16 {
		t.Fatalf("active_gpus = %v, want 16", got["trainpilot_active_gpus"])
	}
	if got["trainpilot_excluded_nodes"] != 1 {
		t.Fatalf("excluded_nodes = %v, want 1 (only non-terminal)", got["trainpilot_excluded_nodes"])
	}
	if got["trainpilot_tasks|phase=Running"] != 1 || got["trainpilot_tasks|phase=Failed"] != 1 {
		t.Fatalf("unexpected phase counts: %+v", got)
	}
	if got["trainpilot_task_metered_gpu_seconds|name=run-1|namespace=ns-a|run_id=1"] != 960 {
		t.Fatalf("metered gpu seconds = %v, want 960", got["trainpilot_task_metered_gpu_seconds|name=run-1|namespace=ns-a|run_id=1"])
	}
}
