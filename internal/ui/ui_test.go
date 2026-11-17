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

package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

func TestHealthzAndIndex(t *testing.T) {
	s := runtime.NewScheme()
	_ = trainpilotv1alpha1.AddToScheme(s)
	h := Handler(fake.NewClientBuilder().WithScheme(s).Build())

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("/healthz unexpected response: %d %q", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "TrainPilot") {
		t.Fatalf("/ unexpected response: %d", rec.Code)
	}
}

func TestAPITasks(t *testing.T) {
	s := runtime.NewScheme()
	_ = trainpilotv1alpha1.AddToScheme(s)

	t1 := &trainpilotv1alpha1.TrainTask{
		Name:              "older-job",
		Namespace:         "ns-a",
		CreationTimestamp: metav1.NewTime(time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)),
		Spec: trainpilotv1alpha1.TrainTaskSpec{
			Image:     "pytorch:2.5",
			Command:   []string{"python train.py"},
			Workers:   2,
			QueueName: "gpu",
			Resources: trainpilotv1alpha1.WorkerResources{GPUsPerWorker: 8},
			RunID:     1,
		},
		Status: trainpilotv1alpha1.TrainTaskStatus{
			Phase:             trainpilotv1alpha1.PhaseRunning,
			Round:             1,
			MeteredSeconds:    120,
			MeteredGPUSeconds: 1920,
			Workers: []trainpilotv1alpha1.WorkerPod{
				{Name: "p0", Node: "n0", Phase: "Running", Ready: true},
				{Name: "p1", Node: "n1", Phase: "Running", Ready: false},
			},
		},
	}
	t2 := &trainpilotv1alpha1.TrainTask{
		Name:              "newer-job",
		Namespace:         "ns-b",
		CreationTimestamp: metav1.NewTime(time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC)),
		Spec: trainpilotv1alpha1.TrainTaskSpec{
			Image:   "busybox:1.36",
			Command: []string{"echo hi"},
			Workers: 1,
			RunID:   1,
		},
		Status: trainpilotv1alpha1.TrainTaskStatus{
			Phase:         trainpilotv1alpha1.PhaseFailed,
			Reason:        trainpilotv1alpha1.ReasonExitCode,
			ExcludedNodes: []string{"bad-node"},
		},
	}

	c := fake.NewClientBuilder().WithScheme(s).WithObjects(t1, t2).Build()
	h := Handler(c)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/tasks status = %d", rec.Code)
	}

	var resp struct {
		GeneratedAt string     `json:"generatedAt"`
		Tasks       []taskView `json:"tasks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(resp.Tasks))
	}
	// Sorted newest first.
	if resp.Tasks[0].Name != "newer-job" || resp.Tasks[0].Reason != trainpilotv1alpha1.ReasonExitCode {
		t.Fatalf("first task mismatch: %+v", resp.Tasks[0])
	}
	if resp.Tasks[1].Name != "older-job" ||
		resp.Tasks[1].ReadyWorkers != 1 ||
		resp.Tasks[1].MeteredGPUSeconds != 1920 ||
		resp.Tasks[1].QueueName != "gpu" {
		t.Fatalf("second task mismatch: %+v", resp.Tasks[1])
	}
}
