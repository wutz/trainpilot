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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrl "sigs.k8s.io/controller-runtime/pkg/reconcile"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
	"github.com/wutz/trainpilot/internal/render"
)

// testScheme covers everything the controller reads and writes.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(trainpilotv1alpha1.AddToScheme(s))
	utilruntime.Must(trainerv1alpha1.AddToScheme(s))
	return s
}

// fixedNow keeps billing math deterministic.
var fixedNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func newReconciler(t *testing.T, objs ...client.Object) (*TrainTaskReconciler, client.Client) {
	t.Helper()
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&trainpilotv1alpha1.TrainTask{}).
		WithObjects(objs...).
		Build()
	return &TrainTaskReconciler{
		Client:            c,
		Scheme:            s,
		GPULabel:          "scclabs.dev/gpu",
		GPUTolerationKeys: []string{"nvidia.com/gpu"},
		SharedMemoryRatio: 0.5,
		Now:               func() time.Time { return fixedNow },
	}, c
}

func reconcileOnce(t *testing.T, r *TrainTaskReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "trainpilot", Name: name},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func getTask(t *testing.T, c client.Client, name string) *trainpilotv1alpha1.TrainTask {
	t.Helper()
	var task trainpilotv1alpha1.TrainTask
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "trainpilot", Name: name}, &task); err != nil {
		t.Fatalf("get task: %v", err)
	}
	return &task
}

// task builds a TrainTask with a run already in progress at round 2.
func task(name string, workers, gpus int32) *trainpilotv1alpha1.TrainTask {
	return &trainpilotv1alpha1.TrainTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "trainpilot",
			UID:               types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(fixedNow.Add(-time.Hour)),
		},
		Spec: trainpilotv1alpha1.TrainTaskSpec{
			Image:                "busybox:latest",
			Command:              []string{"echo hello"},
			Workers:              workers,
			Resources:            trainpilotv1alpha1.WorkerResources{GPUsPerWorker: gpus, CPU: "4", Memory: "8Gi"},
			PricePerWorkerHour:   "10.00",
			FaultTolerance:       trainpilotv1alpha1.FaultTolerance{AutoRestart: true, MaxRestarts: 3},
			DesiredState:         trainpilotv1alpha1.DesiredStateRunning,
			RunID:                1,
		},
		Status: trainpilotv1alpha1.TrainTaskStatus{
			RunID:   1,
			Round:   2,
			Phase:   trainpilotv1alpha1.PhaseRunning,
			Workers: []trainpilotv1alpha1.WorkerPod{{Name: name + "-node-0-0", Node: "gn-a"}},
		},
	}
}

func gpuNode(name string, gpus int64) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				render.GPUKey: *resource.NewQuantity(gpus, resource.DecimalSI),
			},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}
