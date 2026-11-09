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
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

const testMount = "/data"

func mkTask() *trainpilotv1alpha1.TrainTask {
	return &trainpilotv1alpha1.TrainTask{
		Name: "t", Namespace: "n",
		Spec: trainpilotv1alpha1.TrainTaskSpec{
			Image:   "busybox:1.36",
			Command: []string{"echo hi"},
			Workers: 1,
			RunID:   1,
		},
	}
}

func TestCreateAcceptsMinimalTask(t *testing.T) {
	w := &TrainTaskWebhook{}
	if _, err := w.ValidateCreate(context.Background(), mkTask()); err != nil {
		t.Fatalf("minimal task rejected: %v", err)
	}
}

func TestCreateRequiresImageAndCommand(t *testing.T) {
	w := &TrainTaskWebhook{}
	task := mkTask()
	task.Spec.Image = ""
	task.Spec.Command = []string{"   "}
	_, err := w.ValidateCreate(context.Background(), task)
	if err == nil {
		t.Fatal("empty image and command must be rejected")
	}
	for _, want := range []string{"spec.image", "spec.command"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error should mention %s: %v", want, err)
		}
	}
}

func TestCreateRejectsReservedEnv(t *testing.T) {
	w := &TrainTaskWebhook{}
	for _, name := range []string{"RUN_ID", "MASTER_ADDR", "PET_NNODES"} {
		task := mkTask()
		task.Spec.Env = []corev1.EnvVar{{Name: name, Value: "x"}}
		if _, err := w.ValidateCreate(context.Background(), task); err == nil {
			t.Fatalf("env %s must be reserved", name)
		}
	}
	// A normal name is accepted.
	task := mkTask()
	task.Spec.Env = []corev1.EnvVar{{Name: "MY_LEARNING_RATE", Value: "0.1"}}
	if _, err := w.ValidateCreate(context.Background(), task); err != nil {
		t.Fatalf("user env rejected: %v", err)
	}
}

func TestCreateRejectsBadEnvNames(t *testing.T) {
	w := &TrainTaskWebhook{}
	for _, name := range []string{"1ABC", "A-B", "A B"} {
		task := mkTask()
		task.Spec.Env = []corev1.EnvVar{{Name: name, Value: "x"}}
		if _, err := w.ValidateCreate(context.Background(), task); err == nil {
			t.Fatalf("env name %q must be rejected", name)
		}
	}
	task := mkTask()
	task.Spec.Env = []corev1.EnvVar{{Name: "A", Value: "1"}, {Name: "A", Value: "2"}}
	if _, err := w.ValidateCreate(context.Background(), task); err == nil {
		t.Fatal("duplicate env names must be rejected")
	}
}

func TestCreateRejectsMountPathProblems(t *testing.T) {
	w := &TrainTaskWebhook{}
	cases := map[string]string{
		"relative":   "data",
		"root":       "/",
		"dotdot":     "/data/../etc",
		"whitespace": "/my data",
	}
	for name, p := range cases {
		task := mkTask()
		task.Spec.SharedStorage = []trainpilotv1alpha1.StorageMount{{ClaimName: "pvc", MountPath: p}}
		if _, err := w.ValidateCreate(context.Background(), task); err == nil {
			t.Fatalf("%s mount path %q must be rejected", name, p)
		}
	}
}

func TestCreateRejectsOverlappingMounts(t *testing.T) {
	w := &TrainTaskWebhook{}
	task := mkTask()
	task.Spec.SharedStorage = []trainpilotv1alpha1.StorageMount{
		{ClaimName: "a", MountPath: testMount},
		{ClaimName: "b", MountPath: "/data/nested"},
	}
	if _, err := w.ValidateCreate(context.Background(), task); err == nil {
		t.Fatal("nested mounts must be rejected")
	}

	// Public data must not overlap a shared mount either.
	task = mkTask()
	task.Spec.SharedStorage = []trainpilotv1alpha1.StorageMount{{ClaimName: "a", MountPath: testMount}}
	task.Spec.PublicData = &trainpilotv1alpha1.StorageMount{ClaimName: "p", MountPath: "/data/public"}
	if _, err := w.ValidateCreate(context.Background(), task); err == nil {
		t.Fatal("public data overlapping shared storage must be rejected")
	}

	// Sibling paths are fine.
	task = mkTask()
	task.Spec.SharedStorage = []trainpilotv1alpha1.StorageMount{
		{ClaimName: "a", MountPath: testMount},
		{ClaimName: "b", MountPath: "/data2"},
	}
	if _, err := w.ValidateCreate(context.Background(), task); err != nil {
		t.Fatalf("sibling mounts rejected: %v", err)
	}
}

func TestUpdateFreezesImageAndCommandWithinARun(t *testing.T) {
	w := &TrainTaskWebhook{}
	oldTask := mkTask()

	newTask := mkTask()
	newTask.Spec.Image = "busybox:1.37"
	if _, err := w.ValidateUpdate(context.Background(), oldTask, newTask); err == nil {
		t.Fatal("image change within the same runID must be rejected")
	}

	newTask = mkTask()
	newTask.Spec.Command = []string{"echo bye"}
	if _, err := w.ValidateUpdate(context.Background(), oldTask, newTask); err == nil {
		t.Fatal("command change within the same runID must be rejected")
	}

	// Allowed: re-run with a new image (runID incremented).
	newTask = mkTask()
	newTask.Spec.RunID = 2
	newTask.Spec.Image = "busybox:1.37"
	newTask.Spec.Command = []string{"echo bye"}
	if _, err := w.ValidateUpdate(context.Background(), oldTask, newTask); err != nil {
		t.Fatalf("re-run with new image rejected: %v", err)
	}
}

func TestUpdateRejectsRunIDDecrease(t *testing.T) {
	w := &TrainTaskWebhook{}
	oldTask := mkTask()
	oldTask.Spec.RunID = 3
	newTask := mkTask()
	newTask.Spec.RunID = 2
	if _, err := w.ValidateUpdate(context.Background(), oldTask, newTask); err == nil {
		t.Fatal("runID must not decrease")
	}
}

func TestUpdateAllowsStopAndContinue(t *testing.T) {
	w := &TrainTaskWebhook{}
	oldTask := mkTask()
	newTask := mkTask()
	newTask.Spec.DesiredState = trainpilotv1alpha1.DesiredStateStopped
	if _, err := w.ValidateUpdate(context.Background(), oldTask, newTask); err != nil {
		t.Fatalf("stop rejected: %v", err)
	}
}
