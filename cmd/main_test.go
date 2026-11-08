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

package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	trainerv1alpha1 "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

// The controller registers watches on TrainTask, TrainJob, TrainingRuntime and
// JobSet. A type missing from the manager scheme fails at runtime, after the
// manager has started, so guard it here instead.
func TestManagerSchemeCoversWatchedTypes(t *testing.T) {
	for _, obj := range []runtime.Object{
		&corev1.Node{},
		&trainpilotv1alpha1.TrainTask{},
		&trainerv1alpha1.TrainJob{},
		&trainerv1alpha1.TrainingRuntime{},
		&jobsetv1alpha2.JobSet{},
	} {
		gvks, _, err := scheme.ObjectKinds(obj)
		if err != nil {
			t.Errorf("%T is not registered in the manager scheme: %v", obj, err)
			continue
		}
		if len(gvks) == 0 {
			t.Errorf("%T resolved to no GVK", obj)
		}
	}
}
