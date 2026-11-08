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

// Package v1alpha1 implements admission for TrainTask.
//
// The webhook enforces the create-form rules from PRD 7.3.4 and 7.3.3:
// environment variable names, platform-reserved names, mount path shape and
// overlap, and the "keep image/command on continue, change them with re-run"
// rule from PRD 7.5.1.
package v1alpha1

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	corev1 "k8s.io/api/core/v1"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

// envNamePattern is PRD 7.3.3: [A-Za-z_][A-Za-z0-9_]*, at most 128 characters.
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// reservedEnv are injected by the platform and must not be overridden (PRD 7.3.1).
var reservedEnv = map[string]bool{
	"TASK_ID": true, "RUN_ID": true, "RESTART_COUNT": true,
	"NNODES": true, "NODE_RANK": true, "MASTER_ADDR": true, "MASTER_PORT": true,
}

// TrainTaskWebhook validates TrainTask objects.
type TrainTaskWebhook struct{}

var _ admission.Validator[*trainpilotv1alpha1.TrainTask] = &TrainTaskWebhook{}

var traintasklog = logf.Log.WithName("traintask-webhook")

// SetupTrainTaskWebhookWithManager registers the webhook with the manager.
func SetupTrainTaskWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &trainpilotv1alpha1.TrainTask{}).
		WithValidator(&TrainTaskWebhook{}).
		Complete()
}

// ValidateCreate implements webhook.CustomValidator.
func (w *TrainTaskWebhook) ValidateCreate(_ context.Context, task *trainpilotv1alpha1.TrainTask) (admission.Warnings, error) {
	traintasklog.V(1).Info("validating create", "name", task.Name)
	return nil, w.validate(task)
}

// ValidateUpdate implements webhook.CustomValidator.
//
// Re-run (spec.runID increments) may change the image and command; an automatic
// restart or a continue (same runID) may not (PRD 7.5.1).
func (w *TrainTaskWebhook) ValidateUpdate(_ context.Context,
	oldTask, newTask *trainpilotv1alpha1.TrainTask) (admission.Warnings, error) {
	if err := w.validate(newTask); err != nil {
		return nil, err
	}

	sameRun := newTask.Spec.RunID == oldTask.Spec.RunID
	if sameRun {
		var errs field.ErrorList
		if oldTask.Spec.Image != newTask.Spec.Image {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "image"),
				"image can only change with a re-run (increment spec.runID)"))
		}
		if strings.Join(oldTask.Spec.Command, "\n") != strings.Join(newTask.Spec.Command, "\n") {
			errs = append(errs, field.Forbidden(field.NewPath("spec", "command"),
				"command can only change with a re-run (increment spec.runID)"))
		}
		if newTask.Spec.RunID < oldTask.Spec.RunID {
			errs = append(errs, field.Invalid(field.NewPath("spec", "runID"), newTask.Spec.RunID,
				"runID cannot decrease"))
		}
		if len(errs) > 0 {
			return nil, apierrors.NewInvalid(schema.GroupKind{Group: trainpilotv1alpha1.GroupVersion.Group,
				Kind: "TrainTask"}, newTask.Name, errs)
		}
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator.
func (w *TrainTaskWebhook) ValidateDelete(_ context.Context,
	_ *trainpilotv1alpha1.TrainTask) (admission.Warnings, error) {
	return nil, nil
}

func (w *TrainTaskWebhook) validate(task *trainpilotv1alpha1.TrainTask) error {
	var errs field.ErrorList
	spec := &task.Spec

	if spec.Image == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "image"), "image is required"))
	}
	if len(spec.Command) == 0 || strings.TrimSpace(strings.Join(spec.Command, "")) == "" {
		errs = append(errs, field.Required(field.NewPath("spec", "command"), "startup command is required"))
	}

	errs = append(errs, validateEnv(spec.Env)...)
	errs = append(errs, validateMounts(spec.SharedStorage, nil, field.NewPath("spec", "sharedStorage"))...)
	if spec.PublicData != nil {
		errs = append(errs, validateMounts([]trainpilotv1alpha1.StorageMount{*spec.PublicData}, nil,
			field.NewPath("spec", "publicData"))...)
		errs = append(errs, validateMounts(nil, spec.SharedStorage, field.NewPath("spec", "publicData"))...)
	}

	if spec.Resources.GPUsPerWorker > 0 && spec.GPUType == "" {
		// Not an error: the operator falls back to any GPU node, but warn-clean
		// deployments should pin a type for predictable pricing.
		traintasklog.V(1).Info("gpu task without gpuType", "name", task.Name)
	}

	if len(errs) > 0 {
		return apierrors.NewInvalid(schema.GroupKind{
			Group: trainpilotv1alpha1.GroupVersion.Group,
			Kind:  "TrainTask",
		}, task.Name, errs)
	}
	return nil
}

func validateEnv(env []corev1.EnvVar) field.ErrorList {
	var errs field.ErrorList
	seen := map[string]bool{}
	path := field.NewPath("spec", "env")
	for i, e := range env {
		idx := path.Index(i).Child("name")
		if len(e.Name) > 128 {
			errs = append(errs, field.TooLong(idx, e.Name, 128))
		} else if !envNamePattern.MatchString(e.Name) {
			errs = append(errs, field.Invalid(idx, e.Name,
				"must match [A-Za-z_][A-Za-z0-9_]*"))
		}
		if reservedEnv[e.Name] || strings.HasPrefix(e.Name, "PET_") {
			errs = append(errs, field.Forbidden(idx,
				"name is reserved by the platform and injected automatically"))
		}
		if seen[e.Name] {
			errs = append(errs, field.Duplicate(idx, e.Name))
		}
		seen[e.Name] = true
	}
	return errs
}

// validateMounts checks PRD 7.3.4: absolute path, no spaces, no "..", not "/",
// and no overlap with the other list.
func validateMounts(list []trainpilotv1alpha1.StorageMount, others []trainpilotv1alpha1.StorageMount,
	path *field.Path) field.ErrorList {
	var errs field.ErrorList
	for i, m := range list {
		idx := path.Index(i).Child("mountPath")
		if err := checkMountPath(m.MountPath); err != nil {
			errs = append(errs, field.Invalid(idx, m.MountPath, err.Error()))
			continue
		}
		for j, o := range list {
			if j >= i {
				break
			}
			if overlap(m.MountPath, o.MountPath) {
				errs = append(errs, field.Invalid(idx, m.MountPath,
					fmt.Sprintf("overlaps with %s", o.MountPath)))
			}
		}
		for _, o := range others {
			if overlap(m.MountPath, o.MountPath) {
				errs = append(errs, field.Invalid(idx, m.MountPath,
					fmt.Sprintf("overlaps with %s", o.MountPath)))
			}
		}
	}
	return errs
}

func checkMountPath(p string) error {
	switch {
	case p == "":
		return fmt.Errorf("mount path is required")
	case !strings.HasPrefix(p, "/"):
		return fmt.Errorf("must be absolute (start with /)")
	case p == "/":
		return fmt.Errorf("cannot mount over the container root")
	case strings.ContainsAny(p, " \t\n"):
		return fmt.Errorf("must not contain whitespace")
	case strings.Contains(p, ".."):
		return fmt.Errorf("must not contain ..")
	}
	return nil
}

func overlap(a, b string) bool {
	norm := func(s string) string { return strings.TrimSuffix(s, "/") }
	a, b = norm(a), norm(b)
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
