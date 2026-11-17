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
// The webhook enforces the create-form rules: environment variable names,
// reserved names, mount path shape and overlap, and the "keep image/command on
// continue, change them with re-run" rule.
package v1alpha1

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

// envNamePattern is [A-Za-z_][A-Za-z0-9_]*, at most 128 characters.
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// reservedEnv are injected by the operator and must not be overridden.
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
// Re-run (spec.runID increments) may change pod/scheduling fields; an automatic
// restart or a continue (same runID) keeps structural worker fields frozen while
// allowing lifecycle/tolerance updates (desiredState, suspend, faultTolerance,
// description, terminationGracePeriodSeconds, ttlSecondsAfterFinished).
func (w *TrainTaskWebhook) ValidateUpdate(_ context.Context,
	oldTask, newTask *trainpilotv1alpha1.TrainTask) (admission.Warnings, error) {
	if err := w.validate(newTask); err != nil {
		return nil, err
	}

	var errs field.ErrorList
	if newTask.Spec.RunID < oldTask.Spec.RunID {
		errs = append(errs, field.Invalid(field.NewPath("spec", "runID"), newTask.Spec.RunID,
			"runID cannot decrease"))
	}
	if newTask.Spec.RunID == oldTask.Spec.RunID {
		errs = append(errs, validateRunIDFrozenFields(&oldTask.Spec, &newTask.Spec)...)
	}
	if len(errs) > 0 {
		return nil, apierrors.NewInvalid(schema.GroupKind{Group: trainpilotv1alpha1.GroupVersion.Group,
			Kind: "TrainTask"}, newTask.Name, errs)
	}
	return nil, nil
}

func validateRunIDFrozenFields(oldSpec, newSpec *trainpilotv1alpha1.TrainTaskSpec) field.ErrorList {
	var errs field.ErrorList
	specPath := field.NewPath("spec")
	const msg = "can only change with a re-run (increment spec.runID)"

	if oldSpec.Image != newSpec.Image {
		errs = append(errs, field.Forbidden(specPath.Child("image"), "image "+msg))
	}
	if !slices.Equal(oldSpec.Command, newSpec.Command) {
		errs = append(errs, field.Forbidden(specPath.Child("command"), "command "+msg))
	}
	if oldSpec.Workers != newSpec.Workers {
		errs = append(errs, field.Forbidden(specPath.Child("workers"), "workers "+msg))
	}
	if !reflect.DeepEqual(oldSpec.Resources, newSpec.Resources) {
		errs = append(errs, field.Forbidden(specPath.Child("resources"), "resources "+msg))
	}
	if oldSpec.GPUType != newSpec.GPUType {
		errs = append(errs, field.Forbidden(specPath.Child("gpuType"), "gpuType "+msg))
	}
	if oldSpec.WorkingDir != newSpec.WorkingDir {
		errs = append(errs, field.Forbidden(specPath.Child("workingDir"), "workingDir "+msg))
	}
	if !reflect.DeepEqual(oldSpec.ImagePullSecrets, newSpec.ImagePullSecrets) {
		errs = append(errs, field.Forbidden(specPath.Child("imagePullSecrets"), "imagePullSecrets "+msg))
	}
	if !reflect.DeepEqual(oldSpec.NodeSelector, newSpec.NodeSelector) {
		errs = append(errs, field.Forbidden(specPath.Child("nodeSelector"), "nodeSelector "+msg))
	}
	if !reflect.DeepEqual(oldSpec.Tolerations, newSpec.Tolerations) {
		errs = append(errs, field.Forbidden(specPath.Child("tolerations"), "tolerations "+msg))
	}
	if !reflect.DeepEqual(oldSpec.Env, newSpec.Env) {
		errs = append(errs, field.Forbidden(specPath.Child("env"), "env "+msg))
	}
	if !reflect.DeepEqual(oldSpec.EnvFrom, newSpec.EnvFrom) {
		errs = append(errs, field.Forbidden(specPath.Child("envFrom"), "envFrom "+msg))
	}
	if !reflect.DeepEqual(oldSpec.SharedStorage, newSpec.SharedStorage) {
		errs = append(errs, field.Forbidden(specPath.Child("sharedStorage"), "sharedStorage "+msg))
	}
	if !reflect.DeepEqual(oldSpec.PublicData, newSpec.PublicData) {
		errs = append(errs, field.Forbidden(specPath.Child("publicData"), "publicData "+msg))
	}
	if !reflect.DeepEqual(oldSpec.RDMA, newSpec.RDMA) {
		errs = append(errs, field.Forbidden(specPath.Child("rdma"), "rdma "+msg))
	}
	if !reflect.DeepEqual(oldSpec.MasterPort, newSpec.MasterPort) {
		errs = append(errs, field.Forbidden(specPath.Child("masterPort"), "masterPort "+msg))
	}
	if oldSpec.QueueName != newSpec.QueueName {
		errs = append(errs, field.Forbidden(specPath.Child("queueName"), "queueName "+msg))
	}
	return errs
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

	errs = append(errs, validateResources(spec.Resources)...)
	errs = append(errs, validateEnv(spec.Env)...)
	errs = append(errs, validateEnvFrom(spec.EnvFrom)...)
	errs = append(errs, validateMounts(spec.SharedStorage, nil, field.NewPath("spec", "sharedStorage"))...)
	if spec.PublicData != nil {
		errs = append(errs, validateMounts([]trainpilotv1alpha1.StorageMount{*spec.PublicData},
			spec.SharedStorage, field.NewPath("spec", "publicData"))...)
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

func validateResources(res trainpilotv1alpha1.WorkerResources) field.ErrorList {
	var errs field.ErrorList
	resPath := field.NewPath("spec", "resources")
	if res.CPU != "" {
		qty, err := resource.ParseQuantity(res.CPU)
		if err != nil {
			errs = append(errs, field.Invalid(resPath.Child("cpu"), res.CPU,
				fmt.Sprintf("invalid quantity: %v", err)))
		} else if qty.Sign() <= 0 {
			errs = append(errs, field.Invalid(resPath.Child("cpu"), res.CPU,
				"must be greater than zero"))
		}
	}
	if res.Memory != "" {
		qty, err := resource.ParseQuantity(res.Memory)
		if err != nil {
			errs = append(errs, field.Invalid(resPath.Child("memory"), res.Memory,
				fmt.Sprintf("invalid quantity: %v", err)))
		} else if qty.Sign() <= 0 {
			errs = append(errs, field.Invalid(resPath.Child("memory"), res.Memory,
				"must be greater than zero"))
		}
	}
	return errs
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

func validateEnvFrom(envFrom []corev1.EnvFromSource) field.ErrorList {
	var errs field.ErrorList
	path := field.NewPath("spec", "envFrom")
	for i, src := range envFrom {
		hasCM := src.ConfigMapRef != nil
		hasSec := src.SecretRef != nil
		switch {
		case !hasCM && !hasSec:
			errs = append(errs, field.Required(path.Index(i),
				"must set either configMapRef or secretRef"))
		case hasCM && hasSec:
			errs = append(errs, field.Invalid(path.Index(i), "",
				"cannot set both configMapRef and secretRef"))
		case hasCM && strings.TrimSpace(src.ConfigMapRef.Name) == "":
			errs = append(errs, field.Required(path.Index(i).Child("configMapRef", "name"),
				"configMapRef.name is required"))
		case hasSec && strings.TrimSpace(src.SecretRef.Name) == "":
			errs = append(errs, field.Required(path.Index(i).Child("secretRef", "name"),
				"secretRef.name is required"))
		}
	}
	return errs
}

// validateMounts checks absolute path, no spaces, no "..", not "/",
// no overlap with /dev/shm, and no overlap with the other list.
func validateMounts(list []trainpilotv1alpha1.StorageMount, others []trainpilotv1alpha1.StorageMount,
	path *field.Path) field.ErrorList {
	var errs field.ErrorList
	for i, m := range list {
		if strings.TrimSpace(m.ClaimName) == "" {
			errs = append(errs, field.Required(path.Index(i).Child("claimName"), "claimName is required"))
		}
		idx := path.Index(i).Child("mountPath")
		if err := checkMountPath(m.MountPath); err != nil {
			errs = append(errs, field.Invalid(idx, m.MountPath, err.Error()))
			continue
		}
		if overlap(m.MountPath, "/dev/shm") {
			errs = append(errs, field.Invalid(idx, m.MountPath,
				"cannot mount over platform shared memory /dev/shm"))
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
		return fmt.Errorf("must not contain a parent directory segment")
	}
	return nil
}

func overlap(a, b string) bool {
	norm := func(s string) string { return strings.TrimSuffix(s, "/") }
	a, b = norm(a), norm(b)
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
