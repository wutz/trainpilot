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

package render

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

func task(workers, gpus int32) *trainpilotv1alpha1.TrainTask {
	return &trainpilotv1alpha1.TrainTask{
		ObjectMeta: metav1.ObjectMeta{Name: "nccl", Namespace: "trainpilot"},
		Spec: trainpilotv1alpha1.TrainTaskSpec{
			Image:   "example/nccl:latest",
			Command: []string{"echo hello"},
			Workers: workers,
			Resources: trainpilotv1alpha1.WorkerResources{
				GPUsPerWorker: gpus,
				CPU:           "16",
				Memory:        "64Gi",
			},
			GPUType: "NVIDIA-A100-SXM4-80GB",
		},
	}
}

func TestNamesStayWithinDNSLabel(t *testing.T) {
	long := strings.Repeat("a", 63)
	got := Names(long, Round{RunID: 12, Round: 34})
	if len(got) > 63 {
		t.Fatalf("name %q is %d chars, want <= 63", got, len(got))
	}
	if !strings.HasSuffix(got, "-r12-34") {
		t.Fatalf("name %q lost the run/round suffix", got)
	}
	if got := Names("nccl", Round{RunID: 1, Round: 1}); got != "nccl-r1-1" {
		t.Fatalf("got %q", got)
	}
}

func TestMultiNodeEightGPUEnablesRDMA(t *testing.T) {
	pod := WorkerPodSpec(task(2, 8), Round{RunID: 1, Round: 1}, Options{})
	if !pod.HostNetwork {
		t.Fatal("two workers with 8 GPUs must use host networking for RDMA")
	}
	if pod.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Fatalf("dnsPolicy = %q", pod.DNSPolicy)
	}
	c := pod.Containers[0]
	if _, ok := c.Resources.Limits[RDMAKey]; !ok {
		t.Fatal("rdma/hca must be requested")
	}
	if gpu := c.Resources.Limits[GPUKey]; gpu.Value() != 8 {
		t.Fatalf("gpu limit = %v", gpu.Value())
	}
}

func TestSingleNodeSkipsRDMA(t *testing.T) {
	pod := WorkerPodSpec(task(1, 8), Round{RunID: 1, Round: 1}, Options{})
	if pod.HostNetwork {
		t.Fatal("single-node tasks should not use host networking")
	}
	if _, ok := pod.Containers[0].Resources.Limits[RDMAKey]; ok {
		t.Fatal("single-node tasks should not request rdma/hca")
	}
}

func TestSharedMemoryIsHalfOfMemory(t *testing.T) {
	pod := WorkerPodSpec(task(1, 1), Round{RunID: 1, Round: 1}, Options{SharedMemoryRatio: 0.5})
	var vol *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].Name == SharedMemoryVolumeName {
			vol = &pod.Volumes[i]
		}
	}
	if vol == nil || vol.EmptyDir == nil {
		t.Fatal("dshm volume missing")
	}
	if vol.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Fatalf("dshm medium = %q", vol.EmptyDir.Medium)
	}
	// Worker memory is 64Gi, so /dev/shm must be 32Gi (default /dev/shm ratio).
	want := int64(64 * 1024 * 1024 * 1024 / 2)
	if got := (*vol.EmptyDir.SizeLimit).Value(); got != want {
		t.Fatalf("dshm size = %d, want %d", got, want)
	}
}

func TestPlatformEnvAndReservedNames(t *testing.T) {
	tk := task(3, 1)
	tk.Spec.Env = []corev1.EnvVar{
		{Name: "RUN_ID", Value: "hijack"},
		{Name: "PET_NNODES", Value: "1"},
		{Name: "MY_VAR", Value: "ok"},
	}
	pod := WorkerPodSpec(tk, Round{RunID: 4, Round: 2, RestartCount: 3}, Options{})
	env := map[string]corev1.EnvVar{}
	for _, e := range pod.Containers[0].Env {
		env[e.Name] = e
	}
	if env["RUN_ID"].Value != "4" {
		t.Fatalf("RUN_ID = %q, platform value must win", env["RUN_ID"].Value)
	}
	if env["RESTART_COUNT"].Value != "3" {
		t.Fatalf("RESTART_COUNT = %q", env["RESTART_COUNT"].Value)
	}
	if env["NNODES"].Value != "3" {
		t.Fatalf("NNODES = %q", env["NNODES"].Value)
	}
	if env["MASTER_ADDR"].Value != "nccl-r4-2-node-0-0.nccl-r4-2" {
		t.Fatalf("MASTER_ADDR = %q", env["MASTER_ADDR"].Value)
	}
	if _, ok := env["PET_NNODES"]; ok {
		t.Fatal("PET_* is injected by Trainer and must not be duplicated")
	}
	if env["MY_VAR"].Value != "ok" {
		t.Fatal("user variables must be kept")
	}
	if env["NODE_RANK"].ValueFrom == nil ||
		!strings.Contains(env["NODE_RANK"].ValueFrom.FieldRef.FieldPath, "job-completion-index") {
		t.Fatal("NODE_RANK must come from the Job completion index annotation")
	}
}

func TestWorkerAntiAffinityAndExclusion(t *testing.T) {
	tk := task(2, 8)
	pod := WorkerPodSpec(tk, Round{RunID: 1, Round: 2, ExcludedNodes: []string{"gn-a", "gn-b"}}, Options{})
	if pod.Affinity == nil || pod.Affinity.PodAntiAffinity == nil {
		t.Fatal("multi-worker tasks need hard hostname anti-affinity")
	}
	term := pod.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	if term.TopologyKey != HostnameLabel {
		t.Fatalf("topology key = %q", term.TopologyKey)
	}
	if term.LabelSelector.MatchLabels[InstanceLabel] != "nccl" {
		t.Fatal("anti-affinity must be scoped to the task")
	}
	terms := pod.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions
	var excluded []string
	for _, e := range terms {
		if e.Key == HostnameLabel {
			excluded = e.Values
		}
	}
	if len(excluded) != 2 {
		t.Fatalf("excluded nodes = %v", excluded)
	}
}

func TestStorageMounts(t *testing.T) {
	tk := task(1, 1)
	tk.Spec.SharedStorage = []trainpilotv1alpha1.StorageMount{{ClaimName: "data", MountPath: "/data"}}
	tk.Spec.PublicData = &trainpilotv1alpha1.StorageMount{
		ClaimName: "pub", MountPath: "/public", ReadOnly: true,
	}
	pod := WorkerPodSpec(tk, Round{RunID: 1, Round: 1}, Options{})
	byPath := map[string]corev1.VolumeMount{}
	vols := map[string]corev1.Volume{}
	for _, m := range pod.Containers[0].VolumeMounts {
		byPath[m.MountPath] = m
	}
	for _, v := range pod.Volumes {
		vols[v.Name] = v
	}
	shared, ok := byPath["/data"]
	if !ok {
		t.Fatal("/data is not mounted")
	}
	if vols[shared.Name].PersistentVolumeClaim.ClaimName != "data" {
		t.Fatal("wrong claim for /data")
	}
	pub, ok := byPath["/public"]
	if !ok {
		t.Fatal("/public is not mounted")
	}
	if !pub.ReadOnly || !vols[pub.Name].PersistentVolumeClaim.ReadOnly {
		t.Fatal("public data must be read-only")
	}
	if _, ok := byPath["/dev/shm"]; !ok {
		t.Fatal("/dev/shm is not mounted")
	}
}

// The worker entry point must be /bin/sh: images such as busybox ship no bash,
// and a missing interpreter fails the container at start, not at admission.
func TestCommandEntryPointIsPOSIXShell(t *testing.T) {
	tk := task(1, 1)
	tk.Spec.Command = []string{"echo a", "echo b"}
	pod := WorkerPodSpec(tk, Round{RunID: 1, Round: 1}, Options{})
	c := pod.Containers[0]
	if len(c.Command) != 4 || c.Command[0] != "/bin/sh" || c.Command[1] != "-c" {
		t.Fatalf("command = %v", c.Command)
	}
	// The script rides in as $0 so the wrapper is fixed text.
	if c.Command[3] != "echo a\necho b" {
		t.Fatalf("script = %q", c.Command[3])
	}
	if !strings.Contains(c.Command[2], `exec bash -lc "$0"`) {
		t.Fatalf("bash is not preferred when present: %q", c.Command[2])
	}
	if !strings.Contains(c.Command[2], `exec sh -c "$0"`) {
		t.Fatalf("sh fallback missing: %q", c.Command[2])
	}
}

func TestJobSetNeverRestartsOnItsOwn(t *testing.T) {
	rt := TrainingRuntime(task(2, 8), Round{RunID: 3, Round: 1}, Options{})
	if rt.Spec.Template.Spec.FailurePolicy.MaxRestarts != 0 {
		t.Fatal("JobSet must not restart: the operator owns restart accounting")
	}
	if rt.Spec.Template.Spec.SuccessPolicy.Operator != "All" {
		t.Fatal("all trainer nodes must succeed for the round to succeed")
	}
	if *rt.Spec.MLPolicy.NumNodes != 2 {
		t.Fatalf("numNodes = %d", *rt.Spec.MLPolicy.NumNodes)
	}
	pod := rt.Spec.Template.Spec.ReplicatedJobs[0].Template.Spec.Template
	if pod.Labels[TrainerNodeLabel] != "trainer" {
		t.Fatal("the node replicated job needs the trainer ancestor label")
	}
	if pod.Spec.Containers[0].Name != NodeContainerName {
		t.Fatal("container must be named node")
	}
	if *rt.Spec.Template.Spec.ReplicatedJobs[0].Template.Spec.BackoffLimit != 0 {
		t.Fatal("backoffLimit must be 0 so a failed pod ends the round")
	}
	// MASTER_ADDR is a DNS name; without the JobSet network there is no
	// headless Service and the name cannot resolve.
	net := rt.Spec.Template.Spec.Network
	if net == nil || net.EnableDNSHostnames == nil || !*net.EnableDNSHostnames {
		t.Fatal("the runtime must enable DNS hostnames for MASTER_ADDR")
	}
	_ = time.Second
}
