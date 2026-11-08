#!/usr/bin/env bash
# Regenerates examples/nccl-multi-node.yaml by embedding
# examples/multi-node-allreduce.cu into the TrainTask command, so the two can
# never drift apart.
set -euo pipefail
cd "$(dirname "$0")/.."

src=examples/multi-node-allreduce.cu
out=examples/nccl-multi-node.yaml
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

sed 's/^/        /' "$src" >"$tmp"

cat >"$out" <<'YAML'
# Two workers x 8 GPUs across two nodes: verifies multi-node rendering
# (NNODES / NODE_RANK / MASTER_ADDR, hostNetwork, rdma/hca) with a real
# cross-node NCCL all-reduce. The CUDA source is embedded so the pod needs
# nothing but the nccl-tests devel image.
#
# Regenerate with: make example-multi-node
apiVersion: trainpilot.wutz.dev/v1alpha1
kind: TrainTask
metadata:
  name: nccl-multi
  namespace: trainpilot-demo
spec:
  description: 双节点 16 卡 NCCL all_reduce（跨节点 RoCE）
  image: mir.blsc.cn/library/coreweave/nccl-tests:12.9.1-devel-ubuntu22.04-nccl2.27.6-1-7c12c62
  command:
    - |
      set -eux
      nvidia-smi -L
      cat >/tmp/allreduce.cu <<'CU'
YAML

cat "$tmp" >>"$out"

cat >>"$out" <<'YAML'
      CU
      nvcc -O2 -o /tmp/allreduce /tmp/allreduce.cu -lnccl
      NODE_RANK="${NODE_RANK:-0}" GPUS_PER_NODE="${PET_NPROC_PER_NODE:-8}" /tmp/allreduce
  workers: 2
  gpuType: rtx-5090
  resources:
    gpusPerWorker: 8
    cpu: "32"
    memory: 128Gi
  pricePerWorkerHour: "24.00"
  faultTolerance:
    autoRestart: true
    maxRestarts: 3
    maxRuntimeSeconds: 3600
  tolerations:
    - key: scclabs.dev/vgpu
      operator: Exists
      effect: NoSchedule
YAML

echo "wrote $out"
