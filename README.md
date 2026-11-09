# TrainPilot

训练任务 Operator。TrainPilot 以 **Kubeflow Trainer v2** 为执行底座，在它之上补齐
训练平台需要的语义：整任务成组排队、自动重启与抢占重排、节点隔离、按运行段落计量、
最长运行时长、停止/继续/重跑，以及一个只读看板。

```text
TrainTask (trainpilot.wutz.dev/v1alpha1)
   │  本仓库的 operator：排队 / 预算 / 计量 / 生命周期
   ▼
TrainingRuntime + TrainJob  (trainer.kubeflow.org/v1alpha1)
   │  Kubeflow Trainer 控制器
   ▼
JobSet → Job → Pod(node × N)
```

Operator 自己渲染每一轮的 `TrainingRuntime`（而不是复用它人写的 runtime），因此 pod 规格
完全可控：`hostNetwork`、`rdma/hca`、`/dev/shm`、存储挂载、环境变量、亲和性与容忍都在
模板里一次写清；`TrainJob` 只引用这一轮的 runtime。JobSet 的 `failurePolicy.maxRestarts`
固定为 0 —— 重启账本归 operator 所有，operator 从不自作主张重启。

## 功能

| 能力 | 实现位置 |
| --- | --- |
| 排队与整任务成组调度（同批次进出） | `internal/controller` `admit()`：按 `creationTimestamp` FIFO，计算每节点空闲 GPU 后整任务一次性放行 |
| 排队超时（默认 24h） | `spec.faultTolerance.queueTimeoutSeconds` → `Interrupted/QueueTimeout` |
| 自动重启（默认 10 次，1–100） | `restartCount` / `maxRestarts`，每轮新建 `TrainingRuntime`+`TrainJob`，RUN_ID 不变 |
| 抢占重排（默认 40 次） | `preemptionCount`：operator 删掉当前轮 `TrainJob` 即计一次，不消耗用户重启预算 |
| 节点故障隔离（≤8 台 FIFO） | `status.excludedNodes` → 下一轮 `nodeAffinity` 的 `hostname NotIn` |
| 启动失败判定 | 镜像拉取/配置错误 + 15 分钟启动超时，均计入重启预算 |
| 按段落计量 | `status.segments[]`：从所有 worker Running 到本轮结束，记录该段时长与节点数 |
| 最长运行时长（1–720h，默认 168h） | 按**计量时长**累计判定，超时 `Failed/MaxRuntimeExceeded` |
| 停止 / 继续 / 重跑 | `spec.desiredState`，或注解 `trainpilot.wutz.dev/action=stop\|continue\|rerun=<id>` |
| `/dev/shm` 为内存的 50% | `--shared-memory-ratio`（默认 0.5），Memory emptyDir 挂到 `/dev/shm` |
| 多机 + 8 卡自动 RoCE | worker>1 且每 worker 8 卡时自动 `hostNetwork` + `rdma/hca: 1` + `ClusterFirstWithHostNet` |
| 运行记录 | `status.workers[]`（pod、NODE_RANK、节点、阶段）、`status.conditions`、事件 |

多机时注入的环境变量：`NNODES`、`NODE_RANK`（取自 Job 的 `job-completion-index`）、
`MASTER_ADDR`（`<jobset>-node-0-0.<jobset>`，由 JobSet 的 headless Service 解析）、
`MASTER_PORT`、`TASK_ID`、`RUN_ID`、`RESTART_COUNT`，以及 Trainer 注入的 `PET_*`。
这些名字对用户保留，webhook 会拒绝覆盖它们的同名 env。

## 安装

```bash
helm install trainpilot oci://ghcr.io/wutz/trainpilot/charts/trainpilot \
  --namespace trainpilot-system --create-namespace \
  --set ingress.host=trainpilot.wutz.dev
```

前置条件只有一个：集群里已装好 **Kubeflow Trainer v2**（提供 `TrainingRuntime` /
`TrainJob` CRD）与 **JobSet**。Kueue、Volcano 都不需要 —— 排队和成组调度由本 operator 完成。

主要 values：

| Key | 默认值 | 说明 |
| --- | --- | --- |
| `image.repository` / `image.tag` | `ghcr.io/wutz/trainpilot` / Chart appVersion | 控制器镜像 |
| `replicaCount` | `2` | 多副本 + leader election，看板由所有副本提供 |
| `controller.gpuNodeLabel` | `nvidia.com/gpu.product` | GPU 型号所在的节点标签 |
| `controller.gpuTolerationKeys` | `nvidia.com/gpu` | worker 容忍的污点 key |
| `controller.sharedMemoryRatio` | `0.5` | `/dev/shm` 占内存比例 |
| `webhook.enabled` | `true` | TrainTask 校验（默认 `failurePolicy: Ignore`，不会阻塞创建） |
| `ingress.host` / `ingress.tls` | `trainpilot.wutz.dev` / 自签 issuer | 看板入口 |
| `crds.install` | `true` | 随 release 安装并升级 CRD |

## 使用

```bash
kubectl create namespace trainpilot-demo
kubectl apply -n trainpilot-demo -f examples/nccl-single-node.yaml
kubectl -n trainpilot-demo get traintasks -w
```

一个最小任务：

```yaml
apiVersion: trainpilot.wutz.dev/v1alpha1
kind: TrainTask
metadata:
  name: hello
spec:
  image: busybox:1.36
  command: ["echo hello from ${NODE_RANK}/${NNODES}"]
  workers: 2
  resources: { gpusPerWorker: 0, cpu: "2", memory: 4Gi }
```

操作：

```bash
kubectl annotate traintask hello trainpilot.wutz.dev/action=stop=$(date +%s)
kubectl annotate traintask hello trainpilot.wutz.dev/action=continue=$(date +%s)
kubectl annotate traintask hello trainpilot.wutz.dev/action=rerun=$(date +%s)
kubectl -n default patch traintask hello --type=merge -p '{"spec":{"desiredState":"Stopped"}}'
```

`examples/` 下还有两个 NCCL 基准：

- `nccl-single-node.yaml`：单机 8 卡 `all_reduce_perf`；
- `nccl-multi-node.yaml`：双机 16 卡跨节点 all-reduce（内嵌 CUDA 源码，用 `MASTER_ADDR`
  做 rendezvous，不依赖 MPI/SSH）。由 `make example-multi-node` 从
  `examples/multi-node-allreduce.cu` 生成。

## 状态与阶段

```
Queued → Starting → Running → Succeeded
   ↑         │          │  ├→ Recovering → (下一轮) Starting
   └─────────┴──────────┴──→ Failed / Stopped / Interrupted
```

- `Starting` 附带子步骤：`AllocatingResources` → `MountingStorage` → `PullingImage` → `StartingContainers`；
- `Recovering` 表示本轮已结束、等待下一轮（启动退避最长 60s），`status.round` 自增；
- 终态为 `Succeeded` / `Failed` / `Stopped` / `Interrupted`；`continue` 可从终态续跑，
  `rerun` 让 `spec.runID` +1 重新开跑。

只读看板：`/`（任务表、阶段、规模、重启/抢占、节点、计量），`/api/tasks`（同一份数据的 JSON），
`/healthz`。

## 开发

```bash
make manifests generate   # CRD / RBAC / deepcopy
make test                 # 单元测试（fake client，不需要 etcd）
make run                  # 用当前 kubeconfig 跑在本机
make docker-buildx IMG=ghcr.io/<owner>/trainpilot:dev PLATFORMS=linux/amd64,linux/arm64
helm template tp charts/trainpilot --namespace trainpilot-system
```

测试分两层：`internal/render` 校验渲染出的 pod/runtime 规格（RDMA、`/dev/shm`、环境变量、
亲和性、存储、命令），`internal/controller` 用 fake client 跑状态机（抢占计数、预算耗尽、
计量与最长运行时长、FIFO 准入、节点隔离、停止/继续/重跑）。

## 许可

Apache-2.0
