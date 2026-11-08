# 验收场景

按顺序在 `trainpilot-demo` 命名空间执行，每条后面是对应的期望结果。

```bash
NS=trainpilot-demo
kubectl apply -n $NS -f examples/verification/01-basic.yaml
kubectl -n $NS get traintask verify-basic -w
```

| 文件 | 场景 | 期望 |
| --- | --- | --- |
| `01-basic.yaml` | 2 worker 正常退出 | `Succeeded`，`billedSeconds>0`，两个 worker 的 `NODE_RANK` 分别为 0/1 |
| `02-restart.yaml` | 第一轮失败、第二轮成功 | 经历 `Recovering`，`restartCount=1`，`round=2`，最终 `Succeeded`，`runID` 不变 |
| `03-budget.yaml` | 永远失败，`maxRestarts=2` | 3 轮后 `Failed`，`reason=RestartsExhausted`，`restartCount=3` |
| `04-max-runtime.yaml` | 运行超过 120s | `Failed`，`reason=MaxRuntimeExceeded`，worker 被回收 |
| `05-queue.yaml` | 4 个整机任务抢 3 台节点 | 前 3 个 `Running`，第 4 个 `Queued`；释放一个后第 4 个开始 |
| `../nccl-single-node.yaml` | 单机 8 卡 NCCL | pod 日志出现 `Avg bus bandwidth`，任务 `Succeeded` |
| `../nccl-multi-node.yaml` | 双机 16 卡跨节点 NCCL | 日志 `RESULT: OK (all 16 ranks agreed)` |

停止 / 继续 / 重跑：

```bash
kubectl annotate traintask verify-basic trainpilot.wutz.dev/action=stop=$(date +%s)
kubectl annotate traintask verify-basic trainpilot.wutz.dev/action=continue=$(date +%s)
kubectl annotate traintask verify-basic trainpilot.wutz.dev/action=rerun=$(date +%s)
```

抢占重排（平台侧回收当前轮）：

```bash
kubectl -n $NS delete trainjob -l trainpilot.wutz.dev/task=verify-basic
# 期望：phase=Recovering，preemptionCount+1，随后进入下一轮，runID 不变
```
