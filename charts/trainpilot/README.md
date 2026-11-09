# TrainPilot Helm chart

部署 TrainPilot（训练任务 Operator）。依赖集群中已有的 **Kubeflow Trainer v2** 与
**JobSet**；webhook 证书与入口证书默认由 **cert-manager** 签发。

```bash
helm install trainpilot oci://ghcr.io/wutz/trainpilot/charts/trainpilot \
  --namespace trainpilot-system --create-namespace \
  --set ingress.host=trainpilot.example.com
```

## Values

| Key | Type | Default | 说明 |
| --- | --- | --- | --- |
| `image.repository` | string | `ghcr.io/wutz/trainpilot` | 控制器镜像 |
| `image.tag` | string | `""`（取 appVersion） | 镜像 tag |
| `replicaCount` | int | `2` | 副本数；leader election 保证只有一个协调器，所有副本都提供看板 |
| `leaderElection` | bool | `true` | 开启 leader election |
| `serviceAccount.create` | bool | `true` | 创建 ServiceAccount |
| `rbac.create` | bool | `true` | 创建 ClusterRole/Binding 与 leader election Role |
| `controller.gpuNodeLabel` | string | `nvidia.com/gpu.product` | GPU 型号标签，用于节点选择 |
| `controller.gpuTolerationKeys` | string | `nvidia.com/gpu` | worker 容忍的污点 key（逗号分隔） |
| `controller.sharedMemoryRatio` | float | `0.5` | `/dev/shm` 占内存上限比例 |
| `controller.workerServiceAccount` | string | `""` | worker pod 使用的 ServiceAccount |
| `controller.extraArgs` | list | `[]` | 追加的 manager 参数 |
| `ui.enabled` | bool | `true` | 内嵌只读看板 |
| `ui.port` | int | `8082` | 看板监听端口 |
| `metrics.enabled` | bool | `true` | 暴露 controller-runtime 指标 |
| `metrics.secure` | bool | `true` | 指标走 HTTPS + 认证鉴权 |
| `webhook.enabled` | bool | `true` | TrainTask 校验 webhook |
| `webhook.failurePolicy` | string | `Ignore` | webhook 不可达时不阻塞创建 |
| `webhook.certManager.enabled` | bool | `true` | 用 cert-manager 签发并注入 CA |
| `webhook.certManager.issuerRef` | object | `selfsigned-cluster-issuer` | 签发 webhook 证书的 issuer |
| `crds.install` | bool | `true` | 随 release 安装/升级 CRD |
| `ingress.enabled` | bool | `true` | 创建 Ingress |
| `ingress.className` | string | `nginx` | IngressClass |
| `ingress.host` | string | `trainpilot.wutz.dev` | 看板域名 |
| `ingress.tls.issuerRef` | object | `selfsigned-cluster-issuer` | Ingress 证书 issuer |

## 卸载

```bash
helm uninstall trainpilot -n trainpilot-system
```

CRD 带 `helm.sh/resource-policy: keep`，会随 TrainTask 数据保留；确认不再需要时手动删除：

```bash
kubectl delete crd traintasks.trainpilot.wutz.dev
```
