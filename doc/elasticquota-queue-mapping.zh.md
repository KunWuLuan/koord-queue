# ElasticQuota 插件如何为工作负载确定队列

本文说明当 kube-queue 使用 **ElasticQuota** 分组插件（基于树的配额插件）时，如何为一个工作负载决定
**归属哪个配额（quota）**以及**进入哪个队列（queue）**。

> 这里讨论的是通过 `QueueGroupPlugin=elasticquota` 选择的 `ElasticQuota` 插件。关于每个队列的
> *出队顺序*如何配置，请参见
> [为 ElasticQuota 插件设置队列策略](./elasticquota-queue-policy.zh.md)。

## 概述

当你提交一个工作负载（如 `Job`、`PyTorchJob`、`MPIJob`、`TFJob`、`RayJob` 等）时，kube-queue 会生成一个
`QueueUnit` 来在排队系统中代表该工作负载。工作负载的 **labels 和 annotations 会被原样复制到 `QueueUnit`
上**。随后 ElasticQuota 插件会读取这些 labels 和 annotations，分别解析两件事：

1. **配额（quota）**——工作负载计入 `ElasticQuotaTree` 的哪个节点。
2. **队列（queue）**——工作负载实际在哪个 `Queue` 资源中等待。

这两者是相互独立解析的，理解这一区分是控制工作负载落位的关键。

## 第 1 步 —— 确定配额

插件按以下顺序选择配额节点：

1. **显式配额标签。** 如果工作负载带有以下任一标签（且 `ElasticQuota` 特性开关已开启，默认即开启），
   该标签的值将直接指定配额节点：

   ```yaml
   metadata:
     labels:
       quota.scheduling.alibabacloud.com/name: <quota-name>
       # 或等价地：
       quota.scheduling.koordinator.sh/name: <quota-name>
   ```

   如果两个标签同时存在，`quota.scheduling.alibabacloud.com/name` 优先。

2. **命名空间映射（默认）。** 如果两个标签都不存在，则根据工作负载的**命名空间**解析配额。
   每个命名空间都会映射到 `ElasticQuotaTree` 中的某个配额节点，因此没有配额标签的工作负载会自动计入
   拥有其命名空间的那个配额。

如果以上两种方式都无法解析到配额节点，工作负载将被拒绝，并提示其不属于任何 elastic quota。

### 可选的可用性校验

有一个可选、默认关闭的安全校验（`ElasticQuotaTreeCheckAvailableQuota`），它会额外要求标签所指定的配额
必须在工作负载**所在命名空间中可用**。当该校验开启且所指定的配额在该命名空间中不可用时，即使配额节点存在，
工作负载也会被拒绝。当该校验关闭（默认）时，只要标签指定的配额存在即被接受。

## 第 2 步 —— 确定队列

在确定配额之后，插件会把工作负载映射到具体的 `Queue` 资源（队列位于 `kube-queue` 命名空间中）：

1. **显式队列注解。** 如果工作负载带有队列名注解，其值将指定目标队列：

   ```yaml
   metadata:
     annotations:
       kube-queue/queue-name: <queue-name>
   ```

   所指定的 `Queue` 必须存在，且第 1 步解析出的配额必须在该队列中**可用**。如果配额在该队列中不可用，
   工作负载将被拒绝并给出相应错误。

2. **自动队列解析（默认）。** 如果注解缺失或为空，行为取决于 `ElasticQuotaTreeDecoupledQueue`
   特性开关：

   - **开启（默认）：** 插件会自动将工作负载路由到它为该配额创建的 `Queue`。插件为每个配额节点维护
     一个自动创建的队列，并通过配额的全名查找它，因此在常见场景下无需任何注解。
   - **关闭：** 工作负载将被拒绝，并提示必须在 queue unit 上设置队列名——即你必须自行提供
     `kube-queue/queue-name` 注解。

## 特性开关默认值

| 特性开关 | 默认值 | 作用 |
|----------|--------|------|
| `ElasticQuota` | 开启 | 识别显式配额标签；否则按命名空间推导配额。 |
| `ElasticQuotaTreeDecoupledQueue` | 开启 | 未设置队列注解时，自动路由到该配额的托管队列。 |
| `ElasticQuotaTreeCheckAvailableQuota` | 关闭 | 开启时，标签指定的配额必须在工作负载所在命名空间中可用。 |

## 实践建议

在默认特性开关下，大多数工作负载**无需任何排队相关的元数据**：

- **依赖命名空间。** 将工作负载提交到映射了目标配额的命名空间。插件会根据命名空间推导配额，并将工作负载
  路由到该配额自动创建的队列。
- **指定特定配额。** 添加 `quota.scheduling.alibabacloud.com/name` 标签，使工作负载不论所在命名空间
  都计入某个特定配额节点。插件仍会自动将其路由到该配额的托管队列。
- **指定特定队列。** 仅当你希望将工作负载指向某个特定的、手工管理的队列时，才添加
  `kube-queue/queue-name` 注解。请确保解析出的配额在该队列中可用，否则工作负载会被拒绝。

## 小结

- 工作负载的 labels 和 annotations 会被复制到其 `QueueUnit` 上；插件从那里读取它们。
- **配额**由 `quota.scheduling.alibabacloud.com/name` /
  `quota.scheduling.koordinator.sh/name` 标签决定，否则回退到工作负载的命名空间。
- **队列**由 `kube-queue/queue-name` 注解决定，否则（在 `ElasticQuotaTreeDecoupledQueue` 开启时）
  回退到为解析出的配额自动创建的队列。
- 在默认部署中，把工作负载提交到正确的命名空间——或用配额名给它打标签——就足够了；队列会被自动解析。
