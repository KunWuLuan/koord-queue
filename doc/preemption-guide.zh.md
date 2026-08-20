# kube-queue 抢占配置指南

本文档说明如何配置队列和任务以触发抢占，以及两种抢占场景的具体条件。

## 一、队列配置

### 1.1 ElasticQuota（配额）

ElasticQuota 定义了队列可用的资源上限和下限：

```yaml
apiVersion: scheduling.sigs.k8s.io/v1alpha1
kind: ElasticQuota
metadata:
  name: test-quota-wait          # 名称必须与 Queue 名称一致
  namespace: test-used-bug       # 业务命名空间
  labels:
    quota.scheduling.koordinator.sh/parent: koordinator-root-quota
spec:
  max:
    cpu: "4"
    memory: 4Gi
  min:
    cpu: "4"
    memory: 4Gi
```

### 1.2 Queue（队列注解）

Queue 由 ElasticQuota 控制器自动创建，位于 `kube-queue` 命名空间。需要手动添加以下注解才能启用抢占：

```bash
kubectl annotate queue test-quota-wait -n kube-queue \
  kube-queue/wait-for-pods-running=true \
  kube-queue/enable-queueunit-preemption=true
```

| 注解 | 必需 | 作用 |
| --- | --- | --- |
| `kube-queue/wait-for-pods-running` | 是 | 将已 Dequeued 但 Pod 未全部 Running 的 QueueUnit 保留在 `assumed` 集合中，使其可被选为抢占 victim。不设置则抢占被禁用。 |
| `kube-queue/enable-queueunit-preemption` | 是 | `q.Preempt()` 的总开关。仅控制 quota 不足时 Filter 失败后的抢占路径。 |

> **注意**：两个注解都必须为 `"true"`。`wait-for-pods-running` 是基础条件，`enable-queueunit-preemption` 是 quota 不足路径的额外开关。

### 1.3 Queue 策略

Queue 的 `spec.queuePolicy` 必须为 `Priority`，因为抢占逻辑实现在 `schedulingqueuev2`（PriorityQueue）中：

```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: Queue
metadata:
  name: test-quota-wait
  namespace: kube-queue
  annotations:
    kube-queue/wait-for-pods-running: "true"
    kube-queue/enable-queueunit-preemption: "true"
spec:
  queuePolicy: Priority    # 必须为 Priority
```

> **说明**：抢占机制在队列层，与 Filter 插件无关。无论使用哪个 Filter 插件（ElasticQuotaV2、树形 ElasticQuota 等），Filter 返回非 Success 时都会触发 `q.Preempt()`，Filter 通过时 `Reserve()` 也会执行队列内抢占。

## 二、任务配置

### 2.1 必需标签

Job（或其他工作负载）必须携带以下标签才能被正确分配到队列和配额：

```yaml
metadata:
  labels:
    # 指定归属的 ElasticQuota 名称（必须与 ElasticQuota.metadata.name 一致）
    quota.scheduling.koordinator.sh/name: test-quota-wait
```

> 如果使用阿里云 ASI 环境，也可用 `alibabacloud.com/quota-name` 替代。

### 2.2 优先级

优先级决定抢占方向：**高优先级任务可以抢占低优先级任务**。

通过 Pod 模板中的 `priorityClassName` 设置：

```yaml
spec:
  template:
    spec:
      priorityClassName: pai-priority-9   # PriorityClass 必须预先存在
```

也可以通过注解直接指定数值优先级（覆盖 priorityClassName 的值）：

```yaml
metadata:
  annotations:
    scheduling.x-k8s.io/priority: "100"
```

| 配置方式 | 说明 |
| --- | --- |
| `priorityClassName` | 引用集群中已存在的 PriorityClass，取其 value 作为优先级 |
| `scheduling.x-k8s.io/priority` 注解 | 直接指定数值，优先级高于 priorityClassName |

> 若未设置任何优先级，则默认为 0。多个同为 0 的任务之间不会发生抢占。

### 2.3 Job 模板示例

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: high-prio-job
  namespace: test-used-bug
  labels:
    quota.scheduling.koordinator.sh/name: test-quota-wait
spec:
  suspend: true                    # 必须为 true，由 kube-queue 控制何时启动
  template:
    spec:
      restartPolicy: Never
      priorityClassName: pai-priority-9    # 高优先级
      containers:
      - name: worker
        image: busybox
        command: ["sleep", "3600"]
        resources:
          requests:
            cpu: "1"
            memory: "1Gi"
```

## 三、抢占触发条件

### 3.1 两种抢占路径

| | 路径 A：Quota 足够 | 路径 B：Quota 不足 |
| --- | --- | --- |
| **Filter 结果** | Success（配额够用） | Unschedulable（配额耗尽） |
| **抢占入口** | `Reserve()` | `Preempt()` |
| **触发条件** | `waitPodsRunning=true` 且 `assumed` 非空 | `enablePreempt=true` 且 `waitPodsRunning=true` |
| **抢占逻辑** | 抢占所有比抢占者优先级低的 assumed 任务 | 从低到高抢占，直到释放的资源满足抢占者需求 |
| **日志特征** | `preemption: waiting for queueUnit preempted, victims: [...]` | `preempt in queue <queue> completed(...)` |

### 3.2 路径 A：Quota 足够时的抢占

**场景**：配额充足，但队列采用 `wait-for-pods-running` 模式（一次只允许一个任务在调度中），高优任务需要插队。

**触发条件**：
1. Queue 注解 `kube-queue/wait-for-pods-running=true`
2. 队列中已有任务处于 `assumed` 状态（已 Dequeued，Pod 未全部 Running）
3. 新任务的优先级 **高于** assumed 中的任务
4. Filter 检查通过（used + request ≤ max）

**流程**：
```
高优任务提交 → Filter 通过（quota 够）→ Reserve()
  → 发现 waitPodsRunning && assumed 非空
  → 遍历 assumed，找到优先级更低的 victim
  → 设置 victim 的 ReclaimState（replicas 降为 0）
  → 高优任务被标记为 unschedulable（等待 victim 释放）
  → 下一轮调度：高优任务成功 Dequeued
```

### 3.3 路径 B：Quota 不足时的抢占

**场景**：配额已耗尽，高优任务 Filter 失败，需要抢占低优任务释放资源。

**触发条件**：
1. Queue 注解 `kube-queue/wait-for-pods-running=true`
2. Queue 注解 `kube-queue/enable-queueunit-preemption=true`
3. 队列中已有任务处于 `assumed` 状态
4. 新任务的优先级 **高于** assumed 中的至少一个任务
5. Filter 检查失败（used + request > max）
6. Victim 有可回收资源（`Replicas > Running` 且无 `ReclaimState`）

**流程**：
```
高优任务提交 → Filter 失败（quota 不够）→ q.Preempt()
  → dryRunPreemption: 从 assumed 中按优先级从低到高选 victim
  → 累加 victim 可回收资源，直到满足抢占者需求
  → 设置 victim 的 ReclaimState
  → 抢占者重新入队等待
  → job-extension 回收 victim Pod，配额释放
  → 后续调度周期：高优任务成功 Dequeued
```

### 3.4 Victim 可回收条件

一个 QueueUnit 能被选为 victim，需同时满足：

| 条件 | 检查函数 | 说明 |
| --- | --- | --- |
| 已 Dequeued 且预留了资源 | `IsQueueUnitDequeued()` / `IsQueueUnitReservedAnyResource()` | 在 `assumed` 中，且有 Admissions |
| 有可回收的副本 | `GetResourcesCanReclaim()` | `Replicas > Running` 且无 `ReclaimState` |
| 优先级低于抢占者 | `q.lessFunc(preemptor, victim) < 0` | 抢占者优先级更高 |
| 不在保护期内 | `DefaultReclaimProtectTime` | 若配置了保护时间，刚分配的任务不可被回收 |

## 四、完整测试示例

### 4.1 Quota 足够场景

```bash
# 1. 提交低优任务（priority=0）
cat <<'EOF' | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: low-prio-job
  namespace: test-used-bug
  labels:
    quota.scheduling.koordinator.sh/name: test-quota-wait
spec:
  suspend: true
  template:
    spec:
      restartPolicy: Never
      priorityClassName: pai-priority-1
      containers:
      - name: worker
        image: busybox
        command: ["sleep", "3600"]
        resources:
          requests: {cpu: "1", memory: "1Gi"}
EOF

# 2. 等待低优任务 Dequeued（进入 assumed）
kubectl get queueunit low-prio-job -n test-used-bug -w

# 3. 提交高优任务（priority=9）
cat <<'EOF' | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: high-prio-job
  namespace: test-used-bug
  labels:
    quota.scheduling.koordinator.sh/name: test-quota-wait
spec:
  suspend: true
  template:
    spec:
      restartPolicy: Never
      priorityClassName: pai-priority-9
      containers:
      - name: worker
        image: busybox
        command: ["sleep", "3600"]
        resources:
          requests: {cpu: "1", memory: "1Gi"}
EOF

# 4. 验证结果
kubectl get queueunit -n test-used-bug -o wide
# high-prio-job  Dequeued  9   replicas=1, running=0
# low-prio-job   Dequeued      replicas=0  (被抢占)
```

### 4.2 Quota 不足场景

将 ElasticQuota 的 min/max 调小为 1 cpu，两个任务各需 1 cpu：

```bash
# 1. 修改 quota 为 1 cpu
kubectl patch elasticquota test-quota-wait -n test-used-bug --type merge -p \
  '{"spec":{"min":{"cpu":"1","memory":"1Gi"},"max":{"cpu":"1","memory":"1Gi"}}}'

# 2. 提交低优任务 → Dequeued（占用 1 cpu）
# 3. 提交高优任务 → Filter 失败（1+1 > 1）→ Preempt
# 4. 验证：高优 Dequeued，低优 replicas=0
```

## 五、排查抢占不生效

| 现象 | 排查方向 |
| --- | --- |
| `qu doesn't belong to any queue` | Job 缺少 `quota.scheduling.koordinator.sh/name` 标签，或值与 ElasticQuota 名称不匹配 |
| `preemption is disabled in queue` | Queue 缺少 `kube-queue/enable-queueunit-preemption=true` 注解 |
| `preemption is disabled because wait-for-pods-running is disabled` | Queue 缺少 `kube-queue/wait-for-pods-running=true` 注解 |
| `no victims found` | Victim 不在 `assumed` 中（可能 Pod 已全部 Running 被移除），或无可回收资源（Replicas == Running），或优先级不比抢占者低 |
| `can not find priority class` | `priorityClassName` 引用的 PriorityClass 不存在，优先级会被忽略（默认 0） |
| Victim 太快离开 assumed | Pod 调度后立即 Running（如 KWOK 节点），导致 victim 从 assumed 中移除。可通过 nodeSelector 指向带 taint 的节点但不加 toleration 来保持 Pod Pending |
