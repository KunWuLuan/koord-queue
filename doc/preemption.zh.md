# kube-queue 抢占机制

本文档说明 kube-queue 如何抢占已出队(已预留)的任务，从而在配额耗尽时让更高优先级的 QueueUnit 得以出队
——即队列如何**为了抢占而将低优先级任务出队回收**。

## 概念

- **QueueUnit** —— 出队(admission)的基本单位。job-extension 会为每个被托管的工作负载创建一个 QueueUnit。
  等待期间它处于 *enqueued*(排队)状态；一旦调度器准入,它变为 *reserved* / *dequeued*(已预留/已出队),
  此时 job-extension 才被允许创建该工作负载的 Pod。
- **Reserved / assumed(已预留 / 假定占用)** —— 已出队的 QueueUnit 仍占用配额。队列会把它保留在内存中的
  `assumed` 集合里,以便在其 Pod 真正 Running 之前,它仍可被作为抢占的牺牲者(victim)考虑。
- **Victim(牺牲者)** —— 被回收资源以腾出空间的、优先级更低的已预留 QueueUnit。
- **Preemptor(抢占者)** —— 无法在配额内容纳、从而触发抢占的更高优先级 QueueUnit。
- **`ReclaimState`** —— QueueUnit admission 上的一个字段(`Status.Admissions[i].ReclaimState`)。设置它就是
  具体的抢占信号:它告诉 job-extension 需要回收(删除)该 podset 的多少个副本。这是唯一真正回收资源的地方。

## 两种机制

kube-queue 有两条不同层次的抢占路径。具体走哪一条,取决于所使用的分组插件。

### 1. 配额插件准入(树形 ElasticQuota 插件)

树形 `ElasticQuota` 插件在其 **`Filter`** 内部做抢占决策。当一个可抢占的高优先级 QueueUnit 会超过某配额的
`Max` 时,插件会做一次 *试算(dry-run)*(`canPreemptVictims`):检查回收同配额下低优先级的可抢占已预留单元
是否能腾出足够空间。若能,`Filter` 返回 **Success**,抢占者立即被准入——**不会**由调度器设置 `ReclaimState`。
牺牲者的资源会在稍后由外部流程释放:当其 Pod 被删除、job-extension 上报用量下降后,配额随之释放。

对不可抢占的入队作业有特殊处理:插件不为其检查 `Max`;当配额带有属性 `preempt-policy: PreemptLowerPriority`
时,它会扣除可回收的低优先级可抢占用量,从而让该作业在配额 `Min` 以内被准入。

因此,对树形插件而言,"可抢占时出队进行抢占"意味着:*即便配额已满,只要存在可被回收的低优先级可抢占牺牲者,
高优先级作业也会被准入(出队)。*

### 2. 队列级抢占(`q.Preempt` → `ReclaimState`)

调度器**仅在 `Filter` 返回非 Success 状态时**才调用 `q.Preempt`(见 `pkg/scheduler/scheduler.go` 的调度循环)。
这是真正写入 `ReclaimState` 的路径:

1. `q.Preempt`(`pkg/queue/queuepolicies/schedulingqueuev2/preempt.go`)受两个队列注解门控(见下文)。
   任一未开启,则返回错误、不发生抢占。
2. `dryRunPreemption` 遍历队列的 `assumed` 集合并按优先级排序。从最低优先级开始,选择**比抢占者优先级更低**、
   且有可回收资源(`utils.GetResourcesCanReclaim`:某 admission 的 `Replicas > Running` 且尚未处于回收中)的
   牺牲者,直到释放的资源覆盖抢占者的请求。
3. `preemptQueueUnits` 为每个被选中的牺牲者设置 `Status.Admissions[i].ReclaimState.Replicas`(跳过任何已完全
   出队或未预留任何资源者),并把其状态更新为 `"Waiting job extension to reclaim resources."`。

这条路径是 **ElasticQuotaV2(`elasticquotav1alpha1`)** 插件的主要抢占机制——该插件的 `Filter` 没有内部抢占
试算:当配额耗尽时它直接返回 **Unschedulable**,从而将调度器径直导向 `q.Preempt`。

> 注意:抢占者不会在牺牲者被清空期间被阻塞。它会被重新入队并重试;一旦 job-extension 回收了牺牲者的 Pod
> 并上报释放的用量,后续某个调度周期就会准入该抢占者。

## 开启抢占

### 队列注解(`q.Preempt` 必需)

`Queue` 对象上二者都必须为 `"true"`,队列级抢占才会运行:

| 注解 | 作用 |
| --- | --- |
| `kube-queue/wait-for-pods-running` | 将"已预留但未 Running"的单元保留在 `assumed` 集合中,使其可被作为牺牲者考虑。不设置则抢占被禁用。 |
| `kube-queue/enable-queueunit-preemption` | `q.Preempt` 的总开关。 |

它们在队列构建时被读取,并在队列更新时重新读取
(`pkg/queue/queuepolicies/schedulingqueuev2/schedulingqueuev2.go`)。树形 ElasticQuota 插件不会为其自动创建的
队列添加这些注解,因此在需要队列级路径时必须显式设置在 `Queue` 上。

### 作业可抢占标签(树形插件试算)

某作业在树形插件 `Filter` 试算中是否为合法牺牲者/抢占者,由 `util.IsJobPreemptible` 决定:

| QueueUnit 上的标签 | 取值 |
| --- | --- |
| `quota.scheduling.alibabacloud.com/preemptible` | `"true"` / `"false"` |
| `quota.scheduling.koordinator.sh/preemptible` | `"true"` / `"false"` |

若都未设置,则采用全局默认 `--defaultPreemptible`(默认 `true`)。队列级的 `dryRunPreemption` 按优先级与
可回收性选择牺牲者,**不**要求该标签。

### 配额属性(树形插件,不可抢占入队作业)

`ElasticQuota` 属性中的 `preempt-policy: PreemptLowerPriority` 允许一个*不可抢占*的入队作业回收低优先级的
可抢占已预留作业,直至配额 `Min`。

### 优先级

优先级取自 `QueueUnit.Spec.Priority`。优先级越高越靠前;牺牲者需比抢占者优先级更低才有资格被抢占。

## 端到端流程(队列级路径)

```
创建更高优先级的 QueueUnit
  → 调度器弹出它(优先级最高者优先)
  → RunFilterPlugins
      ElasticQuotaV2.Filter:配额耗尽 → Unschedulable
  → status != Success → q.Preempt(preemptor)
      wait-for-pods-running == true  且  enable-queueunit-preemption == true
      dryRunPreemption:挑选有可回收 admission 的低优先级已预留牺牲者
      preemptQueueUnits:设置牺牲者 Status.Admissions[i].ReclaimState.Replicas
  → 抢占者被重新入队并重试
  → job-extension 观察到 ReclaimState,删除相应数量的牺牲者 Pod,
      上报用量下降 → 配额释放
  → 后续调度周期准入抢占者
```

## 各插件对比

| | 树形 `ElasticQuota` | `ElasticQuotaV2`(`elasticquotav1alpha1`) |
| --- | --- | --- |
| 抢占决策 | 在 `Filter` 内试算(`canPreemptVictims`) | Filter 内无 |
| 配额满(可抢占)时 | `Filter` Success,准入抢占者,不设 `ReclaimState` | `Filter` Unschedulable |
| 是否设置 `ReclaimState` | 否(回收由外部完成) | 是,经 `q.Preempt` |
| 是否需要队列抢占注解 | 仅当你也想走队列级路径时 | 需要 |

## 测试

- `pkg/test/integration/filterpreemption/` —— 树形插件 `Filter` 试算准入(断言抢占者被准入,而牺牲者的
  `ReclaimState` 保持为 nil)。
- `pkg/test/integration/elasticquotav1alpha1preemption/` —— ElasticQuotaV2 插件的队列级 `q.Preempt`
  (断言当更高优先级单元需要配额时,低优先级牺牲者被设置 `ReclaimState`)。
- `pkg/queue/queuepolicies/schedulingqueuev2/preempt_test.go` —— 牺牲者选择的单元测试。
