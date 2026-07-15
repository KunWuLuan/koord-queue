# Preemption in kube-queue

This document explains how kube-queue preempts already-dequeued (reserved) work so that a
higher-priority QueueUnit can be admitted when its quota is exhausted — i.e. how the queue
**dequeues lower-priority tasks in order to preempt them**.

## Concepts

- **QueueUnit** — the unit of admission. A job-extension creates one QueueUnit per managed workload.
  While it is waiting it is *enqueued*; once the scheduler admits it, it is *reserved* / *dequeued*
  and the job-extension is allowed to create the workload's pods.
- **Reserved / assumed** — a dequeued QueueUnit still occupies quota. The queue keeps it in an
  in-memory `assumed` set so it can be considered as a preemption victim until its pods are actually
  running.
- **Victim** — a lower-priority reserved QueueUnit whose resources are reclaimed to make room.
- **Preemptor** — the higher-priority QueueUnit that cannot fit within quota and triggers preemption.
- **`ReclaimState`** — a field on a QueueUnit admission (`Status.Admissions[i].ReclaimState`). Setting
  it is the concrete preemption signal: it tells the job-extension how many replicas of that podset
  to reclaim (delete). This is the only place preemption actually reclaims resources.

## Two mechanisms

kube-queue has two distinct preemption paths that operate at different layers. Which one applies
depends on the grouping plugin in use.

### 1. Quota-plugin admission (tree ElasticQuota plugin)

The tree-based `ElasticQuota` plugin makes preemption decisions **inside its `Filter`**. When a
preemptible high-priority QueueUnit would exceed a quota's `Max`, the plugin performs a *dry-run*
(`canPreemptVictims`): it checks whether reclaiming lower-priority preemptible reserved units in the
same quota would free enough room. If so, `Filter` returns **Success** and the preemptor is admitted
immediately — **without** the scheduler setting `ReclaimState`. The victims' resources are freed
later, out of band, when their pods are removed and the job-extension reports the reduced usage.

A non-preemptible incoming job is handled specially: the plugin does not check `Max` for it and, when
the quota carries the attribute `preempt-policy: PreemptLowerPriority`, subtracts the reclaimable
lower-priority preemptible usage so the job can be admitted down to `Min`.

So for the tree plugin, "dequeue-to-preempt when preemptible" means: *the high-priority job is
admitted (dequeued) even though the quota is full, because lower-priority preemptible victims exist
that can be reclaimed.*

### 2. Queue-level preemption (`q.Preempt` → `ReclaimState`)

The scheduler calls `q.Preempt` **only when `Filter` returns a non-Success status**
(`pkg/scheduler/scheduler.go`, the schedule loop). This is the path that actually writes
`ReclaimState`:

1. `q.Preempt` (`pkg/queue/queuepolicies/schedulingqueuev2/preempt.go`) is gated by two queue
   annotations (see below). If either is off, it returns an error and no preemption happens.
2. `dryRunPreemption` scans the queue's `assumed` set, sorted by priority. Starting from the lowest
   priority, it selects victims that are **lower priority than the preemptor** and have reclaimable
   resources (`utils.GetResourcesCanReclaim`: an admission whose `Replicas > Running` and that is not
   already being reclaimed), until the freed resources cover the preemptor's request.
3. `preemptQueueUnits` sets `Status.Admissions[i].ReclaimState.Replicas` on each selected victim
   (skipping any that is already fully dequeued or reserves nothing) and updates its status with
   `"Waiting job extension to reclaim resources."`.

This path is the primary preemption mechanism for the **ElasticQuotaV2 (`elasticquotav1alpha1`)**
plugin, whose `Filter` has no in-Filter preemption dry-run: it simply returns **Unschedulable** when
the quota is exhausted, which routes the scheduler straight into `q.Preempt`.

> Note: the preemptor is not blocked while victims drain. It is requeued and retried; once the
> job-extension reclaims the victims' pods and reports the freed usage, a later scheduling cycle
> admits the preemptor.

## Enabling preemption

### Queue annotations (required for `q.Preempt`)

Both must be `"true"` on the `Queue` object for queue-level preemption to run:

| Annotation | Effect |
| --- | --- |
| `kube-queue/wait-for-pods-running` | Keeps reserved-but-not-running units in the `assumed` set so they can be considered as victims. Preemption is disabled without it. |
| `kube-queue/enable-queueunit-preemption` | Master switch for `q.Preempt`. |

These are read when the queue is built and re-read on queue updates
(`pkg/queue/queuepolicies/schedulingqueuev2/schedulingqueuev2.go`). The tree ElasticQuota plugin does
not add these to the queues it auto-creates, so they must be set explicitly on the `Queue` when the
queue-level path is desired.

### Job preemptibility label (tree plugin dry-run)

Whether a job is a valid victim/preemptor in the tree plugin's `Filter` dry-run is determined by
`util.IsJobPreemptible`:

| Label on the QueueUnit | Value |
| --- | --- |
| `quota.scheduling.alibabacloud.com/preemptible` | `"true"` / `"false"` |
| `quota.scheduling.koordinator.sh/preemptible` | `"true"` / `"false"` |

If neither is set, the global default `--defaultPreemptible` (default `true`) applies. The
queue-level `dryRunPreemption` selects victims by priority and reclaimability and does **not** require
this label.

### Quota attribute (tree plugin, non-preemptible incoming)

`preempt-policy: PreemptLowerPriority` in the `ElasticQuota` attributes lets a *non-preemptible*
incoming job reclaim lower-priority preemptible reserved jobs down to the quota's `Min`.

### Priority

Priority comes from `QueueUnit.Spec.Priority`. Higher priority sorts first; a preemptor must be higher
priority than a victim for the victim to be eligible.

## End-to-end flow (queue-level path)

```
higher-priority QueueUnit created
  → scheduler pops it (highest priority first)
  → RunFilterPlugins
      ElasticQuotaV2.Filter: quota exhausted → Unschedulable
  → status != Success → q.Preempt(preemptor)
      wait-for-pods-running == true  AND  enable-queueunit-preemption == true
      dryRunPreemption: pick lower-priority reserved victims with reclaimable admissions
      preemptQueueUnits: set victim Status.Admissions[i].ReclaimState.Replicas
  → preemptor requeued and retried
  → job-extension observes ReclaimState, deletes that many victim pods,
      reports reduced usage → quota frees up
  → next scheduling cycle admits the preemptor
```

## Per-plugin summary

| | Tree `ElasticQuota` | `ElasticQuotaV2` (`elasticquotav1alpha1`) |
| --- | --- | --- |
| Preemption decision | In-`Filter` dry-run (`canPreemptVictims`) | None in Filter |
| On quota full (preemptible) | `Filter` Success, preemptor admitted, no `ReclaimState` | `Filter` Unschedulable |
| Sets `ReclaimState` | No (reclaim is external) | Yes, via `q.Preempt` |
| Needs queue preemption annotations | Only if you want the queue-level path too | Yes |
