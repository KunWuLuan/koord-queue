# How the ElasticQuota Plugin Determines the Queue for a Workload

This guide explains how kube-queue decides **which quota** and **which queue** a workload lands in
when it runs with the **ElasticQuota** grouping plugin (the tree-based quota plugin).

> This is the `ElasticQuota` plugin, selected with `QueueGroupPlugin=elasticquota`. For how the
> *dequeue ordering* of each queue is configured, see
> [Setting the Queue Policy for the ElasticQuota Plugin](./elasticquota-queue-policy.md).

## Overview

When you submit a workload (a `Job`, `PyTorchJob`, `MPIJob`, `TFJob`, `RayJob`, and so on), kube-queue
generates a `QueueUnit` that represents that workload in the queueing system. The workload's
**labels and annotations are copied verbatim onto the `QueueUnit`**. The ElasticQuota plugin then
reads those labels and annotations to resolve two separate things:

1. **The quota** — which node of the `ElasticQuotaTree` the workload is charged against.
2. **The queue** — which `Queue` resource the workload actually waits in.

These are resolved independently, and understanding the split is the key to controlling placement.

## Step 1 — Determining the quota

The plugin picks the quota node in this order:

1. **Explicit quota label.** If the workload carries one of the following labels (and the
   `ElasticQuota` feature gate is enabled, which it is by default), the label value names the quota
   node directly:

   ```yaml
   metadata:
     labels:
       quota.scheduling.alibabacloud.com/name: <quota-name>
       # or, equivalently:
       quota.scheduling.koordinator.sh/name: <quota-name>
   ```

   If both labels are present, `quota.scheduling.alibabacloud.com/name` takes precedence.

2. **Namespace mapping (default).** If neither label is present, the quota is resolved from the
   workload's **namespace**. Each namespace is mapped to a quota node in the `ElasticQuotaTree`, so a
   workload with no quota label is automatically charged against the quota that owns its namespace.

If neither path resolves to a quota node, the workload is rejected with an error indicating that it
does not belong to any elastic quota.

### Optional availability check

An optional, off-by-default safeguard (`ElasticQuotaTreeCheckAvailableQuota`) additionally requires
that the quota named by the label is listed as **available in the workload's namespace**. When this
check is enabled and the named quota is not available in that namespace, the workload is rejected
even though the quota node exists. When the check is disabled (the default), any existing quota named
by the label is accepted.

## Step 2 — Determining the queue

Once the quota is known, the plugin maps the workload to a concrete `Queue` resource (queues live in
the `kube-queue` namespace):

1. **Explicit queue annotation.** If the workload carries the queue-name annotation, that value names
   the target queue:

   ```yaml
   metadata:
     annotations:
       kube-queue/queue-name: <queue-name>
   ```

   The named `Queue` must exist, and the quota resolved in Step 1 must be **available in that queue**.
   If the quota is not available in the queue, the workload is rejected with an error to that effect.

2. **Automatic queue resolution (default).** If the annotation is absent or empty, behavior depends
   on the `ElasticQuotaTreeDecoupledQueue` feature gate:

   - **Enabled (the default):** the plugin automatically routes the workload to the `Queue` it
     created for that quota. The plugin maintains one auto-created queue per quota node and looks it
     up by the quota's full name, so no annotation is needed in the common case.
   - **Disabled:** the workload is rejected with an error indicating that the queue name must be set
     on the queue unit — i.e. you must supply the `kube-queue/queue-name` annotation yourself.

## Feature gate defaults

| Feature gate | Default | Effect |
|--------------|---------|--------|
| `ElasticQuota` | Enabled | Honors the explicit quota label; otherwise quota is namespace-derived. |
| `ElasticQuotaTreeDecoupledQueue` | Enabled | Auto-routes to the quota's managed queue when no queue annotation is set. |
| `ElasticQuotaTreeCheckAvailableQuota` | Disabled | When on, the labeled quota must be available in the workload's namespace. |

## Practical guidance

With the default feature gates, most workloads need **no queueing-specific metadata at all**:

- **Rely on the namespace.** Submit the workload in a namespace that maps to the intended quota. The
  plugin derives the quota from the namespace and routes the workload to that quota's auto-created
  queue.
- **Target a specific quota.** Add the `quota.scheduling.alibabacloud.com/name` label to charge the
  workload against a specific quota node regardless of its namespace. The plugin still routes it to
  that quota's managed queue automatically.
- **Target a specific queue.** Add the `kube-queue/queue-name` annotation only when you want to point
  a workload at a particular, hand-managed queue. Ensure the resolved quota is available in that
  queue, or the workload will be rejected.

## Summary

- The workload's labels and annotations are copied onto its `QueueUnit`; the plugin reads them from
  there.
- **Quota** is chosen from the `quota.scheduling.alibabacloud.com/name` /
  `quota.scheduling.koordinator.sh/name` label, falling back to the workload's namespace.
- **Queue** is chosen from the `kube-queue/queue-name` annotation, falling back (when
  `ElasticQuotaTreeDecoupledQueue` is enabled) to the queue auto-created for the resolved quota.
- In a default deployment, submitting a workload into the right namespace — or labeling it with the
  quota name — is enough; the queue is resolved automatically.
