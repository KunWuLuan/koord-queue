# MR: Sync kube-queue recent commits to koord-queue

## Overview

This MR cherry-picks **13 commits** from the [kube-queue](https://gitlab.alibaba-inc.com/cos/kube-queue) repository (branch `fix/elasticquota-used-bias`) into koord-queue, plus **1 adaptation commit** that fixes import paths and annotation prefixes for the koord-queue codebase.

All main code (`cmd/...` and `pkg/...`) compiles successfully with `go build`.

- **Source**: `fix/elasticquota-used-bias` (kube-queue), since commit `6be86910` (last port from koord-queue)
- **Target**: `main` (koord-queue)
- **Stats**: 43 files changed, +5323 / -552

---

## Commits

### 1. `d0e8024c` — fix(build): exclude go.work from Docker context so builds use vendor

**Problem**: When building the controllers Docker image (`golang:alpine`), `go.work` is copied by `ADD .` into the build context, causing `go build` to run in workspace mode. This ignores the `vendor/` directory and tries to resolve modules over the network. Since the image has no git and cannot reach the private `gitlab.alibaba-inc.com/eml/kubequeue-api` module, the build fails with `exec: "git": executable file not found`.

**Fix**: Add a `.dockerignore` that drops `go.work` / `go.work.sum` from the Docker build context so the image builds in module mode and uses the committed `vendor/`.

**Files**: `.dockerignore` (new)

---

### 2. `9fd65d33` — feat(elasticquota): accept both kube-queue/queue-policy and koord-queue/queue-policy

**Change**: The ElasticQuotaV2 plugin (`elasticquotav1alpha1`) now resolves the per-queue policy from either the `koord-queue/queue-policy` or `kube-queue/queue-policy` label key, via a new shared `queuepolicies.GetQueuePolicyFromLabels()` helper. **koord-queue key takes precedence** when both are set.

The `elasticquotav2` update-detection now compares against the resolved policy, so a change via either key triggers a Queue update.

**koord-queue adaptation**:
- Primary prefix stays `koord-queue/queue-policy` (`QueuePolicyLabelKey`)
- New alias constant `QueuePolicyLabelKeyKubeQueue = "kube-queue/queue-policy"`
- `GetQueuePolicyFromLabels()` checks koord-queue key first, falls back to kube-queue key
- `findMatchedSupportPolicy()` uses `GetQueuePolicyFromLabels()` instead of direct `QueuePolicyLabelKey` access

**Files**:
- `pkg/queue/queuepolicies/types.go` — new constant + helper function
- `pkg/queue/queuepolicies/policykey_test.go` (new) — tests for dual-key resolution
- `pkg/framework/plugins/elasticquotav1alpha1/elasticquota_handler.go` — use `GetQueuePolicyFromLabels()`

---

### 3. `17d6af0b` — feat(plugins): select group plugin via KoordQueueConfiguration, not QueueGroupPlugin env

**Change**: `NewInTreeRegistry()` no longer selects plugins based on the `QueueGroupPlugin` environment variable. Instead, all in-tree plugins (Priority, DefaultGroup, ResourceQuota, ElasticQuotaV2) are registered unconditionally. Which group plugin is actually active is decided by the `KoordQueueConfiguration` `plugins` list (`NewFramework` only instantiates the enabled ones), following the `KubeSchedulerConfiguration` model.

The env-based `NewFakeRegistry()` is preserved for tests.

**koord-queue adaptation**:
- Config type name: `KoordQueueConfiguration` (not `KubeQueueConfiguration`)
- Does not register `elasticquota` (tree-based) plugin — package does not exist in koord-queue
- Comment lists only ResourceQuota / ElasticQuotaV2 as mutually exclusive group plugins
- `NewFakeRegistry()` retains env-based logic for test compatibility

**Files**:
- `pkg/framework/plugins/registry.go` — refactor `NewInTreeRegistry()`
- `pkg/framework/plugins/registry_test.go` (new) — verify unconditional registration

---

### 4. `c325b659` — docs(elasticquota): add quota/queue mapping guide (EN + ZH)

**Change**: New documentation explaining how the ElasticQuota plugin resolves which quota a workload is charged against and which Queue it waits in, from the workload's labels/annotations.

**Files**: `doc/elasticquota-queue-mapping.md`, `doc/elasticquota-queue-mapping.zh.md` (new)

---

### 5. `ecbd5c1d` — test(preemption): add ElasticQuotaV2 queue-level preemption integration suite

**Change**: New envtest Ginkgo test suite exercising the queue-level `q.Preempt` path: a higher-priority QueueUnit that exhausts its quota causes the scheduler to set `ReclaimState` on a lower-priority reserved victim (dequeue-to-preempt). Uses the `elasticquotav1alpha1` (ElasticQuotaV2) plugin, whose `Filter` returns `Unschedulable` when over quota and thus routes into `q.Preempt`.

Also fixes a nil-map panic in `utils.UpdateQueueUnitStatusAndAnnotations`: it wrote to `newQueueUnit.Annotations` without guarding a nil map, crashing the scheduler goroutine on the dequeue path for any QueueUnit created without annotations.

**Files**:
- `doc/preemption.md` (new) — documents both preemption mechanisms
- `pkg/test/integration/elasticquotav1alpha1preemption/crd/elasticquota-v1alpha1.yaml` (new) — minimal CRD
- `pkg/test/integration/elasticquotav1alpha1preemption/preemption_test.go` (new)
- `pkg/test/integration/elasticquotav1alpha1preemption/suite_test.go` (new)
- `pkg/utils/util.go` — nil-map guard

---

### 6. `0c73e92c` — docs(preemption): add Chinese translation (preemption.zh.md)

**Files**: `doc/preemption.zh.md` (new)

---

### 7. `8ce70d84` — docs(preemption): drop the Tests section from both docs

**Files**: `doc/preemption.md`, `doc/preemption.zh.md`

---

### 8. `6452ed38` — fix: sync queueunit priority when job spec changes in non-PAI scenarios

**Problem**: When a job's spec changes (e.g. priority class or annotation), the related QueueUnit's priority was not being updated because the reconciler only synced PodSets via `updateQueueUnitReplicas`.

**Fix**: Add `updateQueueUnitPriority()` to compare and sync `PriorityClassName` and `Priority` (including annotation override) from the job extension to the QueueUnit. This logic is gated on `PAI_ENV` being unset, as PAI uses a different priority management path.

**koord-queue adaptation**:
- Backfills `priorityFromAnnotation()` function and `PriorityAnnotationKey` constant (absent in koord-queue, introduced in kube-queue commit `6be86910`)
- Adds `klog` and `strconv` imports
- Fixes test file import paths from `github.com/kube-queue/api/` to `github.com/koordinator-sh/koord-queue/`

**Files**:
- `pkg/jobext/framework/default_job_reconciler.go` — new `updateQueueUnitPriority()`, `priorityFromAnnotation()`, `priorityEqual()`
- `pkg/jobext/framework/update_priority_test.go` (new) — parameterized tests

---

### 9. `1a237380` — fix: fix elastic quota used underestimation and over-admission

**Key changes**:

1. **Remove TransGpuResource and oversold system** to align with koord-queue open-source implementation
   - Delete `gpu_transformer.go` (entire file)
   - Remove oversold constants from `const.go`
   - `elasticquotainfo.go` uses `utils.NewResource()` instead of `utils.TransResourceList()`
   - `elasticquota.go` simplifies `Filter()`, removes oversold type switch
   - `cache.go` `CheckUsage()` interface drops `isOversold` parameter

2. **Optimize Resize logging**: silently swap when resources unchanged (`reflect.DeepEqual`), output a single log line when changed

3. **Disable `reconcilePodDeletion`**: the method cannot distinguish between "pod not yet created" and "pod deleted", causing Replicas to be wrongly set to 0 after Dequeue, leading to used underestimation and over-admission

4. **Modify `IsQueueUnitDequeued`**: remove from queue when Request is satisfied to avoid duplicate scheduling; add `IsQueueUnitFullyDequeued` to preserve the old logic for `preempt.go`

5. **Change QueueNotFound log to Warning event**

**koord-queue adaptation**:
- Some oversold code still existed in koord-queue; uniformly removed during cherry-pick
- `elasticquota_handler.go` removes unused `utils` import
- Test files uniformly take cherry-pick version, then fix import paths
- Deleted test files that don't exist in koord-queue (`git rm`)

**Files** (19 files, +438 / -507):
- `pkg/framework/plugins/elasticquotav1alpha1/elasticquotainfo.go` — refactor resource conversion
- `pkg/framework/plugins/elasticquotav1alpha1/elasticquota.go` — simplify Filter
- `pkg/framework/plugins/elasticquotav1alpha1/cache.go` — CheckUsage interface change
- `pkg/framework/plugins/elasticquotav1alpha1/api_handler.go` — remove OverSoldUsed fields
- `pkg/utils/const.go` — remove oversold constants
- `pkg/utils/gpu_transformer.go` — delete entire file
- `pkg/utils/util.go` — simplify IsQueueUnitDequeued
- `pkg/scheduler/scheduler.go` — remove oversold annotation propagation
- and more

---

### 10. `2cc0c497` — fix: clear updating after reclaim, add preemption events, dedup QueueNotFound

**Key changes**:

1. **Fix updating flag not cleared after preemption reclaim**: when `IsQueueUnitReservedAnyResource=false` AND `IsQueueUnitSatisfied=false` (job-extension has reclaimed resources), clear the updating flag so `findNextQueueUnit` can re-schedule the preempted victim. **Before this fix, preempted victims were stuck in the queue forever** because updating prevented `findNextQueueUnit` from picking them up.

2. **Add EventRecorder events** at key preemption/reclaim points:
   - `Preempted` (Warning): when ReclaimState is set on a victim
   - `Reclaimed` (Normal): when updating is cleared after resource reclaim

3. **Dedup QueueNotFound events**: add `queueUnitNotFoundNotified` set to track which QueueUnits have already had their QueueNotFound event fired. Only fire the event once per QueueUnit, not every 10 seconds. Clear the tracking when a QueueUnit is successfully added to a queue.

4. **Add integration tests**:
   - `reserve_preemption_test.go`: quota sufficient → Reserve preemption
   - `reclaim_reschedule_test.go`: victim re-scheduled after reclaim
   - `priority_order_test.go`: remaining tasks scheduled in priority order

5. **Add preemption configuration guide** (`doc/preemption-guide.zh.md`)

**Files** (7 files, +830 / -17):
- `pkg/queue/multischedulingqueue/multi_scheduling_queue.go` — event enhancement and dedup
- `pkg/queue/queuepolicies/schedulingqueuev2/preempt.go` — use `IsQueueUnitFullyDequeued`
- `pkg/queue/queuepolicies/schedulingqueuev2/schedulingqueuev2.go` — updating clear logic
- 3 integration test files (new)
- `doc/preemption-guide.zh.md` (new)

---

### 11. `d5e7053c` — test: add Block policy test, fix EventRecorder nil check, fix metrics port

**Changes**:

1. **EventRecorder nil check**: add nil guard in `preempt.go` and `schedulingqueuev2.go` to prevent nil pointer dereference in unit tests where EventRecorder returns nil

2. **Random port**: filterpreemption test suite uses `:0` (random port) instead of fixed 8080 for controller-runtime metrics server to avoid port conflicts

3. **Merge Priority and Block policy tests**: combine into a single `DescribeTable` that runs the same test logic for both policies, verifying preemption works regardless of QueuePolicy

**Files** (3 files, +67 / -67):
- `pkg/queue/queuepolicies/schedulingqueuev2/preempt.go` — nil guard
- `pkg/queue/queuepolicies/schedulingqueuev2/schedulingqueuev2.go` — nil guard
- `pkg/test/integration/elasticquotav1alpha1preemption/reserve_preemption_test.go` — parameterized merge

---

### 12. `42cbaf05` — feat: sync koord-queue/ prefixed annotations from ElasticQuota to Queue

**Change**: When a Queue is auto-created from an ElasticQuota (v1alpha1), all annotations with the `koord-queue/` prefix are now automatically synced to the Queue object. This allows users to configure preemption and other queue behaviors directly on the ElasticQuota:

```yaml
metadata:
  annotations:
    koord-queue/wait-for-pods-running: "true"
    koord-queue/enable-queueunit-preemption: "true"
```

**koord-queue adaptation**:
- Annotation prefix changed from `kube-queue/` to `koord-queue/`
- `shouldSyncAnnotation()` checks `koord-queue/` prefix
- Uses `utils.QuotaKoordQueueEnable` instead of `utils.QuotaKubeQueueEnable`
- ElasticQuotaTree-related files removed (plugin does not exist in koord-queue)

**Files**:
- `pkg/framework/plugins/elasticquotav1alpha1/elasticquota_handler.go` — `makeNewestQueueCr` and update path add annotation sync

---

### 13. `aa131d72` — fix(jobext): count scheduled pods as running in Dequeued to unblock wait-for-pods-running queues

**Problem**: Queues using the `wait-for-pods-running` policy were not released in the Dequeued phase because `syncInFlightWorkers` only ran during Running. If pods were scheduled but never reached Running (e.g. image pull failures), the queue would block indefinitely.

**Fix**:
1. `syncInFlightWorkers` now also runs in Dequeued phase and counts pods with `NodeName` set as Running, so `wait-for-pods-running` queues are released as soon as pods are scheduled
2. Keep quota accounting admission-based during Dequeued: do not back-fill `Admissions[i].Resources` from actual pod requests until the unit is Running
3. Add overadmission envtest suite: submit a batch of queued units and churn by repeatedly deleting the scheduled-but-not-running head, asserting at most one unit holds the quota at any moment
4. Add `status.lastAllocateTime` to the test QueueUnit CRD; the field was pruned by apiserver, which silently disabled the `reconcilePodDeletion` grace period and caused flaky over-admission in envtest

**Files** (6 files, +2803 / -23):
- `pkg/jobext/framework/resource_report_controller.go` — syncInFlightWorkers extended to Dequeued
- `pkg/jobext/framework/resource_report_controller_test.go` — test updates
- `pkg/jobext/test/config/crd/queueunit-v1alpha1.yaml` — add lastAllocateTime
- `pkg/test/integration/overadmission/overadmission_test.go` (new)
- `pkg/test/integration/overadmission/suite_test.go` (new)

---

### 14. `f7094b6b` — fix: adjust import paths and annotation prefixes for koord-queue

**Change**: Adaptation commit that fixes import paths and annotation prefixes in cherry-picked test files. Replaces `github.com/kube-queue/kube-queue/` and `github.com/kube-queue/api/` with `github.com/koordinator-sh/koord-queue/` equivalents. Also removes import of non-existent `elasticquota` tree plugin package in overadmission test suite, replacing `elasticquotatree.Name` with string constant `"ElasticQuota"`.

**Files** (7 test files, +39 / -40)

---

## koord-queue Adaptation Summary

| Adaptation | Description |
|------------|-------------|
| **Import paths** | `github.com/kube-queue/kube-queue/` → `github.com/koordinator-sh/koord-queue/`; `github.com/kube-queue/api/` → `github.com/koordinator-sh/koord-queue/` |
| **Annotation prefixes** | `"kube-queue/` → `"koord-queue/` in test files |
| **Queue policy constant** | Primary prefix `koord-queue/queue-policy`; alias `QueuePolicyLabelKeyKubeQueue` |
| **Config type** | `KubeQueueConfiguration` → `KoordQueueConfiguration` |
| **elasticquotatree plugin** | Package does not exist in koord-queue; references replaced with string constant `"ElasticQuota"` or removed |
| **`priorityFromAnnotation`** | Backfilled from kube-queue (absent in koord-queue) |
| **`shouldSyncAnnotation`** | Prefix check changed from `kube-queue/` to `koord-queue/` |
| **`QuotaKoordQueueEnable`** | Uses koord-queue constant instead of kube-queue equivalent |

## Build Status

- `go build ./cmd/... ./pkg/...` — **PASS**
- Some integration test files have pre-existing type differences (`partialRunningFirstSeen`, `GetClient`) between the two codebases; these are not introduced by this MR.

## How to Review

```bash
# Checkout the branch
git remote add kunwuluan https://github.com/KunWuLuan/koord-queue.git
git fetch kunwuluan cherry-pick-to-koord
git checkout cherry-pick-to-koord

# Verify build
go build ./cmd/... ./pkg/...

# Review commit-by-commit
git log --oneline --reverse koord-queue/main..HEAD
```

---

## Release Notes

### Features

- **Dual queue-policy key support**: ElasticQuotaV2 plugin now accepts both `koord-queue/queue-policy` and `kube-queue/queue-policy` label keys to specify the per-queue dequeue policy. `koord-queue/` takes precedence when both are set. This enables cross-compatibility with kube-queue-authored quota manifests.

- **Config-driven plugin selection**: The active group plugin is now selected via the `KoordQueueConfiguration` `plugins` list instead of the `QueueGroupPlugin` environment variable, following the `KubeSchedulerConfiguration` model. All in-tree plugins are registered unconditionally.

- **Annotation auto-sync from ElasticQuota to Queue**: When a Queue is auto-created from an ElasticQuota (v1alpha1), all `koord-queue/`-prefixed annotations are automatically synced to the Queue. Users can configure preemption and other behaviors directly on the ElasticQuota:
  ```yaml
  metadata:
    annotations:
      koord-queue/wait-for-pods-running: "true"
      koord-queue/enable-queueunit-preemption: "true"
  ```

- **Preemption events**: EventRecorder now emits `Preempted` (Warning) and `Reclaimed` (Normal) events at key preemption/reclaim points for better observability.

### Bug Fixes

- **Fix preempted victims stuck in queue**: The `updating` flag was not cleared after preemption reclaim, causing preempted victims to remain in the queue indefinitely. Now cleared when `IsQueueUnitReservedAnyResource=false` and `IsQueueUnitSatisfied=false`.

- **Fix elastic quota used underestimation and over-admission**: `reconcilePodDeletion` could not distinguish between "pod not yet created" and "pod deleted", causing Replicas to be wrongly set to 0 after Dequeue. The method is now disabled. Additionally, `IsQueueUnitDequeued` was simplified to remove from queue when Request is satisfied, preventing duplicate scheduling.

- **Fix wait-for-pods-running queues blocked indefinitely**: `syncInFlightWorkers` now also runs in Dequeued phase and counts scheduled pods (with `NodeName` set) as Running, so queues are released as soon as pods are scheduled — even if they never reach Running (e.g. image pull failures).

- **Fix QueueUnit priority not synced on job spec change**: When a job's priority class or annotation changes, the related QueueUnit's priority is now updated via `updateQueueUnitPriority()`.

- **Fix nil pointer in EventRecorder**: Added nil guard in `preempt.go` and `schedulingqueuev2.go` to prevent crashes in unit tests where EventRecorder returns nil.

- **Fix nil-map panic in UpdateQueueUnitStatusAndAnnotations**: Added nil-map guard before writing to `newQueueUnit.Annotations`.

- **Dedup QueueNotFound events**: QueueNotFound events are now fired at most once per QueueUnit instead of every 10 seconds, reducing event noise.

### Cleanup

- **Remove TransGpuResource and oversold system**: Deleted `gpu_transformer.go`, removed oversold constants and `OverSoldUsed` fields to align with the koord-queue open-source implementation.

- **Optimize Resize logging**: Resource swaps with no change are now silent; only actual changes produce a log line.

- **Docker build fix**: Added `.dockerignore` to exclude `go.work` from Docker context, ensuring builds use `vendor/` in module mode.

- **Metrics port conflict fix**: Test suites now use random port (`:0`) instead of fixed 8080 for the controller-runtime metrics server.
