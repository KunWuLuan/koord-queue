/*
 Copyright 2021 The Koord-Queue Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package scheduler

import (
	"context"

	"github.com/go-logr/logr"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

// findPartialPodSet returns the index of the podSet that opted into partial admission and the
// minimum number of replicas it accepts. Only one podSet may do so, matching the upstream Kueue
// restriction, so the search below stays one-dimensional.
func findPartialPodSet(qu *v1alpha1.QueueUnit) (idx int, minCount int32) {
	found := -1
	for i := range qu.Spec.PodSets {
		if qu.Spec.PodSets[i].MinCount == nil {
			continue
		}
		if found >= 0 {
			// Ambiguous request: refuse to guess which podSet should shrink.
			return -1, 0
		}
		found = i
	}
	if found < 0 {
		return -1, 0
	}
	return found, *qu.Spec.PodSets[found].MinCount
}

// scaledQueueUnitInfo builds a copy of the queue unit whose target podSet is shrunk to count
// replicas, with the requested resources recomputed from the podSet templates so the quota
// plugins see a consistent request.
func scaledQueueUnitInfo(unitInfo *framework.QueueUnitInfo, idx int, count int32) *framework.QueueUnitInfo {
	scaled := unitInfo.Unit.DeepCopy()
	scaled.Spec.PodSets[idx].Count = count

	ads := utils.GetQueueUnitResourceRequirementAds(scaled)
	resources := utils.ConvertFromAdmissionToResource(scaled, ads).ResourceList()
	scaled.Spec.Resource = resources
	scaled.Spec.Request = resources

	copied := *unitInfo
	copied.Unit = scaled
	return &copied
}

// tryPartialAdmission looks for the largest replica count between minCount and the requested
// count that fits the remaining quota. It is attempted before preemption: shrinking a job that
// declared it can run smaller is less disruptive than evicting somebody else's job.
//
// It returns nil when partial admission does not apply or no acceptable size fits, leaving the
// caller on its normal unschedulable path.
func (s *Scheduler) tryPartialAdmission(ctx context.Context, logger logr.Logger,
	unitInfo *framework.QueueUnitInfo) (*framework.QueueUnitInfo, *framework.Status) {
	if !features.Enabled(features.PartialAdmission) {
		return nil, nil
	}

	idx, minCount := findPartialPodSet(unitInfo.Unit)
	if idx < 0 {
		return nil, nil
	}
	full := unitInfo.Unit.Spec.PodSets[idx].Count
	if minCount < 1 || minCount >= full {
		return nil, nil
	}

	// Quota admission is monotonic in the replica count, so a binary search finds the largest
	// feasible size while running the filters O(log n) times instead of once per replica.
	lo, hi := minCount, full-1
	var bestUnit *framework.QueueUnitInfo
	var bestStatus *framework.Status
	for lo <= hi {
		mid := lo + (hi-lo)/2
		candidate := scaledQueueUnitInfo(unitInfo, idx, mid)
		status := s.fw.RunFilterPlugins(ctx, candidate)
		if status.Code() == framework.Success {
			bestUnit, bestStatus = candidate, status
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	if bestUnit == nil {
		logger.V(2).Info("partial admission found no feasible size",
			"podSet", unitInfo.Unit.Spec.PodSets[idx].Name, "minCount", minCount, "count", full)
		return nil, nil
	}

	logger.V(0).Info("admitting the queue unit partially",
		"podSet", bestUnit.Unit.Spec.PodSets[idx].Name,
		"count", bestUnit.Unit.Spec.PodSets[idx].Count, "requested", full, "minCount", minCount)
	return bestUnit, bestStatus
}
