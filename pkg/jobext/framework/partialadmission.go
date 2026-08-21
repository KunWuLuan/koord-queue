package framework

import (
	"context"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
)

// applyPartialAdmission resizes the job to the replica count the scheduler actually granted,
// which may be smaller than requested when the podSet declared a minCount. This has to happen
// before the job is resumed: otherwise the job would create its full set of pods and consume more
// than was admitted, which is exactly the over-admission this feature must not cause.
//
// Job types that cannot express a resize simply do not implement the capability, in which case
// nothing happens here and the job runs at its requested size.
func (d *GenericJobReconciler) applyPartialAdmission(ctx context.Context, log logr.Logger, handle JobHandle,
	object client.Object, queueUnit *v1alpha1.QueueUnit) (updated bool, err error) {
	if !features.Enabled(features.PartialAdmission) {
		return false, nil
	}
	resizer, ok := handle.genericJobExtension.(PartialAdmissionJobExtension)
	if !ok {
		return false, nil
	}

	for _, ad := range queueUnit.Status.Admissions {
		requested, found := requestedCount(queueUnit, ad.Name)
		if !found || int64(requested) <= ad.Replicas {
			continue
		}
		changed, err := resizer.SetReplicas(ctx, object, ad.Name, int32(ad.Replicas))
		if err != nil {
			log.V(0).Info("cannot run the job partially", "podSet", ad.Name, "reason", err.Error())
			return false, nil
		}
		if !changed {
			continue
		}
		log.V(0).Info("resizing the job to its admitted size",
			"podSet", ad.Name, "requested", requested, "admitted", ad.Replicas)
		return true, d.client.Update(ctx, object)
	}
	return false, nil
}

// requestedCount returns the replica count the queue unit asked for in the given podSet.
func requestedCount(qu *v1alpha1.QueueUnit, podSetName string) (int32, bool) {
	for i := range qu.Spec.PodSets {
		if qu.Spec.PodSets[i].Name == podSetName {
			return qu.Spec.PodSets[i].Count, true
		}
	}
	return 0, false
}
