package framework

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
)

// isPodTerminal reports whether a pod finished for good and will never need its slot back.
// Only Succeeded counts: a Failed pod may still be retried by the job controller, and a pod that
// merely disappeared from the API is not evidence of completion at all. That distinction is what
// keeps this from repeating the over-admission bug that got reconcilePodDeletion disabled.
func isPodTerminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded
}

// syncReclaimablePods records how many pods of each podSet have completed and shrinks the
// admitted replicas accordingly, which is what actually hands the quota back: the elastic quota
// plugin derives used from the admitted replicas, so no separate quota bookkeeping is involved.
//
// The recorded count only ever grows while the queue unit holds a reservation, mirroring the
// upstream Kueue guarantee, and it never exceeds the admitted replicas.
func syncReclaimablePods(qu *v1alpha1.QueueUnit, terminalByPs map[string]int64) bool {
	if !features.Enabled(features.ReclaimablePods) {
		return false
	}

	updated := false
	for i := range qu.Status.Admissions {
		ad := &qu.Status.Admissions[i]
		terminal := terminalByPs[ad.Name]
		if terminal > ad.Replicas {
			terminal = ad.Replicas
		}

		previous := int64(0)
		idx := -1
		for j := range qu.Status.ReclaimablePods {
			if qu.Status.ReclaimablePods[j].Name == ad.Name {
				idx = j
				previous = int64(qu.Status.ReclaimablePods[j].Count)
				break
			}
		}
		// Monotonic: a pod that completed cannot un-complete, and a transient read that sees
		// fewer pods must not inflate the reservation again.
		if terminal < previous {
			terminal = previous
		}
		if terminal == 0 {
			continue
		}

		if idx < 0 {
			qu.Status.ReclaimablePods = append(qu.Status.ReclaimablePods,
				v1alpha1.ReclaimablePod{Name: ad.Name, Count: int32(terminal)})
			updated = true
		} else if int64(qu.Status.ReclaimablePods[idx].Count) != terminal {
			qu.Status.ReclaimablePods[idx].Count = int32(terminal)
			updated = true
		}
	}
	return updated
}
