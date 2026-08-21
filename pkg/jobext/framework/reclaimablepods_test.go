package framework

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
)

func TestIsPodTerminal(t *testing.T) {
	tests := []struct {
		name  string
		phase corev1.PodPhase
		want  bool
	}{
		{name: "succeeded pods are done for good", phase: corev1.PodSucceeded, want: true},
		// A failed pod may still be retried by the job controller, so its slot is not free.
		{name: "failed pods may be retried", phase: corev1.PodFailed, want: false},
		{name: "running pods still need their slot", phase: corev1.PodRunning, want: false},
		{name: "pending pods still need their slot", phase: corev1.PodPending, want: false},
		{name: "unknown phase is not treated as complete", phase: corev1.PodUnknown, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Status: corev1.PodStatus{Phase: tt.phase}}
			if got := isPodTerminal(pod); got != tt.want {
				t.Errorf("isPodTerminal(%s) = %v, want %v", tt.phase, got, tt.want)
			}
		})
	}
}

func TestSyncReclaimablePods(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ReclaimablePods, true)

	tests := []struct {
		name        string
		admissions  []v1alpha1.Admission
		existing    []v1alpha1.ReclaimablePod
		terminal    map[string]int64
		wantUpdated bool
		wantCounts  map[string]int32
	}{
		{
			name:        "no completed pods records nothing",
			admissions:  []v1alpha1.Admission{{Name: "worker", Replicas: 3}},
			terminal:    map[string]int64{},
			wantUpdated: false,
			wantCounts:  map[string]int32{},
		},
		{
			name:        "records completed pods per podset",
			admissions:  []v1alpha1.Admission{{Name: "worker", Replicas: 3}, {Name: "master", Replicas: 1}},
			terminal:    map[string]int64{"worker": 2, "master": 1},
			wantUpdated: true,
			wantCounts:  map[string]int32{"worker": 2, "master": 1},
		},
		{
			// A transient read that sees fewer completed pods must not give the reservation back.
			name:        "count never decreases",
			admissions:  []v1alpha1.Admission{{Name: "worker", Replicas: 3}},
			existing:    []v1alpha1.ReclaimablePod{{Name: "worker", Count: 2}},
			terminal:    map[string]int64{"worker": 1},
			wantUpdated: false,
			wantCounts:  map[string]int32{"worker": 2},
		},
		{
			name:        "count grows as more pods complete",
			admissions:  []v1alpha1.Admission{{Name: "worker", Replicas: 3}},
			existing:    []v1alpha1.ReclaimablePod{{Name: "worker", Count: 1}},
			terminal:    map[string]int64{"worker": 3},
			wantUpdated: true,
			wantCounts:  map[string]int32{"worker": 3},
		},
		{
			// Never claim back more than was admitted in the first place.
			name:        "count is capped at the admitted replicas",
			admissions:  []v1alpha1.Admission{{Name: "worker", Replicas: 2}},
			terminal:    map[string]int64{"worker": 5},
			wantUpdated: true,
			wantCounts:  map[string]int32{"worker": 2},
		},
		{
			name:        "podsets without an admission are ignored",
			admissions:  []v1alpha1.Admission{{Name: "worker", Replicas: 2}},
			terminal:    map[string]int64{"ghost": 3},
			wantUpdated: false,
			wantCounts:  map[string]int32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qu := &v1alpha1.QueueUnit{
				ObjectMeta: metav1.ObjectMeta{Name: "qu", Namespace: "default"},
				Status: v1alpha1.QueueUnitStatus{
					Admissions:      tt.admissions,
					ReclaimablePods: tt.existing,
				},
			}

			if got := syncReclaimablePods(qu, tt.terminal); got != tt.wantUpdated {
				t.Errorf("syncReclaimablePods() updated = %v, want %v", got, tt.wantUpdated)
			}

			got := map[string]int32{}
			for _, rp := range qu.Status.ReclaimablePods {
				got[rp.Name] = rp.Count
			}
			if len(got) != len(tt.wantCounts) {
				t.Fatalf("reclaimablePods = %+v, want %+v", got, tt.wantCounts)
			}
			for name, want := range tt.wantCounts {
				if got[name] != want {
					t.Errorf("reclaimablePods[%q] = %d, want %d", name, got[name], want)
				}
			}
		})
	}
}

func TestSyncReclaimablePodsFeatureGateDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ReclaimablePods, false)

	qu := &v1alpha1.QueueUnit{Status: v1alpha1.QueueUnitStatus{
		Admissions: []v1alpha1.Admission{{Name: "worker", Replicas: 3}},
	}}
	if syncReclaimablePods(qu, map[string]int64{"worker": 2}) {
		t.Errorf("syncReclaimablePods() reported a change while the gate is disabled")
	}
	if len(qu.Status.ReclaimablePods) != 0 {
		t.Errorf("reclaimablePods = %+v, want empty while the gate is disabled", qu.Status.ReclaimablePods)
	}
}
