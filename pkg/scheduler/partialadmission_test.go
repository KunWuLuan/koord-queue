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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
)

func podSet(name string, count int32, minCount *int32, cpu string) kueue.PodSet {
	return kueue.PodSet{
		Name:     name,
		Count:    count,
		MinCount: minCount,
		Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "main",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
				},
			}},
		}},
	}
}

func TestFindPartialPodSet(t *testing.T) {
	tests := []struct {
		name         string
		podSets      []kueue.PodSet
		wantIdx      int
		wantMinCount int32
	}{
		{
			name:    "no minCount means no partial admission",
			podSets: []kueue.PodSet{podSet("worker", 4, nil, "1")},
			wantIdx: -1,
		},
		{
			name:         "single podset with minCount",
			podSets:      []kueue.PodSet{podSet("worker", 4, ptr.To(int32(2)), "1")},
			wantIdx:      0,
			wantMinCount: 2,
		},
		{
			name: "minCount on the second podset",
			podSets: []kueue.PodSet{
				podSet("master", 1, nil, "1"),
				podSet("worker", 4, ptr.To(int32(1)), "1"),
			},
			wantIdx:      1,
			wantMinCount: 1,
		},
		{
			// Two shrinkable podsets would make the search ambiguous, so it is refused.
			name: "two podsets with minCount are refused",
			podSets: []kueue.PodSet{
				podSet("master", 2, ptr.To(int32(1)), "1"),
				podSet("worker", 4, ptr.To(int32(2)), "1"),
			},
			wantIdx: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qu := &v1alpha1.QueueUnit{Spec: v1alpha1.QueueUnitSpec{PodSets: tt.podSets}}
			idx, minCount := findPartialPodSet(qu)
			if idx != tt.wantIdx {
				t.Fatalf("findPartialPodSet() idx = %d, want %d", idx, tt.wantIdx)
			}
			if idx >= 0 && minCount != tt.wantMinCount {
				t.Errorf("findPartialPodSet() minCount = %d, want %d", minCount, tt.wantMinCount)
			}
		})
	}
}

func TestScaledQueueUnitInfo(t *testing.T) {
	tests := []struct {
		name     string
		podSets  []kueue.PodSet
		idx      int
		count    int32
		wantCPU  string
		wantName string
	}{
		{
			name:     "shrinking the only podset scales its resources down",
			podSets:  []kueue.PodSet{podSet("worker", 4, ptr.To(int32(2)), "1")},
			idx:      0,
			count:    2,
			wantCPU:  "2",
			wantName: "worker",
		},
		{
			// Untouched podsets must keep contributing their full request.
			name: "other podsets keep their full request",
			podSets: []kueue.PodSet{
				podSet("master", 1, nil, "1"),
				podSet("worker", 4, ptr.To(int32(1)), "2"),
			},
			idx:      1,
			count:    1,
			wantCPU:  "3",
			wantName: "worker",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			unitInfo := &framework.QueueUnitInfo{
				Name: "default/qu",
				Unit: &v1alpha1.QueueUnit{
					ObjectMeta: metav1.ObjectMeta{Name: "qu", Namespace: "default"},
					Spec: v1alpha1.QueueUnitSpec{
						PodSets:  tt.podSets,
						Resource: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("99")},
						Request:  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("99")},
					},
				},
			}

			scaled := scaledQueueUnitInfo(unitInfo, tt.idx, tt.count)

			if got := scaled.Unit.Spec.PodSets[tt.idx].Count; got != tt.count {
				t.Errorf("scaled podset count = %d, want %d", got, tt.count)
			}
			if got := scaled.Unit.Spec.PodSets[tt.idx].Name; got != tt.wantName {
				t.Errorf("scaled the wrong podset: %q", got)
			}
			wantCPU := resource.MustParse(tt.wantCPU)
			gotCPU := scaled.Unit.Spec.Resource[corev1.ResourceCPU]
			if gotCPU.Cmp(wantCPU) != 0 {
				t.Errorf("scaled cpu request = %s, want %s", gotCPU.String(), wantCPU.String())
			}
			// Request must track Resource, since different call sites read one or the other.
			gotRequest := scaled.Unit.Spec.Request[corev1.ResourceCPU]
			if gotRequest.Cmp(wantCPU) != 0 {
				t.Errorf("scaled cpu in spec.request = %s, want %s", gotRequest.String(), wantCPU.String())
			}

			// The original must not be mutated: it is still queued at its requested size.
			if got := unitInfo.Unit.Spec.PodSets[tt.idx].Count; got == tt.count {
				t.Errorf("the original queue unit was mutated, count = %d", got)
			}
			originalCPU := unitInfo.Unit.Spec.Resource[corev1.ResourceCPU]
			if originalCPU.Cmp(resource.MustParse("99")) != 0 {
				t.Errorf("the original resources were mutated: %s", originalCPU.String())
			}
		})
	}
}
