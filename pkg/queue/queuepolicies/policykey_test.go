package queuepolicies

import "testing"

func TestGetQueuePolicyFromLabels(t *testing.T) {
	cases := []struct {
		name string
		m    map[string]string
		want string
	}{
		{"nil", nil, ""},
		{"neither", map[string]string{"other": "x"}, ""},
		{"koord-queue key", map[string]string{QueuePolicyLabelKey: "Block"}, "Block"},
		{"kube-queue key", map[string]string{QueuePolicyLabelKeyKubeQueue: "Intelligent"}, "Intelligent"},
		{"both -> koord-queue wins", map[string]string{QueuePolicyLabelKey: "Priority", QueuePolicyLabelKeyKubeQueue: "Block"}, "Priority"},
		{"empty koord-queue falls back to kube-queue", map[string]string{QueuePolicyLabelKey: "", QueuePolicyLabelKeyKubeQueue: "Block"}, "Block"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := GetQueuePolicyFromLabels(tc.m); got != tc.want {
				t.Fatalf("GetQueuePolicyFromLabels(%v) = %q, want %q", tc.m, got, tc.want)
			}
		})
	}
}
