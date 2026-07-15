package queuepolicies

import (
	"flag"
	"os"
)

const Priority string = "Priority"
const Block string = "Block"
const Round string = "Round"
const Intelligent string = "Intelligent"

const QueueArgsAnnotationKey string = "koord-queue/queue-args"
const QueuePolicyLabelKey string = "koord-queue/queue-policy"

// QueuePolicyLabelKeyKubeQueue is an accepted alias of QueuePolicyLabelKey. Both the koord-queue and the
// kube-queue key are honored so quotas authored for either flavor select the same queue policy.
const QueuePolicyLabelKeyKubeQueue string = "kube-queue/queue-policy"
const PriorityThresholdAnnotationKey string = "koord-queue/priority-threshold"

// GetQueuePolicyFromLabels returns the requested queue policy from either the koord-queue or the
// kube-queue queue-policy key in the given labels/attributes map. koord-queue/queue-policy takes
// precedence when both are set; an empty string is returned when neither is present.
func GetQueuePolicyFromLabels(labels map[string]string) string {
	if labels == nil {
		return ""
	}
	if v := labels[QueuePolicyLabelKey]; v != "" {
		return v
	}
	return labels[QueuePolicyLabelKeyKubeQueue]
}

var defaultPolicyEnv string
var defaultPolicyCLI string

func init() {
	if os.Getenv("StrictPriority") == "true" {
		defaultPolicyEnv = Block
	} else if os.Getenv("StrictConsistency") == "true" {
		defaultPolicyEnv = Priority
	} else {
		defaultPolicyEnv = Priority
	}
}

func AddCommandLine(fs *flag.FlagSet) {
	fs.StringVar(&defaultPolicyCLI, "default-queue-policy", defaultPolicyEnv, "The policy to use for dequeuing queueunits in the koord-queue.")
}

func GetDefaultQueuePolicy() string {
	return defaultPolicyCLI
}
