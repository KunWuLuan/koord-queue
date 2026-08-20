package plugins

import (
	"testing"

	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/defaultgroup"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/priority"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/resourcequota"
)

// TestNewInTreeRegistryIsConfigDriven verifies the in-tree registry exposes every group plugin
// unconditionally (no QueueGroupPlugin env gating), so KoordQueueConfiguration.plugins selects them.
func TestNewInTreeRegistryIsConfigDriven(t *testing.T) {
	t.Setenv("QueueGroupPlugin", "resourceQuota") // must be ignored now
	r := NewInTreeRegistry()
	for _, name := range []string{
		priority.Name, defaultgroup.Name, resourcequota.Name,
		elasticquotav1alpha1.Name,
	} {
		if _, ok := r[name]; !ok {
			t.Errorf("expected plugin %q registered regardless of QueueGroupPlugin env", name)
		}
	}
}
