package reclaimablepods

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	koordinatorschedulerv1alpha1 "github.com/koordinator-sh/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/framework"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/handles"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
)

// This suite runs the real ResourceReporter so the reclaimablePods bookkeeping is exercised
// against real pods: the reporter is the only component allowed to shrink an admitted
// reservation, and getting that wrong is what caused over-admission in the past.
var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
)

func TestReclaimablePods(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ReclaimablePods Suite")
}

func getFirstFoundEnvTestBinaryDir() string {
	if assets := os.Getenv("KUBEBUILDER_ASSETS"); assets != "" {
		return assets
	}
	basePath := filepath.Join("..", "..", "..", "..", "..", "bin", "k8s")
	if entries, err := os.ReadDir(basePath); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			subPath := filepath.Join(basePath, entry.Name())
			subEntries, err := os.ReadDir(subPath)
			if err != nil {
				continue
			}
			for _, subEntry := range subEntries {
				if subEntry.IsDir() {
					return filepath.Join(subPath, subEntry.Name())
				}
			}
		}
	}
	home, _ := os.UserHomeDir()
	sysPath := filepath.Join(home, "Library", "Application Support", "io.kubebuilder.envtest", "k8s")
	if entries, err := os.ReadDir(sysPath); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				return filepath.Join(sysPath, entry.Name())
			}
		}
	}
	return ""
}

var _ = BeforeSuite(func() {
	os.Setenv("TESTENV", "true")
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("enabling the ReclaimablePods feature gate")
	Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
		string(features.ReclaimablePods): true,
	})).To(Succeed())

	ctx, cancel = context.WithCancel(context.TODO())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	testEnv.BinaryAssetsDirectory = getFirstFoundEnvTestBinaryDir()
	os.Setenv("KUBEBUILDER_ASSETS", getFirstFoundEnvTestBinaryDir())

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	k8sManager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		Cache:  cache.Options{},
	})
	Expect(err).ToNot(HaveOccurred())
	k8sClient = k8sManager.GetClient()
	Expect(k8sClient).NotTo(BeNil())

	Expect(v1alpha1.AddToScheme(k8sManager.GetScheme())).To(Succeed())
	Expect(koordinatorschedulerv1alpha1.AddToScheme(k8sManager.GetScheme())).To(Succeed())

	// The reporter finds a queue unit's pods through these indexes, exactly as the production
	// controller sets them up in cmd/controllers.
	Expect(k8sManager.GetCache().IndexField(ctx, &corev1.Pod{}, util.PodsByOwnersCacheFields, func(o client.Object) []string {
		p, ok := o.(*corev1.Pod)
		if !ok {
			return nil
		}
		res := []string{}
		for _, owner := range p.OwnerReferences {
			res = append(res, fmt.Sprintf("%v/%v", owner.Kind, owner.Name))
		}
		return res
	})).To(Succeed())
	Expect(k8sManager.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, util.RelatedQueueUnitCacheFields, func(o client.Object) []string {
		pod, ok := o.(*corev1.Pod)
		if !ok {
			return []string{}
		}
		return []string{pod.Annotations[util.RelatedQueueUnitAnnoKey]}
	})).To(Succeed())

	jobHandle := handles.NewJobReconciler(k8sManager.GetClient(), k8sManager.GetConfig(), k8sManager.GetScheme(), true, "")
	reporter := framework.NewResourceReporter(k8sManager.GetClient(), k8sManager.GetScheme(), jobHandle)
	Expect(reporter.SetupWithManager(k8sManager, 1, 100)).ToNot(HaveOccurred())

	go func() {
		defer GinkgoRecover()
		Expect(k8sManager.Start(ctx)).ToNot(HaveOccurred(), "failed to run manager")
	}()
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
		string(features.ReclaimablePods): false,
	})).To(Succeed())
	if testEnv != nil {
		if err := testEnv.Stop(); err != nil {
			GinkgoWriter.Printf("warning: failed to stop test environment cleanly: %v\n", err)
		}
	}
})
