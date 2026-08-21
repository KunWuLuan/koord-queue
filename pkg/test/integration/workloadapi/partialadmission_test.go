package workloadapi_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/features"
	eqv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// MR5: a job that declares it can run smaller is admitted at a reduced size when the full request
// does not fit, instead of waiting for the whole quota to free up. The admitted replica count is
// what the job extension later applies to the job, so it must never exceed what fits.
var _ = Describe("QueueUnit partial admission", Ordered, func() {

	const (
		quotaName = "partial-quota"
		queueNS   = "koord-queue"
		unitNS    = "default"
	)

	// unit asks for count replicas of 1 CPU each, accepting as few as minCount.
	unit := func(name string, count, minCount int32) *v1alpha1.QueueUnit {
		one := resource.MustParse("1")
		w := queueunits.MakeQueueUnit(name, unitNS).
			PodSets(kueue.PodSet{
				Name:  name,
				Count: count,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{"cpu": one},
						},
					}},
				}},
			}).
			Resources(map[string]int64{"cpu": int64(count)}).
			Priority(10).
			Labels(map[string]string{elasticquotav1alpha1.QuotaNameLabelKey: quotaName})
		if minCount > 0 {
			w = w.MinCount(name, minCount)
		}
		return w.QueueUnit()
	}

	BeforeAll(func() {
		By("enabling the PartialAdmission feature gate")
		Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
			string(features.PartialAdmission): true,
		})).To(Succeed())

		By("creating the Queue and an ElasticQuota of 4 CPUs")
		_, err := cli.SchedulingV1alpha1().Queues(queueNS).Create(ctx, &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: queueNS},
			Spec:       v1alpha1.QueueSpec{QueuePolicy: "Priority"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		_, err = eqcli.SchedulingV1alpha1().ElasticQuotas("kube-system").Create(ctx, &eqv1alpha1.ElasticQuota{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: "kube-system"},
			Spec: eqv1alpha1.ElasticQuotaSpec{
				Min: corev1.ResourceList{"cpu": resource.MustParse("4")},
				Max: corev1.ResourceList{"cpu": resource.MustParse("4")},
			},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		for _, n := range []string{"partial-holder", "partial-shrinkable", "partial-toobig"} {
			_ = cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, n, metav1.DeleteOptions{})
		}
		Expect(utilfeature.DefaultMutableFeatureGate.SetFromMap(map[string]bool{
			string(features.PartialAdmission): false,
		})).To(Succeed())
	})

	It("should admit a shrinkable queue unit with the replicas that still fit", func() {
		By("filling 3 of the 4 CPUs with a job that cannot shrink")
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, unit("partial-holder", 3, 0), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "partial-holder", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions).To(HaveLen(1))
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(3)))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("asking for 3 more replicas, of which only 1 fits, accepting as few as 1")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, unit("partial-shrinkable", 3, 1), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying minCount survived the apiserver round trip")
		created, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "partial-shrinkable", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(created.Spec.PodSets).To(HaveLen(1))
		Expect(created.Spec.PodSets[0].MinCount).NotTo(BeNil(), "minCount must be persisted by the CRD schema")
		Expect(*created.Spec.PodSets[0].MinCount).To(Equal(int32(1)))

		By("verifying it is admitted with the single replica that fits")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "partial-shrinkable", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions).To(HaveLen(1))
			// One CPU was left, so exactly one replica may run: not three, and not zero.
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
			// The request itself is untouched, so the shortfall stays visible to users.
			g.Expect(qu.Spec.PodSets[0].Count).To(Equal(int32(3)))
		}, 60*time.Second, 500*time.Millisecond).Should(Succeed())

		By("verifying the quota is not oversubscribed")
		Consistently(func(g Gomega) {
			holder, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "partial-holder", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			shrunk, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "partial-shrinkable", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())

			total := int64(0)
			for _, ad := range holder.Status.Admissions {
				total += ad.Replicas
			}
			for _, ad := range shrunk.Status.Admissions {
				total += ad.Replicas
			}
			g.Expect(total).To(BeNumerically("<=", int64(4)), "admitted replicas must fit the 4 CPU quota")
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	It("should leave a queue unit pending when even its minCount does not fit", func() {
		By("asking for 4 replicas with a minCount of 3 while the quota is full")
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, unit("partial-toobig", 4, 3), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Nothing is free, so not even the minimum size can be granted: the unit must wait
		// rather than be admitted at a size that does not fit.
		Consistently(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "partial-toobig", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).NotTo(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions).To(BeEmpty())
		}, 15*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})
