package elasticquotav1alpha1preemption_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kube-queue/api/pkg/apis/scheduling/v1alpha1"
	eqv1alpha1 "github.com/kube-queue/kube-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/kube-queue/kube-queue/pkg/framework/plugins/elasticquotav1alpha1"
	"github.com/kube-queue/kube-queue/pkg/queue/queuepolicies/schedulingqueuev2"
	"github.com/kube-queue/kube-queue/pkg/test/testutils/queueunits"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// This suite exercises the Reserve-level preemption path:
// when quota is sufficient and Filter passes, but waitPodsRunning is enabled
// and a lower-priority unit occupies the assumed set, Reserve() preempts the
// victim by setting ReclaimState — without going through q.Preempt().
var _ = Describe("ElasticQuotaV2 Reserve-level preemption (quota sufficient)", Ordered, func() {

	const (
		quotaName = "reserve-preempt-quota"
		queueNS   = "kube-queue"
		unitNS    = "default"
	)

	// preemptibleUnit builds a QueueUnit routed to quotaName, requesting cpu and carrying a named
	// PodSet so it acquires a reclaimable admission once dequeued.
	preemptibleUnit := func(name string, priority int32, cpu string) *v1alpha1.QueueUnit {
		cpuQty := resource.MustParse(cpu)
		qu := queueunits.MakeQueueUnit(name, unitNS).
			PodSets(kueue.PodSet{
				Name:  name,
				Count: 1,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{"cpu": cpuQty},
						},
					}},
				}},
			}).
			Resources(map[string]int64{"cpu": cpuQty.Value()}).
			Priority(priority).
			Labels(map[string]string{
				elasticquotav1alpha1.QuotaNameLabelKey:          quotaName,
				"quota.scheduling.alibabacloud.com/preemptible": "true",
			}).
			QueueUnit()
		return qu
	}

	BeforeAll(func() {
		By("pre-creating a preemption-enabled Queue with wait-for-pods-running")
		_, err := cli.SchedulingV1alpha1().Queues(queueNS).Create(ctx, &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name:      quotaName,
				Namespace: queueNS,
				Annotations: map[string]string{
					schedulingqueuev2.WaitForPodsRunningAnnotation: "true",
					schedulingqueuev2.EnableQueueUnitPreemption:    "true",
				},
			},
			Spec: v1alpha1.QueueSpec{QueuePolicy: "Priority"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("creating the ElasticQuota (Min == Max == cpu:4) so both units fit within quota")
		_, err = eqcli.SchedulingV1alpha1().ElasticQuotas("kube-system").Create(ctx, &eqv1alpha1.ElasticQuota{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: "kube-system"},
			Spec: eqv1alpha1.ElasticQuotaSpec{
				Min: corev1.ResourceList{"cpu": resource.MustParse("4")},
				Max: corev1.ResourceList{"cpu": resource.MustParse("4")},
			},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying the preemption annotations survived the plugin's Queue reconcile")
		Eventually(func(g Gomega) {
			q, err := cli.SchedulingV1alpha1().Queues(queueNS).Get(ctx, quotaName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(q.Annotations[schedulingqueuev2.WaitForPodsRunningAnnotation]).To(Equal("true"))
			g.Expect(q.Annotations[schedulingqueuev2.EnableQueueUnitPreemption]).To(Equal("true"))
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	AfterAll(func() {
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "reserve-victim", metav1.DeleteOptions{})
		cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, "reserve-preemptor", metav1.DeleteOptions{})
	})

	It("should preempt a lower-priority victim in Reserve() when quota is sufficient but waitPodsRunning blocks", func() {
		By("creating the low-priority victim and waiting for it to be Dequeued with a reserved admission")
		_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("reserve-victim", 10, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reserve-victim", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued))
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
			g.Expect(qu.Status.Admissions[0].ReclaimState).To(BeNil(), "victim not preempted yet")
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("creating the high-priority preemptor whose request fits within quota (Filter passes -> Reserve preempts)")
		_, err = cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, preemptibleUnit("reserve-preemptor", 100, "1"), metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying the victim's admission gets ReclaimState set (preempted in Reserve)")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reserve-victim", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].ReclaimState).NotTo(BeNil(),
				"a higher-priority unit should have caused the victim to be marked for reclamation in Reserve")
			g.Expect(qu.Status.Admissions[0].ReclaimState.Replicas).To(Equal(int64(1)))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("verifying the preemptor is eventually Dequeued (victim removed from assumed after ReclaimState)")
		Eventually(func(g Gomega) {
			qu, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, "reserve-preemptor", metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			// After the victim's ReclaimState is set, its effective replicas become 0,
			// so IsQueueUnitReservedAnyResource returns false and the victim is removed
			// from assumed. In the next cycle, len(assumed)==0 and Reserve passes,
			// allowing the preemptor to be Dequeued successfully.
			g.Expect(qu.Status.Phase).To(Equal(v1alpha1.Dequeued),
				"preemptor should be Dequeued after Reserve-level preemption, got phase: %s, msg: %s", qu.Status.Phase, qu.Status.Message)
			g.Expect(qu.Status.Admissions).NotTo(BeEmpty())
			g.Expect(qu.Status.Admissions[0].Replicas).To(Equal(int64(1)))
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	})
})
