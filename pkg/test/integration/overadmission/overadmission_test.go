package overadmission_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	eqv1alpha1 "github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework/plugins/elasticquotav1alpha1"
	jobextutil "github.com/koordinator-sh/koord-queue/pkg/jobext/util"
	"github.com/koordinator-sh/koord-queue/pkg/queue/queuepolicies/schedulingqueuev2"
	"github.com/koordinator-sh/koord-queue/pkg/test/testutils/queueunits"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// This spec guards against quota over-admission in wait-for-pods-running queues.
//
// Background: with the "scheduled pods count as Running" behavior, the ResourceReporter releases
// the queue's assumed slot as soon as a unit's pods are scheduled (NodeName set), even if the pods
// never reach the Running phase (e.g. stuck pulling an image). Releasing assumed early must NOT
// allow the queue to admit a second unit that exceeds the quota: quota accounting (used) still
// counts the head unit's full admission until it is deleted.
//
// Scenario: submit a large batch of queued units at once (all competing for a quota that fits
// exactly one of them), then repeatedly delete the current head whose pod is scheduled but never
// runs. Through the whole churn, at most ONE unit may hold the quota (be Dequeued) at any moment.
var _ = Describe("wait-for-pods-running quota over-admission protection", Ordered, func() {

	const (
		quotaName = "overadmit-quota"
		queueNS   = "kube-queue"
		unitNS    = "default"
		// The whole guaranteed quota fits exactly ONE unit: Min == Max == cpu:2, every unit asks 2.
		unitCPU = "2"
		// A large batch of queued units churning through the single-slot quota. Each churn deletes
		// the scheduled-but-not-running head, which also repeats the scenario many times to filter
		// out timing-dependent flukes.
		totalUnits = 8
	)

	cpuQty := resource.MustParse(unitCPU)

	buildJob := func(name string) *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: unitNS,
				Annotations: map[string]string{
					"koord-queue/job-has-enqueued":      "true",
					"koord-queue/job-dequeue-timestamp": time.Now().Format("2006-01-02 15:04:05.999999999 -0700 MST"),
				},
			},
			Spec: batchv1.JobSpec{
				Parallelism: ptr.To(int32(1)),
				Completions: ptr.To(int32(1)),
				Suspend:     ptr.To(false),
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "worker",
							Image: "busybox",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{"cpu": cpuQty},
							},
						}},
						RestartPolicy: corev1.RestartPolicyNever,
					},
				},
			},
		}
	}

	buildQU := func(name string, priority int32) *v1alpha1.QueueUnit {
		qu := queueunits.MakeQueueUnit(name, unitNS).
			PodSets(kueue.PodSet{
				Name:  name,
				Count: 1,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "worker",
						Image: "busybox",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{"cpu": cpuQty},
						},
					}},
				}},
			}).
			Resources(map[string]int64{"cpu": cpuQty.Value()}).
			Priority(priority).
			Labels(map[string]string{elasticquotav1alpha1.QuotaNameLabelKey: quotaName}).
			QueueUnit()
		qu.Spec.ConsumerRef = &corev1.ObjectReference{
			APIVersion: "batch/v1",
			Kind:       "Job",
			Name:       name,
			Namespace:  unitNS,
		}
		return qu
	}

	buildScheduledPendingPod := func(job *batchv1.Job) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name + "-pod",
				Namespace: unitNS,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       job.Name,
					UID:        job.UID,
				}},
				Labels: map[string]string{"batch.kubernetes.io/controller-uid": string(job.UID)},
				Annotations: map[string]string{
					jobextutil.RelatedQueueUnitAnnoKey: unitNS + "/" + job.Name,
					jobextutil.RelatedPodSetAnnoKey:    job.Name,
				},
			},
			Spec: corev1.PodSpec{
				// Scheduled (NodeName set) but the pod phase stays Pending: simulates a pod that
				// cannot run, e.g. blocked by an image pull failure.
				NodeName: "fake-node",
				Containers: []corev1.Container{{
					Name:  "worker",
					Image: "busybox",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{"cpu": cpuQty},
					},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		}
	}

	BeforeAll(func() {
		By("pre-creating a wait-for-pods-running Queue (name == quota name) so the plugin's reconcile preserves the annotations")
		_, err := cli.SchedulingV1alpha1().Queues(queueNS).Create(ctx, &v1alpha1.Queue{
			ObjectMeta: metav1.ObjectMeta{
				Name:      quotaName,
				Namespace: queueNS,
				Annotations: map[string]string{
					schedulingqueuev2.WaitForPodsRunningAnnotation: "true",
				},
			},
			Spec: v1alpha1.QueueSpec{QueuePolicy: "Priority"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("creating the ElasticQuota (Min == Max == cpu:2) so a single unit exhausts the guaranteed quota")
		_, err = eqcli.SchedulingV1alpha1().ElasticQuotas("kube-system").Create(ctx, &eqv1alpha1.ElasticQuota{
			ObjectMeta: metav1.ObjectMeta{Name: quotaName, Namespace: "kube-system"},
			Spec: eqv1alpha1.ElasticQuotaSpec{
				Min: corev1.ResourceList{"cpu": cpuQty},
				Max: corev1.ResourceList{"cpu": cpuQty},
			},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())

		By("verifying the wait-for-pods-running annotation survived the plugin's Queue reconcile")
		Eventually(func(g Gomega) {
			q, err := cli.SchedulingV1alpha1().Queues(queueNS).Get(ctx, quotaName, metav1.GetOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(q.Annotations[schedulingqueuev2.WaitForPodsRunningAnnotation]).To(Equal("true"))
		}, 10*time.Second, 200*time.Millisecond).Should(Succeed())
	})

	It(fmt.Sprintf("should never over-admit while churning %d queued units by repeatedly deleting the scheduled-but-not-running head", totalUnits), func() {
		listDequeued := func(g Gomega) []v1alpha1.QueueUnit {
			list, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).List(ctx, metav1.ListOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			dequeued := []v1alpha1.QueueUnit{}
			for _, qu := range list.Items {
				if qu.Status.Phase == v1alpha1.Dequeued {
					dequeued = append(dequeued, qu)
				}
			}
			return dequeued
		}

		By("submitting a large batch of queued units at once")
		for i := 1; i <= totalUnits; i++ {
			jobName := fmt.Sprintf("oadm-job-%d", i)
			Expect(crClient.Create(ctx, buildJob(jobName))).To(Succeed())
			_, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Create(ctx, buildQU(jobName, 10), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}

		for churn := 1; churn <= totalUnits; churn++ {
			By(fmt.Sprintf("churn %d/%d: exactly one unit may be Dequeued (holding the quota)", churn, totalUnits))
			var headName string
			Eventually(func(g Gomega) {
				dequeued := listDequeued(g)
				g.Expect(dequeued).To(HaveLen(1))
				headName = dequeued[0].Name
				g.Expect(dequeued[0].Status.Admissions).NotTo(BeEmpty())
				g.Expect(dequeued[0].Status.Admissions[0].Replicas).To(Equal(int64(1)))
			}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

			By(fmt.Sprintf("churn %d/%d: head %q gets a scheduled-but-not-running pod (simulates image pull stuck)", churn, totalUnits, headName))
			job := &batchv1.Job{}
			Expect(crClient.Get(ctx, types.NamespacedName{Namespace: unitNS, Name: headName}, job)).To(Succeed())
			pod := buildScheduledPendingPod(job)
			Expect(crClient.Create(ctx, pod)).To(Succeed())

			By(fmt.Sprintf("churn %d/%d: the reporter counts the scheduled pod as Running and releases the assumed slot", churn, totalUnits))
			Eventually(func(g Gomega) {
				got, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).Get(ctx, headName, metav1.GetOptions{})
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got.Status.Admissions).NotTo(BeEmpty())
				g.Expect(got.Status.Admissions[0].Running).To(Equal(int64(1)))
				// Quota accounting must stay admission-based in Dequeued: Resources not back-filled.
				g.Expect(got.Status.Admissions[0].Resources).To(BeEmpty())
			}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

			if churn < totalUnits {
				By(fmt.Sprintf("churn %d/%d: the queue did try the next unit (proof the assumed slot was released) but quota blocked it", churn, totalUnits))
				// ErrorFunc writes the quota rejection reason into Status.Message of a unit it
				// attempted and failed. A still-blocked (non-released) queue would never try the
				// next unit, so a filled Message proves the early release really happened.
				Eventually(func(g Gomega) {
					list, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).List(ctx, metav1.ListOptions{})
					g.Expect(err).NotTo(HaveOccurred())
					attempted := false
					for _, qu := range list.Items {
						if qu.Name == headName || qu.Status.Phase == v1alpha1.Dequeued {
							continue
						}
						if qu.Status.Message != "" {
							attempted = true
							break
						}
					}
					g.Expect(attempted).To(BeTrue(),
						"scheduler never attempted any waiting unit: assumed slot not released or queue stuck")
				}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
			}

			By(fmt.Sprintf("churn %d/%d: over-admission guard — never more than one Dequeued unit", churn, totalUnits))
			Consistently(func(g Gomega) {
				g.Expect(listDequeued(g)).To(HaveLen(1),
					"over-admitted: more than one unit holds the quota whose capacity fits exactly one")
			}, 5*time.Second, 300*time.Millisecond).Should(Succeed())

			By(fmt.Sprintf("churn %d/%d: delete the scheduled-but-not-running head to free the quota", churn, totalUnits))
			gracePeriod := int64(0)
			Expect(crClient.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &gracePeriod})).To(Succeed())
			Expect(cli.SchedulingV1alpha1().QueueUnits(unitNS).Delete(ctx, headName, metav1.DeleteOptions{})).To(Succeed())
			Expect(crClient.Delete(ctx, job)).To(Succeed())
		}

		By("verifying the whole batch was processed and nothing is left behind")
		Eventually(func(g Gomega) {
			list, err := cli.SchedulingV1alpha1().QueueUnits(unitNS).List(ctx, metav1.ListOptions{})
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(list.Items).To(BeEmpty())
		}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})
