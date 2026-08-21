package reclaimablepods

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/jobext/util"
)

// MR4: as pods of a job complete, their share of the reservation is handed back before the whole
// job finishes. The reservation is what the elastic quota plugin turns into used, so shrinking
// the admitted replicas is what frees capacity for the next job.
var _ = Describe("QueueUnit reclaimablePods", func() {

	const ns = "default"

	makeJob := func(name string, parallelism int32) *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
				// Marks the job as taken over by kube-queue, which is what ManagedByQueue looks at.
				Annotations: map[string]string{"koord-queue/job-has-enqueued": "true"},
			},
			Spec: batchv1.JobSpec{
				Parallelism: ptr.To(parallelism),
				Suspend:     ptr.To(false),
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers: []corev1.Container{{
							Name:  "main",
							Image: "registry.k8s.io/pause:3.9",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{"cpu": resource.MustParse("1")},
							},
						}},
					},
				},
			},
		}
	}

	// makePod creates a pod already bound to a node, since only scheduled pods count towards the
	// reservation, and then forces the requested phase through the status subresource.
	makePod := func(name, jobName string, uid types.UID, phase corev1.PodPhase) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       jobName,
					UID:        uid,
				}},
				Annotations: map[string]string{
					util.RelatedQueueUnitAnnoKey: ns + "/" + jobName,
					// batch/v1 Job uses the owner name as its single podSet name.
					util.RelatedPodSetAnnoKey: jobName,
				},
			},
			Spec: corev1.PodSpec{
				NodeName:      "fake-node",
				RestartPolicy: corev1.RestartPolicyNever,
				Containers: []corev1.Container{{
					Name:  "main",
					Image: "registry.k8s.io/pause:3.9",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{"cpu": resource.MustParse("1")},
					},
				}},
			},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())

		Eventually(func(g Gomega) {
			cur := &corev1.Pod{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())
			cur.Status.Phase = phase
			g.Expect(k8sClient.Status().Update(ctx, cur)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	}

	// markJobActive fills in the job status a real job controller would publish. Without it the
	// job still looks like it is queueing and the reporter has nothing in flight to account for.
	markJobActive := func(name string, active int32) {
		Eventually(func(g Gomega) {
			cur := &batchv1.Job{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())
			cur.Status.Active = active
			g.Expect(k8sClient.Status().Update(ctx, cur)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
	}

	reclaimableCount := func(qu *v1alpha1.QueueUnit, podSet string) (int32, bool) {
		for _, rp := range qu.Status.ReclaimablePods {
			if rp.Name == podSet {
				return rp.Count, true
			}
		}
		return 0, false
	}

	It("should report completed pods and release their share of the reservation", func() {
		const name = "reclaim-partial"

		job := makeJob(name, 2)
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, job) })

		created := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, created)).To(Succeed())
		markJobActive(name, 2)

		By("creating an admitted queue unit holding two replicas")
		qu := &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: name, UID: created.UID,
				}},
			},
			Spec: v1alpha1.QueueUnitSpec{
				ConsumerRef: &corev1.ObjectReference{
					APIVersion: "batch/v1", Kind: "Job", Name: name, Namespace: ns,
				},
				Resource: corev1.ResourceList{"cpu": resource.MustParse("2")},
				Request:  corev1.ResourceList{"cpu": resource.MustParse("2")},
			},
		}
		Expect(k8sClient.Create(ctx, qu)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, qu) })

		Eventually(func(g Gomega) {
			cur := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())
			cur.Status.Phase = v1alpha1.Running
			cur.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
			cur.Status.LastAllocateTime = &metav1.Time{Time: time.Now()}
			cur.Status.Admissions = []v1alpha1.Admission{{
				Name:      name,
				Replicas:  2,
				Running:   2,
				Resources: corev1.ResourceList{"cpu": resource.MustParse("2")},
			}}
			g.Expect(k8sClient.Status().Update(ctx, cur)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		By("one pod completes while the other keeps running")
		makePod("reclaim-partial-a", name, created.UID, corev1.PodSucceeded)
		makePod("reclaim-partial-b", name, created.UID, corev1.PodRunning)
		DeferCleanup(func() {
			for _, p := range []string{"reclaim-partial-a", "reclaim-partial-b"} {
				_ = k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: p, Namespace: ns}})
			}
		})

		By("verifying the completed pod is reported and its replica is given back")
		// Nudge the queue unit so the reporter reconciles with the pods in their final state.
		touch := func() {
			Eventually(func(g Gomega) {
				cur := &v1alpha1.QueueUnit{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())
				cur.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
				g.Expect(k8sClient.Status().Update(ctx, cur)).To(Succeed())
			}, 30*time.Second, 200*time.Millisecond).Should(Succeed())
		}
		touch()

		Eventually(func(g Gomega) {
			cur := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())

			count, found := reclaimableCount(cur, name)
			g.Expect(found).To(BeTrue(), "reclaimablePods must be persisted, not pruned by the CRD schema")
			g.Expect(count).To(Equal(int32(1)))

			g.Expect(cur.Status.Admissions).To(HaveLen(1))
			// Only the still-running pod keeps its slot, so one CPU returns to the quota.
			g.Expect(cur.Status.Admissions[0].Replicas).To(Equal(int64(1)))
			g.Expect(cur.Status.Admissions[0].Running).To(Equal(int64(1)))
		}, 60*time.Second, 500*time.Millisecond).Should(Succeed())

		By("verifying the count never walks back when the completed pod is garbage collected")
		Expect(k8sClient.Delete(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "reclaim-partial-a", Namespace: ns},
		})).To(Succeed())

		// A pod that is simply gone is no evidence about completion, so the recorded count has
		// to stay put: this is the exact confusion that previously caused over-admission.
		Consistently(func(g Gomega) {
			cur := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())
			count, found := reclaimableCount(cur, name)
			g.Expect(found).To(BeTrue())
			g.Expect(count).To(Equal(int32(1)))
			g.Expect(cur.Status.Admissions[0].Replicas).To(BeNumerically(">=", int64(1)))
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())
	})

	It("should not report anything while every pod is still running", func() {
		const name = "reclaim-none"

		job := makeJob(name, 1)
		Expect(k8sClient.Create(ctx, job)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, job) })

		created := &batchv1.Job{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, created)).To(Succeed())
		markJobActive(name, 1)

		qu := &v1alpha1.QueueUnit{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ns,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "batch/v1", Kind: "Job", Name: name, UID: created.UID,
				}},
			},
			Spec: v1alpha1.QueueUnitSpec{
				ConsumerRef: &corev1.ObjectReference{
					APIVersion: "batch/v1", Kind: "Job", Name: name, Namespace: ns,
				},
				Resource: corev1.ResourceList{"cpu": resource.MustParse("1")},
				Request:  corev1.ResourceList{"cpu": resource.MustParse("1")},
			},
		}
		Expect(k8sClient.Create(ctx, qu)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, qu) })

		Eventually(func(g Gomega) {
			cur := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())
			cur.Status.Phase = v1alpha1.Running
			cur.Status.LastUpdateTime = &metav1.Time{Time: time.Now()}
			cur.Status.LastAllocateTime = &metav1.Time{Time: time.Now()}
			cur.Status.Admissions = []v1alpha1.Admission{{
				Name: name, Replicas: 1, Running: 1,
				Resources: corev1.ResourceList{"cpu": resource.MustParse("1")},
			}}
			g.Expect(k8sClient.Status().Update(ctx, cur)).To(Succeed())
		}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

		makePod("reclaim-none-a", name, created.UID, corev1.PodRunning)
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "reclaim-none-a", Namespace: ns}})
		})

		Consistently(func(g Gomega) {
			cur := &v1alpha1.QueueUnit{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, cur)).To(Succeed())
			_, found := reclaimableCount(cur, name)
			g.Expect(found).To(BeFalse(), fmt.Sprintf("nothing completed, got %+v", cur.Status.ReclaimablePods))
			g.Expect(cur.Status.Admissions[0].Replicas).To(Equal(int64(1)), "a running pod must keep its reservation")
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed())
	})
})
