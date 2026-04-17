package elasticquotav1alpha1

import (
	"testing"

	api "github.com/koordinator-sh/koord-queue/pkg/apis/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
	"k8s.io/apimachinery/pkg/types"

	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNewElasticQuotaInfo(t *testing.T) {
	q := &v1alpha1.ElasticQuota{}
	q.Spec = v1alpha1.ElasticQuotaSpec{}
	q.Spec.Max = make(v1.ResourceList)
	q.Spec.Min = make(v1.ResourceList)
	q.Spec.Max["cpu"] = resource.MustParse("15")
	q.Spec.Min["cpu"] = resource.MustParse("10")
	q.Annotations = map[string]string{
		"aa": "bb",
	}

	elasticQuotaInfo := NewElasticQuotaInfo(q)
	assert.True(t, elasticQuotaInfo.Reserved != nil)
	assert.Equal(t, int64(15), elasticQuotaInfo.Max["cpu"])
	assert.Equal(t, int64(10), elasticQuotaInfo.Min["cpu"])
	assert.Equal(t, "bb", elasticQuotaInfo.Quota.Annotations["aa"])
}

func TestAddQueueUnit_DeleteQueueUnit(t *testing.T) {
	{
		q := &v1alpha1.ElasticQuota{}
		q.Name = "q1"
		q.Spec = v1alpha1.ElasticQuotaSpec{}
		q.Spec.Max = make(v1.ResourceList)
		q.Spec.Min = make(v1.ResourceList)
		q.Spec.Max["cpu"] = resource.MustParse("15")
		q.Spec.Min["cpu"] = resource.MustParse("10")
		q.Annotations = map[string]string{
			"aa": "bb",
		}

		elasticQuotaInfo := NewElasticQuotaInfo(q)
		assert.True(t, elasticQuotaInfo.Reserved != nil)
		assert.Equal(t, int64(15), elasticQuotaInfo.Max["cpu"])
		assert.Equal(t, int64(10), elasticQuotaInfo.Min["cpu"])
		assert.Equal(t, "bb", elasticQuotaInfo.Quota.Annotations["aa"])

		queueUnit1 := &framework.QueueUnitInfo{}
		queueUnit1.Unit = &api.QueueUnit{}
		queueUnit1.Unit.Name = "job1"
		queueUnit1.Unit.UID = types.UID("1")
		queueUnit1.Unit.Spec = api.QueueUnitSpec{}
		queueUnit1.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit1.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
		queueUnit1.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit2 := &framework.QueueUnitInfo{}
		queueUnit2.Unit = &api.QueueUnit{}
		queueUnit2.Unit.Name = "job2"
		queueUnit2.Unit.UID = types.UID("2")
		queueUnit2.Unit.Spec = api.QueueUnitSpec{}
		queueUnit2.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit2.Unit.Spec.Resource["cpu"] = resource.MustParse("3")
		queueUnit2.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit1.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit2.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}

		elasticQuotaInfo.AddQueueUnit("q1", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q1", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q1", queueUnit2)
		elasticQuotaInfo.AddQueueUnit("q1", queueUnit2)

		assert.Equal(t, 2, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("2")].Unit.Name, "job2")
		assert.Equal(t, int64(8), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q1", queueUnit2)
		elasticQuotaInfo.DeleteQueueUnit("q1", queueUnit2)
		assert.Equal(t, 1, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, int64(5), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q1", queueUnit1)
		elasticQuotaInfo.DeleteQueueUnit("q1", queueUnit1)
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, 0, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])
	}
	{
		q := &v1alpha1.ElasticQuota{}
		q.Name = "q1"
		q.Spec = v1alpha1.ElasticQuotaSpec{}
		q.Spec.Max = make(v1.ResourceList)
		q.Spec.Min = make(v1.ResourceList)
		q.Spec.Max["cpu"] = resource.MustParse("15")
		q.Spec.Min["cpu"] = resource.MustParse("10")
		q.Annotations = map[string]string{
			"aa": "bb",
		}

		elasticQuotaInfo := NewElasticQuotaInfo(q)
		assert.True(t, elasticQuotaInfo.Reserved != nil)
		assert.Equal(t, int64(15), elasticQuotaInfo.Max["cpu"])
		assert.Equal(t, int64(10), elasticQuotaInfo.Min["cpu"])
		assert.Equal(t, "bb", elasticQuotaInfo.Quota.Annotations["aa"])

		queueUnit1 := &framework.QueueUnitInfo{}
		queueUnit1.Unit = &api.QueueUnit{}
		queueUnit1.Unit.Name = "job1"
		queueUnit1.Unit.UID = types.UID("1")
		queueUnit1.Unit.Spec = api.QueueUnitSpec{}
		queueUnit1.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit1.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
		queueUnit1.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit2 := &framework.QueueUnitInfo{}
		queueUnit2.Unit = &api.QueueUnit{}
		queueUnit2.Unit.Name = "job2"
		queueUnit2.Unit.UID = types.UID("2")
		queueUnit2.Unit.Spec = api.QueueUnitSpec{}
		queueUnit2.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit2.Unit.Spec.Resource["cpu"] = resource.MustParse("3")
		queueUnit2.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit1.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
		}
		queueUnit2.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
		}

		elasticQuotaInfo.AddQueueUnit("q1", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q1", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q1", queueUnit2)
		elasticQuotaInfo.AddQueueUnit("q1", queueUnit2)

		assert.Equal(t, 2, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("2")].Unit.Name, "job2")
		assert.Equal(t, int64(8), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q1", queueUnit2)
		elasticQuotaInfo.DeleteQueueUnit("q1", queueUnit2)
		assert.Equal(t, 1, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, int64(5), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q1", queueUnit1)
		elasticQuotaInfo.DeleteQueueUnit("q1", queueUnit1)
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, 0, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])
	}
	{
		q := &v1alpha1.ElasticQuota{}
		q.Name = "q1"
		q.Spec = v1alpha1.ElasticQuotaSpec{}
		q.Spec.Max = make(v1.ResourceList)
		q.Spec.Min = make(v1.ResourceList)
		q.Spec.Max["cpu"] = resource.MustParse("15")
		q.Spec.Min["cpu"] = resource.MustParse("10")
		q.Annotations = map[string]string{
			"aa": "bb",
		}

		elasticQuotaInfo := NewElasticQuotaInfo(q)
		assert.True(t, elasticQuotaInfo.Reserved != nil)
		assert.Equal(t, int64(15), elasticQuotaInfo.Max["cpu"])
		assert.Equal(t, int64(10), elasticQuotaInfo.Min["cpu"])
		assert.Equal(t, "bb", elasticQuotaInfo.Quota.Annotations["aa"])

		queueUnit1 := &framework.QueueUnitInfo{}
		queueUnit1.Unit = &api.QueueUnit{}
		queueUnit1.Unit.Name = "job1"
		queueUnit1.Unit.UID = types.UID("1")
		queueUnit1.Unit.Spec = api.QueueUnitSpec{}
		queueUnit1.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit1.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
		queueUnit1.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit2 := &framework.QueueUnitInfo{}
		queueUnit2.Unit = &api.QueueUnit{}
		queueUnit2.Unit.Name = "job2"
		queueUnit2.Unit.UID = types.UID("2")
		queueUnit2.Unit.Spec = api.QueueUnitSpec{}
		queueUnit2.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit2.Unit.Spec.Resource["cpu"] = resource.MustParse("3")
		queueUnit2.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit1.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit2.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}

		elasticQuotaInfo.AddQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit2)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit2)

		assert.Equal(t, 2, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("2")].Unit.Name, "job2")
		assert.Equal(t, int64(8), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit2)
		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit2)
		assert.Equal(t, 1, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, int64(5), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit1)
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, 0, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])
	}
	{
		q := &v1alpha1.ElasticQuota{}
		q.Name = "q1"
		q.Spec = v1alpha1.ElasticQuotaSpec{}
		q.Spec.Max = make(v1.ResourceList)
		q.Spec.Min = make(v1.ResourceList)
		q.Spec.Max["cpu"] = resource.MustParse("15")
		q.Spec.Min["cpu"] = resource.MustParse("10")
		q.Annotations = map[string]string{
			"aa": "bb",
		}

		elasticQuotaInfo := NewElasticQuotaInfo(q)
		assert.True(t, elasticQuotaInfo.Reserved != nil)
		assert.Equal(t, int64(15), elasticQuotaInfo.Max["cpu"])
		assert.Equal(t, int64(10), elasticQuotaInfo.Min["cpu"])
		assert.Equal(t, "bb", elasticQuotaInfo.Quota.Annotations["aa"])

		queueUnit1 := &framework.QueueUnitInfo{}
		queueUnit1.Unit = &api.QueueUnit{}
		queueUnit1.Unit.Name = "job1"
		queueUnit1.Unit.UID = types.UID("1")
		queueUnit1.Unit.Spec = api.QueueUnitSpec{}
		queueUnit1.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit1.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
		queueUnit1.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit2 := &framework.QueueUnitInfo{}
		queueUnit2.Unit = &api.QueueUnit{}
		queueUnit2.Unit.Name = "job2"
		queueUnit2.Unit.UID = types.UID("2")
		queueUnit2.Unit.Spec = api.QueueUnitSpec{}
		queueUnit2.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit2.Unit.Spec.Resource["cpu"] = resource.MustParse("3")
		queueUnit2.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit1.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
		}
		queueUnit2.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
		}

		elasticQuotaInfo.AddQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit2)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit2)

		assert.Equal(t, 2, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("2")].Unit.Name, "job2")
		assert.Equal(t, int64(8), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit2)
		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit2)
		assert.Equal(t, 1, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, int64(5), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit1)
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, 0, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])
	}
	{
		q := &v1alpha1.ElasticQuota{}
		q.Name = "q1"
		q.Spec = v1alpha1.ElasticQuotaSpec{}
		q.Spec.Max = make(v1.ResourceList)
		q.Spec.Min = make(v1.ResourceList)
		q.Spec.Max["cpu"] = resource.MustParse("15")
		q.Spec.Min["cpu"] = resource.MustParse("10")
		q.Annotations = map[string]string{
			"aa": "bb",
		}

		elasticQuotaInfo := NewElasticQuotaInfo(q)
		assert.True(t, elasticQuotaInfo.Reserved != nil)
		assert.Equal(t, int64(15), elasticQuotaInfo.Max["cpu"])
		assert.Equal(t, int64(10), elasticQuotaInfo.Min["cpu"])
		assert.Equal(t, "bb", elasticQuotaInfo.Quota.Annotations["aa"])

		queueUnit1 := &framework.QueueUnitInfo{}
		queueUnit1.Unit = &api.QueueUnit{}
		queueUnit1.Unit.Name = "job1"
		queueUnit1.Unit.UID = types.UID("1")
		queueUnit1.Unit.Spec = api.QueueUnitSpec{}
		queueUnit1.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit1.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
		queueUnit1.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit2 := &framework.QueueUnitInfo{}
		queueUnit2.Unit = &api.QueueUnit{}
		queueUnit2.Unit.Name = "job2"
		queueUnit2.Unit.UID = types.UID("2")
		queueUnit2.Unit.Spec = api.QueueUnitSpec{}
		queueUnit2.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit2.Unit.Spec.Resource["cpu"] = resource.MustParse("3")
		queueUnit2.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit1.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}
		queueUnit2.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
		}

		elasticQuotaInfo.AddQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit2)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit2)

		assert.Equal(t, 2, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("2")].Unit.Name, "job2")
		assert.Equal(t, int64(8), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit2)
		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit2)
		assert.Equal(t, 1, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, int64(5), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit1)
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, 0, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])
	}
	{
		q := &v1alpha1.ElasticQuota{}
		q.Name = "q1"
		q.Spec = v1alpha1.ElasticQuotaSpec{}
		q.Spec.Max = make(v1.ResourceList)
		q.Spec.Min = make(v1.ResourceList)
		q.Spec.Max["cpu"] = resource.MustParse("15")
		q.Spec.Min["cpu"] = resource.MustParse("10")
		q.Annotations = map[string]string{
			"aa": "bb",
		}

		elasticQuotaInfo := NewElasticQuotaInfo(q)
		assert.True(t, elasticQuotaInfo.Reserved != nil)
		assert.Equal(t, int64(15), elasticQuotaInfo.Max["cpu"])
		assert.Equal(t, int64(10), elasticQuotaInfo.Min["cpu"])
		assert.Equal(t, "bb", elasticQuotaInfo.Quota.Annotations["aa"])

		queueUnit1 := &framework.QueueUnitInfo{}
		queueUnit1.Unit = &api.QueueUnit{}
		queueUnit1.Unit.Name = "job1"
		queueUnit1.Unit.UID = types.UID("1")
		queueUnit1.Unit.Spec = api.QueueUnitSpec{}
		queueUnit1.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit1.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
		queueUnit1.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit2 := &framework.QueueUnitInfo{}
		queueUnit2.Unit = &api.QueueUnit{}
		queueUnit2.Unit.Name = "job2"
		queueUnit2.Unit.UID = types.UID("2")
		queueUnit2.Unit.Spec = api.QueueUnitSpec{}
		queueUnit2.Unit.Spec.Resource = make(v1.ResourceList)
		queueUnit2.Unit.Spec.Resource["cpu"] = resource.MustParse("3")
		queueUnit2.Unit.Labels = map[string]string{
			QuotaNameLabelKey: "q1",
		}

		queueUnit1.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
		}
		queueUnit2.Unit.Annotations = map[string]string{
			utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
		}

		elasticQuotaInfo.AddQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit2)
		elasticQuotaInfo.AddQueueUnit("q2", queueUnit2)

		assert.Equal(t, 2, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("2")].Unit.Name, "job2")
		assert.Equal(t, int64(8), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(8), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit2)
		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit2)
		assert.Equal(t, 1, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, elasticQuotaInfo.Reserved[types.UID("1")].Unit.Name, "job1")
		assert.Equal(t, int64(5), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(5), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])

		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit1)
		elasticQuotaInfo.DeleteQueueUnit("q2", queueUnit1)
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, 0, len(elasticQuotaInfo.Reserved))
		assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.OverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenOverSoldUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.GuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.SelfGuaranteedUsed["cpu"])
		assert.Equal(t, int64(0), elasticQuotaInfo.ChildrenGuaranteedUsed["cpu"])
	}
}

func TestAddQueueUnit_CheckUsage(t *testing.T) {
	q := &v1alpha1.ElasticQuota{}
	q.Spec = v1alpha1.ElasticQuotaSpec{}
	q.Spec.Max = make(v1.ResourceList)
	q.Spec.Min = make(v1.ResourceList)
	q.Spec.Max["cpu"] = resource.MustParse("15")
	q.Spec.Min["cpu"] = resource.MustParse("10")
	q.Annotations = map[string]string{
		"aa": "bb",
	}

	elasticQuotaInfo := NewElasticQuotaInfo(q)
	assert.True(t, elasticQuotaInfo.Reserved != nil)
	assert.Equal(t, int64(15), elasticQuotaInfo.Max["cpu"])
	assert.Equal(t, int64(10), elasticQuotaInfo.Min["cpu"])
	assert.Equal(t, "bb", elasticQuotaInfo.Quota.Annotations["aa"])

	queueUnit1 := &framework.QueueUnitInfo{}
	queueUnit1.Unit = &api.QueueUnit{}
	queueUnit1.Unit.Name = "job1"
	queueUnit1.Unit.UID = types.UID("1")
	queueUnit1.Unit.Spec = api.QueueUnitSpec{}
	queueUnit1.Unit.Spec.Resource = make(v1.ResourceList)
	queueUnit1.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
	queueUnit1.Unit.Spec.Resource["mem"] = resource.MustParse("5")
	queueUnit1.Unit.Labels = map[string]string{
		QuotaNameLabelKey: "q1",
	}
	queueUnit1.Unit.Annotations = map[string]string{
		utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForce,
	}

	{
		elasticQuotaInfo = NewElasticQuotaInfo(q)
		elasticQuotaInfo.Used = map[v1.ResourceName]int64{
			"cpu": 9,
		}

		err := elasticQuotaInfo.CheckUsage("q1", queueUnit1, 1)
		assert.True(t, err == nil)
		err = elasticQuotaInfo.CheckUsage("q1", queueUnit1, 0.1)
		assert.True(t, err != nil)
	}
	{
		elasticQuotaInfo = NewElasticQuotaInfo(q)
		elasticQuotaInfo.Used = map[v1.ResourceName]int64{
			"cpu": 9,
		}

		err := elasticQuotaInfo.CheckUsage("q2", queueUnit1, 1)
		assert.True(t, err == nil)
		err = elasticQuotaInfo.CheckUsage("q2", queueUnit1, 0.1)
		assert.True(t, err != nil)
	}
	{
		elasticQuotaInfo = NewElasticQuotaInfo(q)
		elasticQuotaInfo.Max = nil
		err := elasticQuotaInfo.CheckUsage("q2", queueUnit1, 1)
		assert.True(t, err != nil)
	}

	queueUnit1.Unit.Annotations = map[string]string{
		utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
	}

	{
		elasticQuotaInfo = NewElasticQuotaInfo(q)
		elasticQuotaInfo.SelfGuaranteedUsed = map[v1.ResourceName]int64{
			"cpu": 4,
		}
		elasticQuotaInfo.ChildrenGuaranteedUsed = map[v1.ResourceName]int64{
			"cpu": 4,
		}

		err := elasticQuotaInfo.CheckUsage("q1", queueUnit1, 1)
		assert.True(t, err == nil)
		err = elasticQuotaInfo.CheckUsage("q1", queueUnit1, 0.1)
		assert.True(t, err != nil)

		err = elasticQuotaInfo.CheckUsage("", queueUnit1, 0.1)
		assert.True(t, err == nil)
	}
}

func TestDeleteQueueUnit_UsesReservedQueueUnit(t *testing.T) {
	// This test verifies the fix where DeleteQueueUnit uses the reserved queueUnit
	// instead of the parameter to ensure correct resource tracking
	q := &v1alpha1.ElasticQuota{}
	q.Name = "q1"
	q.Spec = v1alpha1.ElasticQuotaSpec{}
	q.Spec.Max = make(v1.ResourceList)
	q.Spec.Min = make(v1.ResourceList)
	q.Spec.Max["cpu"] = resource.MustParse("20")
	q.Spec.Min["cpu"] = resource.MustParse("10")

	elasticQuotaInfo := NewElasticQuotaInfo(q)

	// Create and add a queueUnit with 5 CPU
	queueUnit := &framework.QueueUnitInfo{}
	queueUnit.Unit = &api.QueueUnit{}
	queueUnit.Unit.Name = "job1"
	queueUnit.Unit.UID = types.UID("test-uid-1")
	queueUnit.Unit.Spec = api.QueueUnitSpec{}
	queueUnit.Unit.Spec.Resource = make(v1.ResourceList)
	queueUnit.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
	queueUnit.Unit.Labels = map[string]string{
		QuotaNameLabelKey: "q1",
	}
	queueUnit.Unit.Annotations = map[string]string{
		utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
	}

	// Add the queueUnit
	elasticQuotaInfo.AddQueueUnit("q1", queueUnit)
	assert.Equal(t, 1, len(elasticQuotaInfo.Reserved))
	assert.Equal(t, int64(5000), elasticQuotaInfo.Used["cpu"]) // 5 cores = 5000 milli-cores

	// Create a different queueUnit with different resource to pass as parameter
	// This simulates the bug where the parameter might have stale/different data
	differentQueueUnit := &framework.QueueUnitInfo{}
	differentQueueUnit.Unit = &api.QueueUnit{}
	differentQueueUnit.Unit.Name = "job1"
	differentQueueUnit.Unit.UID = types.UID("test-uid-1")
	differentQueueUnit.Unit.Spec = api.QueueUnitSpec{}
	differentQueueUnit.Unit.Spec.Resource = make(v1.ResourceList)
	// Different resource value - if DeleteQueueUnit uses this instead of reserved, it would be wrong
	differentQueueUnit.Unit.Spec.Resource["cpu"] = resource.MustParse("10")
	differentQueueUnit.Unit.Labels = map[string]string{
		QuotaNameLabelKey: "q1",
	}
	differentQueueUnit.Unit.Annotations = map[string]string{
		utils.AnnotationActualQuotaOversoldType: utils.QuotaOversoldTypeForbidden,
	}

	// Delete using the different queueUnit parameter
	// The fix ensures it uses the reserved queueUnit (5 CPU) not the parameter (10 CPU)
	elasticQuotaInfo.DeleteQueueUnit("q1", differentQueueUnit)

	// After delete, used should be 0 (5000 - 5000 = 0), not -5000 (5000 - 10000)
	// This proves DeleteQueueUnit uses the reserved queueUnit's resource, not the parameter's
	assert.Equal(t, 0, len(elasticQuotaInfo.Reserved))
	assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
	assert.Equal(t, int64(0), elasticQuotaInfo.SelfUsed["cpu"])
}

func TestDeleteQueueUnit_NonExistentQueueUnit(t *testing.T) {
	// Test that deleting a non-existent queueUnit doesn't cause panic or errors
	q := &v1alpha1.ElasticQuota{}
	q.Name = "q1"
	q.Spec = v1alpha1.ElasticQuotaSpec{}
	q.Spec.Max = make(v1.ResourceList)
	q.Spec.Min = make(v1.ResourceList)
	q.Spec.Max["cpu"] = resource.MustParse("20")
	q.Spec.Min["cpu"] = resource.MustParse("10")

	elasticQuotaInfo := NewElasticQuotaInfo(q)

	// Try to delete a queueUnit that was never added
	nonExistentQueueUnit := &framework.QueueUnitInfo{}
	nonExistentQueueUnit.Unit = &api.QueueUnit{}
	nonExistentQueueUnit.Unit.Name = "non-existent"
	nonExistentQueueUnit.Unit.UID = types.UID("non-existent-uid")
	nonExistentQueueUnit.Unit.Spec = api.QueueUnitSpec{}
	nonExistentQueueUnit.Unit.Spec.Resource = make(v1.ResourceList)
	nonExistentQueueUnit.Unit.Spec.Resource["cpu"] = resource.MustParse("5")
	nonExistentQueueUnit.Unit.Labels = map[string]string{
		QuotaNameLabelKey: "q1",
	}

	// Should not panic and should not change any state
	elasticQuotaInfo.DeleteQueueUnit("q1", nonExistentQueueUnit)
	assert.Equal(t, 0, len(elasticQuotaInfo.Reserved))
	assert.Equal(t, int64(0), elasticQuotaInfo.Used["cpu"])
}
