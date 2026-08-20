package elasticquotav1alpha1

import (
	"errors"
	"fmt"
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
	"github.com/koordinator-sh/koord-queue/pkg/utils"
)

type ElasticQuotaInfo struct {
	Quota *v1alpha1.ElasticQuota
	Min   map[v1.ResourceName]int64
	Max   map[v1.ResourceName]int64

	Used         map[v1.ResourceName]int64
	SelfUsed     map[v1.ResourceName]int64
	ChildrenUsed map[v1.ResourceName]int64

	GuaranteedUsed         map[v1.ResourceName]int64
	SelfGuaranteedUsed     map[v1.ResourceName]int64
	ChildrenGuaranteedUsed map[v1.ResourceName]int64

	Reserved map[types.UID]*framework.QueueUnitInfo
}

func NewElasticQuotaInfo(q *v1alpha1.ElasticQuota) *ElasticQuotaInfo {
	info := &ElasticQuotaInfo{
		Used:                   make(map[v1.ResourceName]int64),
		SelfUsed:               make(map[v1.ResourceName]int64),
		ChildrenUsed:           make(map[v1.ResourceName]int64),
		GuaranteedUsed:         make(map[v1.ResourceName]int64),
		SelfGuaranteedUsed:     make(map[v1.ResourceName]int64),
		ChildrenGuaranteedUsed: make(map[v1.ResourceName]int64),

		Quota:    q,
		Reserved: make(map[types.UID]*framework.QueueUnitInfo),
	}

	info.Max = utils.NewResource(q.Spec.Max).Resources
	info.Min = utils.NewResource(q.Spec.Min).Resources

	return info
}

func (info *ElasticQuotaInfo) AddQueueUnit(currentQuota string, queueUnit *framework.QueueUnitInfo) {
	if _, exist := info.Reserved[queueUnit.Unit.UID]; exist {
		return
	}

	info.addQueueUnitInternal(currentQuota, queueUnit)

	res := utils.GetReservedResource(queueUnit.Unit).Resources
	queueUnitQuota := getQuotaName(queueUnit.Unit)

	klog.Infof("success AddQueueUnit, currentQuotaName:%v, item QueueName:%v, itemName:%v, "+
		"itemRes:%v, max:%v, min:%v, used:%v, selfUsed:%v, childrenUsed:%v, "+
		"guaranteedUsed:%v, selfGuaranteedUsed:%v, childrenGuaranteedUsed:%v",
		currentQuota, queueUnitQuota, queueUnit.Name,
		res, info.Max, info.Min, info.Used, info.SelfUsed, info.ChildrenUsed, info.GuaranteedUsed,
		info.SelfGuaranteedUsed, info.ChildrenGuaranteedUsed)
}

// addQueueUnitInternal adds the queueUnit to Reserved and updates Used counters without logging.
func (info *ElasticQuotaInfo) addQueueUnitInternal(currentQuota string, queueUnit *framework.QueueUnitInfo) {
	if _, exist := info.Reserved[queueUnit.Unit.UID]; exist {
		return
	}

	info.Reserved[queueUnit.Unit.UID] = queueUnit
	res := utils.GetReservedResource(queueUnit.Unit).Resources

	queueUnitQuota := getQuotaName(queueUnit.Unit)
	sameQuota := queueUnitQuota == currentQuota

	utils.UpdateUsage(info.Used, res, 1)
	if sameQuota {
		utils.UpdateUsage(info.SelfUsed, res, 1)
	} else {
		utils.UpdateUsage(info.ChildrenUsed, res, 1)
	}

	utils.UpdateUsage(info.GuaranteedUsed, res, 1)
	if sameQuota {
		utils.UpdateUsage(info.SelfGuaranteedUsed, res, 1)
	} else {
		utils.UpdateUsage(info.ChildrenGuaranteedUsed, res, 1)
	}
}

func (info *ElasticQuotaInfo) DeleteQueueUnit(currentQuota string, queueUnit *framework.QueueUnitInfo) {
	reserved, exist := info.Reserved[queueUnit.Unit.UID]
	if reserved == nil || !exist {
		return
	}

	// Capture info for logging before delete
	reservedUnit := reserved.Unit
	res := utils.GetReservedResource(reservedUnit).Resources
	queueUnitQuota := getQuotaName(reservedUnit)

	info.deleteQueueUnitInternal(currentQuota, queueUnit)

	klog.Infof("success DeleteQueueUnit, currentQuotaName:%v, item QueueName:%v, itemName:%v, "+
		"itemRes:%v, max:%v, min:%v, used:%v, selfUsed:%v, childrenUsed:%v, "+
		"guaranteedUsed:%v, selfGuaranteedUsed:%v, childrenGuaranteedUsed:%v",
		currentQuota, queueUnitQuota, reservedUnit.Name,
		res, info.Max, info.Min, info.Used, info.SelfUsed, info.ChildrenUsed, info.GuaranteedUsed,
		info.SelfGuaranteedUsed, info.ChildrenGuaranteedUsed)
}

// deleteQueueUnitInternal removes the queueUnit from Reserved and updates Used counters without logging.
// It uses the stored reserved object (not the passed queueUnit) to compute the release amount.
func (info *ElasticQuotaInfo) deleteQueueUnitInternal(currentQuota string, queueUnit *framework.QueueUnitInfo) {
	reserved, exist := info.Reserved[queueUnit.Unit.UID]
	if reserved == nil || !exist {
		return
	}

	reservedUnit := reserved.Unit
	delete(info.Reserved, reservedUnit.UID)
	res := utils.GetReservedResource(reservedUnit).Resources

	queueUnitQuota := getQuotaName(reservedUnit)
	sameQuota := queueUnitQuota == currentQuota

	utils.UpdateUsage(info.Used, res, -1)
	if sameQuota {
		utils.UpdateUsage(info.SelfUsed, res, -1)
	} else {
		utils.UpdateUsage(info.ChildrenUsed, res, -1)
	}

	utils.UpdateUsage(info.GuaranteedUsed, res, -1)
	if sameQuota {
		utils.UpdateUsage(info.SelfGuaranteedUsed, res, -1)
	} else {
		utils.UpdateUsage(info.ChildrenGuaranteedUsed, res, -1)
	}
}

// ResizeQueueUnit updates the reserved queueUnit in-place. If the resource amount is unchanged,
// it silently replaces the stored object. If it differs, it adjusts the Used counters and logs
// a single line with the before/after resource amounts.
func (info *ElasticQuotaInfo) ResizeQueueUnit(currentQuota string, newQueueUnit *framework.QueueUnitInfo) {
	reserved, exist := info.Reserved[newQueueUnit.Unit.UID]
	if reserved == nil || !exist {
		return
	}

	oldRes := utils.GetReservedResource(reserved.Unit).Resources
	newRes := utils.GetReservedResource(newQueueUnit.Unit).Resources

	// If resource amount is the same, just swap the stored object silently
	if reflect.DeepEqual(oldRes, newRes) {
		info.Reserved[newQueueUnit.Unit.UID] = newQueueUnit
		return
	}

	// Resource amount changed: delete old, add new, log once
	info.deleteQueueUnitInternal(currentQuota, newQueueUnit)
	info.addQueueUnitInternal(currentQuota, newQueueUnit)

	klog.Infof("success ResizeQueueUnit, currentQuotaName:%v, itemName:%v, "+
		"oldRes:%v, newRes:%v, max:%v, min:%v, used:%v, selfUsed:%v, childrenUsed:%v, "+
		"guaranteedUsed:%v, selfGuaranteedUsed:%v, childrenGuaranteedUsed:%v",
		currentQuota, newQueueUnit.Name,
		oldRes, newRes, info.Max, info.Min, info.Used, info.SelfUsed, info.ChildrenUsed, info.GuaranteedUsed,
		info.SelfGuaranteedUsed, info.ChildrenGuaranteedUsed)
}

func (info *ElasticQuotaInfo) CheckUsage(currentQuota string,
	queueUnit *framework.QueueUnitInfo, oversellRate float64) error {
	queueUnitRes := utils.NewResource(queueUnit.Unit.Spec.Resource).Resources
	queueUnitQuota := getQuotaName(queueUnit.Unit)

	limit := info.Max
	used := info.Used

	if len(limit) == 0 && len(queueUnitRes) != 0 {
		klog.Infof("limit is empty, itemName:%v, queueUnitQuota:%v, currentQuota:%v, "+
			"queueUnitRes:%v, max:%v", queueUnit.Name, queueUnitQuota,
			currentQuota, queueUnitRes, info.Max)

		return fmt.Errorf("limited quotaName:%v, quota spec is empty but queue unit res is not empty", currentQuota)
	}

	valid, resKey := checkResource(limit, used, queueUnitRes, oversellRate)
	if !valid {
		klog.Infof("res not enough, itemName:%v, queueUnitQuota:%v, currentQuota:%v, "+
			"oversellRate:%v,  resKey:%v, queueUnitRes:%v, max:%v, used:%v", queueUnit.Name,
			queueUnitQuota, currentQuota, oversellRate, resKey, queueUnitRes, info.Max, info.Used)

		errMsg := fmt.Sprintf("limited quotaName:%v, limited resKey:%v,", currentQuota, resKey)
		return errors.New(errMsg)
	}

	return nil
}

func checkResource(limit map[v1.ResourceName]int64, used map[v1.ResourceName]int64,
	req map[v1.ResourceName]int64, oversellRate float64) (bool, v1.ResourceName) {
	for resKey, resValue := range req {
		if _, exist := limit[resKey]; !exist {
			continue
		}

		scaledLimit := float64(limit[resKey]) * oversellRate
		if scaledLimit-float64(used[resKey]) < float64(resValue) {
			return false, resKey
		}
	}

	return true, ""
}
