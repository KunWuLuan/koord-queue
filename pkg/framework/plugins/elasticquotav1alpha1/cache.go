package elasticquotav1alpha1

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/koordinator-sh/koord-queue/pkg/utils"

	"github.com/koordinator-sh/koord-queue/pkg/framework"
	"github.com/koordinator-sh/koord-queue/pkg/metrics"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"

	"github.com/koordinator-sh/koord-queue/pkg/framework/apis/elasticquota/scheduling/v1alpha1"
)

type Cache interface {
	CheckUsage(quotaKey string, queueUnit *framework.QueueUnitInfo, oversellrate float64) error
	Reserve(quotaKey string, queueUnit *framework.QueueUnitInfo) error
	Unreserve(quotaKey string, queueUnit *framework.QueueUnitInfo) error
	Resize(old, new *framework.QueueUnitInfo) error
	AddOrUpdateQuota(q *v1alpha1.ElasticQuota)
	DeleteQuota(q *v1alpha1.ElasticQuota)
	GetElasticQuotaInfo4Test(quotaName string) *ElasticQuotaInfo
	GetReserved() map[types.UID]string
	IsReserved(uid types.UID) bool
	MarkScheduling(uid types.UID)
	ClearScheduling(uid types.UID)
	IsScheduling(uid types.UID) bool
}

var once sync.Once
var internalCache *cacheImpl

type cacheImpl struct {
	lock sync.RWMutex

	reserved    map[types.UID]string
	scheduling  map[types.UID]bool // Track queueUnits that are being scheduled (between Reserve and DequeueComplete)
	quotas      map[string]*ElasticQuotaInfo
	quotaParent map[string]string
}

func newElasticQuotaCache() Cache {
	once.Do(func() {
		internalCache = buildCache()
	})
	return internalCache
}

func buildCache() *cacheImpl {
	internalCache = &cacheImpl{
		quotas:      make(map[string]*ElasticQuotaInfo),
		reserved:    make(map[types.UID]string),
		scheduling:  make(map[types.UID]bool),
		quotaParent: make(map[string]string),
	}
	go wait.Until(internalCache.updateUsageMetrics, time.Second*5, wait.NeverStop)

	klog.Infof("success build elastic quota v1alpha1 cache")

	return internalCache
}

func (c *cacheImpl) CheckUsage(queueUnitQuota string, queueUnit *framework.QueueUnitInfo, oversellRate float64) error {
	c.lock.RLock()
	defer c.lock.RUnlock()

	visited := sets.NewString()
	visitedQuotaPath := make([]string, 0)
	currentQuota := queueUnitQuota
	for currentQuota != KoordRootQuota {
		if visited.Has(currentQuota) {
			errMsg := fmt.Sprintf("CheckUsage found cycle reference, item:%v, quotaName:%v, visited quota: %s",
				queueUnit.Name, currentQuota, strings.Join(visitedQuotaPath, ","))
			klog.Infof("%v", errMsg)
			return errors.New(errMsg)
		}

		visited.Insert(currentQuota)
		visitedQuotaPath = append(visitedQuotaPath, currentQuota)

		info, exist := c.quotas[currentQuota]
		if !exist {
			errMsg := fmt.Sprintf("CheckUsage found quota not exist, item:%v, quotaName:%v, visited quota: %s",
				queueUnit.Name, currentQuota, strings.Join(visitedQuotaPath, ","))
			klog.Infof("%v", errMsg)
			return errors.New(errMsg)
		}

		err := info.CheckUsage(currentQuota, queueUnit, oversellRate)
		if err != nil {
			return err
		}

		if parent, ok := c.quotaParent[currentQuota]; ok {
			currentQuota = parent
		} else {
			break
		}
	}

	return nil
}

func (c *cacheImpl) Resize(old, new *framework.QueueUnitInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.unreserve(getQuotaName(old.Unit), old)
	if err != nil {
		return err
	}
	return c.reserve(getQuotaName(new.Unit), new)
}

func (c *cacheImpl) Reserve(queueUnitQuota string, queueUnit *framework.QueueUnitInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.reserve(queueUnitQuota, queueUnit)
}

func (c *cacheImpl) reserve(queueUnitQuota string, queueUnit *framework.QueueUnitInfo) error {
	if quota, exist := c.reserved[queueUnit.Unit.UID]; exist {
		if quota == queueUnitQuota {
			return nil
		}

		errMsg := fmt.Sprintf("reserve item failed, itemName:%v, quotaName:%v, "+
			"already reserved in another quota %v", queueUnit.Name, queueUnitQuota, quota)
		klog.Infof("%v", errMsg)
		return errors.New(errMsg)
	}

	currentQuota, exist := c.quotas[queueUnitQuota]
	if !exist {
		errMsg := fmt.Sprintf("reserve item failed, quota not found, "+
			"itemName:%v, quotaName:%v", queueUnit.Name, queueUnitQuota)
		klog.Infof("%v", errMsg)
		return errors.New(errMsg)
	}

	c.reserved[queueUnit.Unit.UID] = queueUnitQuota

	visited := sets.NewString()
	visitedQuotaPath := make([]string, 0)

	for currentQuota != nil {
		if visited.Has(key(currentQuota.Quota)) {
			klog.Errorf("reserve item failed, itemName:%v, quotaName:%v, "+
				"cycle visited found:%v", queueUnit.Name, queueUnitQuota, strings.Join(visitedQuotaPath, ","))
			break
		}

		visited.Insert(key(currentQuota.Quota))
		visitedQuotaPath = append(visitedQuotaPath, key(currentQuota.Quota))

		currentQuota.AddQueueUnit(key(currentQuota.Quota), queueUnit)

		if parent, ok := c.quotaParent[key(currentQuota.Quota)]; ok {
			currentQuota = c.quotas[parent]
		} else {
			break
		}
	}
	return nil
}

func (c *cacheImpl) Unreserve(queueUnitQuota string, queueUnit *framework.QueueUnitInfo) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.unreserve(queueUnitQuota, queueUnit)
}

func (c *cacheImpl) unreserve(queueUnitQuota string, queueUnit *framework.QueueUnitInfo) error {
	_, exist := c.reserved[queueUnit.Unit.UID]
	if !exist {
		return nil
	}

	currentQuota, exist := c.quotas[queueUnitQuota]
	if !exist {
		return fmt.Errorf("unreserve item failed, quota not found, "+
			"itemName:%v, quotaName:%v", queueUnit.Name, queueUnitQuota)
	}

	delete(c.reserved, queueUnit.Unit.UID)

	visited := sets.NewString()
	visitedQuotaPath := make([]string, 0)

	for currentQuota != nil {
		if visited.Has(key(currentQuota.Quota)) {
			klog.Errorf("unreserve item failed, itemName:%v, quotaName:%v, "+
				"cycle visited found:%v", queueUnit.Name, queueUnitQuota, strings.Join(visitedQuotaPath, ","))
			break
		}

		visited.Insert(key(currentQuota.Quota))
		visitedQuotaPath = append(visitedQuotaPath, key(currentQuota.Quota))

		currentQuota.DeleteQueueUnit(key(currentQuota.Quota), queueUnit)

		if parent, ok := c.quotaParent[key(currentQuota.Quota)]; ok {
			currentQuota = c.quotas[parent]
		} else {
			break
		}
	}
	return nil
}

func (c *cacheImpl) AddOrUpdateQuota(q *v1alpha1.ElasticQuota) {
	c.lock.Lock()
	defer c.lock.Unlock()

	info, exist := c.quotas[key(q)]
	if !exist {
		info = NewElasticQuotaInfo(q)
		c.quotas[key(q)] = info

		klog.Infof("create quotaInfo success, quotaName:%v, parentName:%v, min:%v, max:%v",
			key(q), getParentQuotaName(q), info.Min, info.Max)
	} else {
		// Replace entire Max/Min to avoid stale keys from previous updates
		info.Max = utils.TransResourceList(q.Spec.Max)
		info.Min = utils.TransResourceList(q.Spec.Min)
		klog.Infof("update quotaInfo success, quotaName:%v, parentName:%v, oldMin:%v, newMin:%v,"+
			"oldMax:%v, newMax:%v", key(q), getParentQuotaName(q), info.Min, utils.TransResourceList(q.Spec.Min),
			info.Max, utils.TransResourceList(q.Spec.Max))

		info.Quota = q
	}

	// Set parent, defaulting to KoordRootQuota if not specified (matching upstream behavior)
	c.quotaParent[key(q)] = getParentQuotaName(q)
}

func (c *cacheImpl) DeleteQuota(q *v1alpha1.ElasticQuota) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.quotas, key(q))
	delete(c.quotaParent, key(q))

	klog.Infof("delete quotaInfo success, quotaName:%v, parentName:%v", key(q), q.Labels[ParentQuotaNameLabelKey])
}

func (c *cacheImpl) GetReserved() map[types.UID]string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	result := make(map[types.UID]string)
	for uid, value := range c.reserved {
		result[uid] = value
	}

	return result
}

func (c *cacheImpl) IsReserved(uid types.UID) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	_, exist := c.reserved[uid]
	return exist
}

func (c *cacheImpl) MarkScheduling(uid types.UID) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.scheduling[uid] = true
	klog.V(4).Infof("Mark queueUnit as scheduling: %v", uid)
}

func (c *cacheImpl) ClearScheduling(uid types.UID) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.scheduling, uid)
	klog.V(4).Infof("Clear scheduling mark for queueUnit: %v", uid)
}

func (c *cacheImpl) IsScheduling(uid types.UID) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.scheduling[uid]
}

func (c *cacheImpl) GetElasticQuotaInfo4Test(quotaName string) *ElasticQuotaInfo {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.quotas[quotaName]
}

func (c *cacheImpl) updateUsageMetrics() {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for name, quota := range c.quotas {
		for k, v := range quota.Used {
			metrics.QuotaUsageByQuota.WithLabelValues(name, string(k)).Set(float64(v))
		}
	}
}
