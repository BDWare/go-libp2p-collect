package collect

import (
	lru "github.com/hashicorp/golang-lru"
)

// RequestCache .
// RequestCache is used to store the request control message,
// which is for response routing.
type requestCache struct {
	cache *lru.Cache
}

// ReqItem .
type ReqItem struct {
	RecvRecvHandle FinalRespHandler
	Topic          string
	Cancel         func()
}

// callback when a request is evicted.
func onReqCacheEvict(key interface{}, value interface{}) {
	// cancel context
	item := value.(*ReqItem)
	item.Cancel()
	// TODO: add logging
}

// newRequestCache .
func newRequestCache(size int) (*requestCache, error) {
	l, err := lru.NewWithEvict(size, onReqCacheEvict)
	return &requestCache{
		cache: l,
	}, err
}

// AddReqItem .
func (rc *requestCache) AddReqItem(reqid string, reqItem *ReqItem) {
	rc.cache.Add(reqid, reqItem)
}

// RemoveReqItem .
func (rc *requestCache) RemoveReqItem(reqid string) {
	rc.cache.Remove(reqid)
}

// GetReqItem .
func (rc *requestCache) GetReqItem(reqid string) (out *ReqItem, ok bool) {
	var v interface{}
	v, ok = rc.cache.Get(reqid)
	out, ok = v.(*ReqItem)
	return
}

// RemoveTopic .
func (rc *requestCache) RemoveTopic(topic string) {
	for _, k := range rc.cache.Keys() {
		if v, ok := rc.cache.Peek(k); ok {
			item := v.(*ReqItem)
			if item.Topic == topic {
				rc.cache.Remove(k)
			}
		}
	}
}

// RemoveAll .
func (rc *requestCache) RemoveAll() {
	rc.cache.Purge()
}

// ResponseCache .
// ResponseCache is used to deduplicate the response
type responseCache struct {
	cache *lru.Cache
}

// ResponseItem .
type ResponseItem struct {
}

// newResponseCache .
func newResponseCache(size int) (*responseCache, error) {
	l, err := lru.NewWithEvict(size, onRespCacheEvict)
	return &responseCache{
		cache: l,
	}, err
}

func onRespCacheEvict(key interface{}, value interface{}) {
}
