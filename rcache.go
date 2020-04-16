package collect

import (
	lru "github.com/hashicorp/golang-lru"
)

// RequestCache .
// RequestCache is used to store the request control message,
// which is for response routing.
type RequestCache struct {
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

// NewRequestCache .
func NewRequestCache(size int) (*RequestCache, error) {
	l, err := lru.NewWithEvict(size, onReqCacheEvict)
	return &RequestCache{
		cache: l,
	}, err
}

// AddReqItem .
func (rc *RequestCache) AddReqItem(reqid string, reqItem *ReqItem) {
	rc.cache.Add(reqid, reqItem)
}

// RemoveReqItem .
func (rc *RequestCache) RemoveReqItem(reqid string) {
	rc.cache.Remove(reqid)
}

// GetReqItem .
func (rc *RequestCache) GetReqItem(reqid string) (out *ReqItem, ok bool) {
	var v interface{}
	v, ok = rc.cache.Get(reqid)
	out, ok = v.(*ReqItem)
	return
}

// RemoveTopic .
func (rc *RequestCache) RemoveTopic(topic string) {
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
func (rc *RequestCache) RemoveAll() {
	rc.cache.Purge()
}

// ResponseCache .
// ResponseCache is used to deduplicate the response
type ResponseCache struct {
	cache *lru.Cache
}

// ResponseItem .
type ResponseItem struct {
}

// NewResponseCache .
func NewResponseCache(size int) (*ResponseCache, error) {
	l, err := lru.NewWithEvict(size, onRespCacheEvict)
	return &ResponseCache{
		cache: l,
	}, err
}

func onRespCacheEvict(key interface{}, value interface{}) {
}
