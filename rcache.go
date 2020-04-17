package collect

import (
	lru "github.com/hashicorp/golang-lru"
)

// requestCache .
// requestCache is used to store the request control message,
// which is for response routing.
type requestCache struct {
	cache *lru.Cache
}

// reqstItem .
type reqItem struct {
	finalHandler FinalRespHandler
	topic        string
	cancel       func()
}

// callback when a request is evicted.
func onReqCacheEvict(key interface{}, value interface{}) {
	// cancel context
	item := value.(*reqItem)
	item.cancel()
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
func (rc *requestCache) AddReqItem(reqid string, item *reqItem) {
	rc.cache.Add(reqid, item)
}

// RemoveReqItem .
func (rc *requestCache) RemoveReqItem(reqid string) {
	rc.cache.Remove(reqid)
}

// GetReqItem .
func (rc *requestCache) GetReqItem(reqid string) (out *reqItem, ok bool) {
	var v interface{}
	v, ok = rc.cache.Get(reqid)
	out, ok = v.(*reqItem)
	return
}

// RemoveTopic .
func (rc *requestCache) RemoveTopic(topic string) {
	for _, k := range rc.cache.Keys() {
		if v, ok := rc.cache.Peek(k); ok {
			item := v.(*reqItem)
			if item.topic == topic {
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

// respItem .
type respItem struct {
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
