package rcache

import (
	"bdware.org/libp2p/go-libp2p-collect/opt"
	lru "github.com/hashicorp/golang-lru"
)

// RequestCache .
type RequestCache struct {
	cache *lru.Cache
}

// ReqItem .
type ReqItem struct {
	RecvRecvHandle opt.RecvRespHandler
	Topic          string
	Cancel         func()
}

// callback when a request is evicted.
func onEvict(key interface{}, value interface{}) {
	// cancel context
	item := value.(*ReqItem)
	item.Cancel()
	// TODO: add logging
}

// NewRequestCache .
func NewRequestCache(size int) (*RequestCache, error) {
	l, err := lru.NewWithEvict(size, onEvict)
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
