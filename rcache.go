package collect

import (
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

type requestHandlersMap struct {
	lock     sync.RWMutex
	handlers map[string]RequestHandler
}

func newRequestHandlerMap() *requestHandlersMap {
	return &requestHandlersMap{
		lock:     sync.RWMutex{},
		handlers: make(map[string]RequestHandler),
	}
}

func (td *requestHandlersMap) addOrReplaceReqHandler(topic string, rqhandle RequestHandler) {
	td.lock.Lock()
	defer td.lock.Unlock()
	td.handlers[topic] = rqhandle
}

func (td *requestHandlersMap) addReqHandler(topic string, rqhandle RequestHandler) error {
	td.lock.Lock()
	defer td.lock.Unlock()
	if _, ok := td.handlers[topic]; ok {
		return fmt.Errorf("unexpected rqhandle exists")
	}
	td.handlers[topic] = rqhandle
	return nil
}

func (td *requestHandlersMap) delReqHandler(topic string) {
	td.lock.Lock()
	defer td.lock.Unlock()
	delete(td.handlers, topic)
}

func (td *requestHandlersMap) getReqHandler(topic string) (RequestHandler, bool) {
	td.lock.RLock()
	defer td.lock.RUnlock()
	rqhandle, ok := td.handlers[topic]
	return rqhandle, ok
}

func (td *requestHandlersMap) removeAll() {
	td.lock.Lock()
	defer td.lock.Unlock()
	for k := range td.handlers {
		delete(td.handlers, k)
	}
}

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
