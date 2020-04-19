package collect

import (
	"context"
	"hash/fnv"

	"bdware.org/libp2p/go-libp2p-collect/pb"
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
	msg          *Message
}

type reqItemCtx struct {
	*reqItem
	cancel func()
}

func (i *reqItemCtx) Cancel() {
	i.cancel()
}

// callback when a request is evicted.
func onReqCacheEvict(key interface{}, value interface{}) {
	// cancel context
	item := value.(*reqItemCtx)
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
// Add with the same reqid will make the previous one cancelled.
// When context is done, item will be removed from the cache;
// When the item is evicted, the context will be cancelled.
func (rc *requestCache) AddReqItem(ctx context.Context, reqid string, item *reqItem) {
	rc.RemoveReqItem(reqid)
	cctx, cc := context.WithCancel(ctx)
	itemCtx := &reqItemCtx{
		reqItem: item,
		cancel:  cc,
	}
	// Is there any better idea without using goroutine?
	go func() {
		<-cctx.Done()
		rc.RemoveReqItem(reqid)
	}()
	rc.cache.Add(reqid, itemCtx)
}

// RemoveReqItem .
func (rc *requestCache) RemoveReqItem(reqid string) {
	rc.cache.Remove(reqid)
}

// GetReqItem .
func (rc *requestCache) GetReqItem(reqid string) (out *reqItem, ok bool) {
	var v interface{}
	v, ok = rc.cache.Get(reqid)
	if ok {
		out = v.(*reqItemCtx).reqItem
	}
	return
}

// RemoveTopic .
func (rc *requestCache) RemoveTopic(topic string) {
	for _, k := range rc.cache.Keys() {
		if v, ok := rc.cache.Peek(k); ok {
			item := v.(*reqItemCtx).reqItem
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
type respItem struct{}

// newResponseCache .
func newResponseCache(size int) (*responseCache, error) {
	l, err := lru.New(size)
	return &responseCache{
		cache: l,
	}, err
}

func (r *responseCache) markSeen(resp *pb.Response) bool {
	var (
		err   error
		hash  uint64
		found bool
	)
	data := resp.Payload
	if err == nil {
		s := fnv.New64()
		_, err = s.Write(data)
		hash = s.Sum64()
	}
	if err == nil {
		_, found = r.cache.Get(hash)
	}
	if err == nil && !found {
		r.cache.Add(hash, respItem{})
		return true
	}
	return false
}
