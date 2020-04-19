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

type reqWorker struct {
	item    *reqItem
	cc      func()
	onClose func()
}

func newReqWorker(ctx context.Context, item *reqItem, onClose func()) *reqWorker {
	cctx, cc := context.WithCancel(ctx)
	r := &reqWorker{
		item:    item,
		cc:      cc,
		onClose: onClose,
	}
	go r.loop(cctx)
	return r
}

func (rw *reqWorker) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			if rw.onClose != nil {
				rw.onClose()
			}
			break
		}
	}
}

func (rw *reqWorker) close() {
	rw.cc()
}

// callback when a request is evicted.
func onReqCacheEvict(key interface{}, value interface{}) {
	// cancel context
	w := value.(*reqWorker)
	w.close()
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
	w := newReqWorker(ctx, item, func() {
		rc.RemoveReqItem(reqid)
	})
	rc.cache.Add(reqid, w)
}

// RemoveReqItem .
func (rc *requestCache) RemoveReqItem(reqid string) {
	rc.cache.Remove(reqid)
}

// GetReqItem .
func (rc *requestCache) GetReqItem(reqid string) (out *reqItem, ok bool, cancel func()) {
	var w *reqWorker
	w, ok = rc.GetReqWorker(reqid)
	if w != nil {
		out, cancel = w.item, w.cc
	}
	return
}

// GetReqItem .
func (rc *requestCache) GetReqWorker(reqid string) (w *reqWorker, ok bool) {
	var v interface{}
	v, ok = rc.cache.Get(reqid)
	if ok {
		w = v.(*reqWorker)
	}
	return
}

// RemoveTopic .
func (rc *requestCache) RemoveTopic(topic string) {
	for _, k := range rc.cache.Keys() {
		if v, ok := rc.cache.Peek(k); ok {
			w := v.(*reqWorker)
			if w.item.topic == topic {
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
	if err == nil {
		s := fnv.New64()
		_, err = s.Write(resp.Payload)
		_, err = s.Write([]byte(resp.Control.RequestId))
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
