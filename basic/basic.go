package basicpsc

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"sync/atomic"

	psc "bdware.org/libp2p/go-libp2p-collect"
	"bdware.org/libp2p/go-libp2p-collect/apsub"
	"bdware.org/libp2p/go-libp2p-collect/pb"
	lru "github.com/hashicorp/golang-lru"
	host "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
)

// BasicPubSubCollector implements of psc.BasicPubSubCollector Interface.
// It broadcasts request by libp2p.PubSub.
// When the remote nodes receive the request, they will try to dial the request node
// and return the response directly.
type BasicPubSubCollector struct {
	// conf is static configuration of BasicPubSubCollector
	conf innerConf
	// seqno is an increasing number to give each request ID
	seqno uint64

	host host.Host
	// a wrapper of apsub system
	apubsub *apsub.AsyncPubSub

	thandles *topicHandlers
	// requestCache is a cache of requests.
	// We don't know when there is no incoming response for a certain request.
	// We have to eliminate the out-dated request resource.
	// After elimination, the response related to this request will be ignored.
	rcache *requestCache
	ridgen ReqIDGenerator
}

// NewBasicPubSubCollector returns a new BasicPubSubCollector
func NewBasicPubSubCollector(h host.Host, opts ...Option) (bpsc *BasicPubSubCollector, err error) {
	var (
		top *apsub.AsyncPubSub
	)
	bpsc = &BasicPubSubCollector{
		conf:  newDefaultInnerConf(),
		seqno: rand.Uint64(),
		host:  h,
		// top: top,
		// top's initialization should be put off, for we need to know request protocol
		thandles: newTopicHandlers(),
		// requestCache:    newRequestCache(),
		// initialize requestCache later
		ridgen: defaultReqIDGenerator(),
	}
	//apply opts
	for _, opt := range opts {
		if err == nil {
			err = opt(bpsc)
		}
	}
	if err == nil {
		// initialize apsub
		top, err = apsub.NewTopics(
			h,
			apsub.WithSelfNotif(true),
			apsub.WithCustomPubSubFactory(
				newPubsubFactoryWithProtocol(bpsc.conf.RequestProtocol),
			),
		)
	}
	if err == nil {
		bpsc.apubsub = top

		// initialize requestcache
		bpsc.rcache, err = newRequestCache(bpsc.conf.RequestBufSize)
	}
	return
}

// Join the overlay network defined by topic
// Join the same topic is allowed here.
// Rejoin will refresh the requestHandler.
func (bpsc *BasicPubSubCollector) Join(topic string, opts ...psc.JoinOpt) (err error) {
	options := psc.NewJoinOptions(opts)

	// subscribe the topic
	if err == nil {
		err = bpsc.apubsub.Subscribe(topic, bpsc.topicHandle)
	}

	// register request handler
	if err == nil {
		bpsc.thandles.addOrReplaceReqHandler(topic, options.RequestHandler)
	}

	return
}

// Publish a serilized request payload.
func (bpsc *BasicPubSubCollector) Publish(topic string, payload []byte, opts ...psc.PubOpt) (err error) {
	var (
		root    []byte
		rqID    string
		options *psc.PublishOptions
		tosend  []byte
	)
	// assemble the request struct
	root, err = bpsc.host.ID().MarshalBinary()
	if err == nil {
		seq := atomic.AddUint64(&(bpsc.seqno), 1)
		req := &pb.Request{
			Control: &pb.RequestControl{
				Root:  root,
				Seqno: seq,
			},
			Payload: payload,
		}
		rqID = bpsc.ridgen(req)

		tosend, err = req.Marshal()
	}
	if err == nil {
		options = psc.NewPublishOptions(opts)

		// register notif handler
		bpsc.rcache.addReqItem(rqID, reqItem{
			recvRecvHandle: options.RecvRespHandle,
			topic:          topic,
			cancel:         options.Cancel,
		})

		// add stream handler when responses return
		bpsc.host.SetStreamHandler(bpsc.conf.ResponseProtocol, bpsc.streamHandler)

		//  publish marshaled request
		err = bpsc.apubsub.Publish(options.RequestContext, topic, tosend)

		// delete reqItem when options.Ctx expires
		go func() {
			select {
			case <-options.RequestContext.Done():
				bpsc.rcache.removeReqItem(rqID)
			}
		}()
	}
	return
}

// Leave the overlay.
// The registered topichandles and responseHandlers will be closed.
func (bpsc *BasicPubSubCollector) Leave(topic string) (err error) {
	err = bpsc.apubsub.Unsubscribe(topic)
	bpsc.thandles.delReqHandler(topic)
	bpsc.rcache.removeTopic(topic)
	return
}

// Close the BasicPubSubCollector.
func (bpsc *BasicPubSubCollector) Close() error {
	bpsc.thandles.removeAll()
	bpsc.rcache.removeAll()
	return bpsc.apubsub.Close()
}

// topicHandle will be called when a request arrived.
func (bpsc *BasicPubSubCollector) topicHandle(topic string, data []byte) {
	var (
		err       error
		ok        bool
		rqhandle  psc.RequestHandler
		rqresult  *pb.Intermediate
		rqID      string
		rootID    peer.ID
		resp      *pb.Response
		respBytes []byte
		s         network.Stream
	)
	// unmarshal the received data into request struct
	req := &pb.Request{}
	err = req.Unmarshal(data)

	if err == nil {
		rqID = bpsc.ridgen(req)
		// Dispatch request to relative topic request handler,
		// which should be initialized in join function
		rqhandle, ok = bpsc.thandles.getReqHandler(topic)
	}
	if !ok {
		err = fmt.Errorf("cannot find request handler for topic %s", topic)
	}
	if err == nil {
		// handle request
		// TODO: add timeout
		rqresult = rqhandle(context.Background(), req)

		// After request is processed, we will have a Intermediate.
		// We send the response to the root node directly if sendback is set to true.
		// Another protocol will be used to inform the root node.
		if !rqresult.Sendback {
			// drop any response if sendback is false
			goto end
		}

		// assemble the response
		resp = &pb.Response{
			Control: &pb.ResponseControl{
				RequestId: rqID,
			},
			Payload: rqresult.Payload,
		}
		respBytes, err = resp.Marshal()
	}
	if err == nil {
		// find the root peer_id
		rootID = peer.ID(req.Control.Root)

		// receive self-published message
		if rootID == bpsc.host.ID() {
			bpsc.handleResponse(resp)
			goto end
		}

		// send payload
		ctx, cc := context.WithCancel(context.Background())
		bpsc.rcache.addReqItem(rqID, reqItem{
			topic:  topic,
			cancel: cc,
		})
		// clean up later
		defer bpsc.rcache.removeReqItem(rqID)
		s, err = bpsc.host.NewStream(ctx, rootID, bpsc.conf.ResponseProtocol)

	}
	// everything is done, send payload by write to stream
	if err == nil {
		// don't forget to close the stream
		defer s.Close()
		_, err = s.Write(respBytes)
	}
end:
	//TODO: check error and logging
}

// streamHandler reads response from stream, and calls related notifHandler
func (bpsc *BasicPubSubCollector) streamHandler(s network.Stream) {
	var (
		respBytes []byte
		err       error
	)
	defer s.Close()
	respBytes, err = ioutil.ReadAll(s)
	if err == nil {
		err = bpsc.handleResponseBytes(respBytes)
	}

}

// handleResponsePayload unmarshals the ResponseBytes, and calls the notifHandler
func (bpsc *BasicPubSubCollector) handleResponseBytes(respBytes []byte) (err error) {
	var (
		resp *pb.Response
	)
	if err == nil {
		resp = &pb.Response{}
		err = resp.Unmarshal(respBytes)
	}
	if err == nil {
		err = bpsc.handleResponse(resp)
	}
	return
}

// handleResponse calls notifHandler
func (bpsc *BasicPubSubCollector) handleResponse(resp *pb.Response) (err error) {
	var (
		reqID   string
		reqItem reqItem
		ok      bool
	)
	if resp == nil {
		err = fmt.Errorf("unexpect nil response")
	}
	if err == nil {
		reqID = resp.Control.RequestId
		reqItem, ok = bpsc.rcache.getReqItem(reqID)
		if !ok {
			err = fmt.Errorf("cannot find reqitem for request %s", reqID)
		}
	}
	if err == nil {
		reqItem.recvRecvHandle(resp)
	}
	return err
}

type topicHandlers struct {
	lock     sync.RWMutex
	handlers map[string]psc.RequestHandler
}

func newTopicHandlers() *topicHandlers {
	return &topicHandlers{
		lock:     sync.RWMutex{},
		handlers: make(map[string]psc.RequestHandler),
	}
}

func (td *topicHandlers) addOrReplaceReqHandler(topic string, rqhandle psc.RequestHandler) {
	td.lock.Lock()
	defer td.lock.Unlock()
	td.handlers[topic] = rqhandle
}

func (td *topicHandlers) addReqHandler(topic string, rqhandle psc.RequestHandler) error {
	td.lock.Lock()
	defer td.lock.Unlock()
	if _, ok := td.handlers[topic]; ok {
		return fmt.Errorf("unexpected rqhandle exists")
	}
	td.handlers[topic] = rqhandle
	return nil
}

func (td *topicHandlers) delReqHandler(topic string) {
	td.lock.Lock()
	defer td.lock.Unlock()
	delete(td.handlers, topic)
}

func (td *topicHandlers) getReqHandler(topic string) (psc.RequestHandler, bool) {
	td.lock.RLock()
	defer td.lock.RUnlock()
	rqhandle, ok := td.handlers[topic]
	return rqhandle, ok
}

func (td *topicHandlers) removeAll() {
	for k := range td.handlers {
		delete(td.handlers, k)
	}
}

type requestCache struct {
	Cache *lru.Cache
}

type reqItem struct {
	recvRecvHandle psc.RecvRespHandler
	topic          string
	cancel         func()
}

// callback when a request is evicted.
func onEvict(key interface{}, value interface{}) {
	// cancel context
	item := value.(reqItem)
	item.cancel()
	// TODO: add logging
}

func newRequestCache(size int) (*requestCache, error) {
	l, err := lru.NewWithEvict(size, onEvict)
	return &requestCache{
		Cache: l,
	}, err
}

func (rc *requestCache) addReqItem(reqid string, reqItem reqItem) {
	rc.Cache.Add(reqid, reqItem)
}

func (rc *requestCache) removeReqItem(reqid string) {
	rc.Cache.Remove(reqid)
}

func (rc *requestCache) getReqItem(reqid string) (out reqItem, ok bool) {
	var v interface{}
	v, ok = rc.Cache.Get(reqid)
	out, ok = v.(reqItem)
	return
}

func (rc *requestCache) removeTopic(topic string) {
	for _, k := range rc.Cache.Keys() {
		if v, ok := rc.Cache.Peek(k); ok {
			item := v.(reqItem)
			if item.topic == topic {
				rc.Cache.Remove(k)
			}
		}
	}
}

func (rc *requestCache) removeAll() {
	rc.Cache.Purge()
}
