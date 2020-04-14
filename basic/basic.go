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
	"bdware.org/libp2p/go-libp2p-collect/opt"
	"bdware.org/libp2p/go-libp2p-collect/pb"
	rc "bdware.org/libp2p/go-libp2p-collect/rcache"
	pubsub "bdware.org/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

// BasicPubSubCollector implements of psc.BasicPubSubCollector Interface.
// It broadcasts request by libp2p.PubSub.
// When the remote nodes receive the request, they will try to dial the request node
// and return the response directly.
type BasicPubSubCollector struct {
	// conf is static configuration of BasicPubSubCollector
	conf *conf
	// seqno is an increasing number to give each request ID
	seqno uint64

	host host.Host
	// a wrapper of apsub system
	apubsub *apsub.AsyncPubSub

	thandles *topicHandlers
	// RequestCache is a cache of requests.
	// We don't know when there is no incoming response for a certain request.
	// We have to eliminate the out-dated request resource.
	// After elimination, the response related to this request will be ignored.
	rcache *rc.RequestCache
	ridgen ReqIDGenerator
}

// NewBasicPubSubCollector returns a new BasicPubSubCollector
func NewBasicPubSubCollector(h host.Host, opts ...Option) (bpsc *BasicPubSubCollector, err error) {

	var (
		initopts *opt.InitOpts
		c        *conf
		apubsub  *apsub.AsyncPubSub
		thandles *topicHandlers
		rcache   *rc.RequestCache
	)
	{
		initopts, err = opt.NewInitOpts(opts)
	}
	if err == nil {
		c, err = checkOptConfAndGetInnerConf(initopts.Conf)
	}
	if err == nil {
		apubsub, err = apsub.NewAsyncPubSub(
			h,
			apsub.WithSelfNotif(true),
			apsub.WithCustomPubSubFactory(
				func(h host.Host) (*pubsub.PubSub, error) {
					return pubsub.NewRandomSub(
						// TODO: add context in initopts
						context.TODO(),
						h,
						// we do the pubsub with conf.RequestProtocol
						pubsub.WithCustomProtocols([]protocol.ID{c.RequestProtocol}),
					)
				}),
		)
	}
	if err == nil {
		rcache, err = rc.NewRequestCache(c.RequestBufSize)
	}
	if err == nil {
		thandles = newTopicHandlers()
	}
	if err == nil {
		bpsc = &BasicPubSubCollector{
			conf:     c,
			seqno:    rand.Uint64(),
			host:     h,
			apubsub:  apubsub,
			thandles: thandles,
			rcache:   rcache,
			ridgen:   initopts.IDGenerator,
		}
	}

	return
}

// Join the overlay network defined by topic
// Join the same topic is allowed here.
// Rejoin will refresh the requestHandler.
func (bpsc *BasicPubSubCollector) Join(topic string, opts ...psc.JoinOpt) (err error) {
	var options *opt.JoinOpts
	{
		options, err = opt.NewJoinOptions(opts)
	}
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
		options *opt.PubOpts
		tosend  []byte
	)
	// assemble the request struct
	root, err = bpsc.host.ID().MarshalBinary()
	if err == nil {
		seq := atomic.AddUint64(&(bpsc.seqno), 1)
		req := &pb.Request{
			Control: pb.RequestControl{
				Root:  root,
				Seqno: seq,
			},
			Payload: payload,
		}
		rqID = bpsc.ridgen(req)

		tosend, err = req.Marshal()
	}
	if err == nil {
		options = opt.NewPublishOptions(opts)

		// register notif handler
		bpsc.rcache.AddReqItem(rqID, &rc.ReqItem{
			RecvRecvHandle: options.FinalRespHandle,
			Topic:          topic,
			Cancel:         options.Cancel,
		})

		// add stream handler when responses return
		bpsc.host.SetStreamHandler(bpsc.conf.ResponseProtocol, bpsc.streamHandler)

		//  publish marshaled request
		err = bpsc.apubsub.Publish(options.RequestContext, topic, tosend)

		// delete reqItem when options.Ctx expires
		go func() {
			select {
			case <-options.RequestContext.Done():
				bpsc.rcache.RemoveReqItem(rqID)
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
	bpsc.rcache.RemoveTopic(topic)
	return
}

// Close the BasicPubSubCollector.
func (bpsc *BasicPubSubCollector) Close() error {
	bpsc.thandles.removeAll()
	bpsc.rcache.RemoveAll()
	return bpsc.apubsub.Close()
}

// topicHandle will be called when a request arrived.
func (bpsc *BasicPubSubCollector) topicHandle(topic string, data []byte) {
	var (
		err       error
		ok        bool
		rqhandle  opt.RequestHandler
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
			Control: pb.ResponseControl{
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
		bpsc.rcache.AddReqItem(rqID, &rc.ReqItem{
			Topic:  topic,
			Cancel: cc,
		})
		// clean up later
		defer bpsc.rcache.RemoveReqItem(rqID)
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
		reqItem *rc.ReqItem
		ok      bool
	)
	if resp == nil {
		err = fmt.Errorf("unexpect nil response")
	}
	if err == nil {
		reqID = resp.Control.RequestId
		reqItem, ok = bpsc.rcache.GetReqItem(reqID)
		if !ok {
			err = fmt.Errorf("cannot find reqitem for request %s", reqID)
		}
	}
	if err == nil {
		reqItem.RecvRecvHandle(resp)
	}
	return err
}

type topicHandlers struct {
	lock     sync.RWMutex
	handlers map[string]opt.RequestHandler
}

func newTopicHandlers() *topicHandlers {
	return &topicHandlers{
		lock:     sync.RWMutex{},
		handlers: make(map[string]opt.RequestHandler),
	}
}

func (td *topicHandlers) addOrReplaceReqHandler(topic string, rqhandle opt.RequestHandler) {
	td.lock.Lock()
	defer td.lock.Unlock()
	td.handlers[topic] = rqhandle
}

func (td *topicHandlers) addReqHandler(topic string, rqhandle opt.RequestHandler) error {
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

func (td *topicHandlers) getReqHandler(topic string) (opt.RequestHandler, bool) {
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

// Option is type alias
type Option = opt.InitOpt

// ReqIDGenerator is type alias
type ReqIDGenerator = opt.ReqIDGenerator

type conf struct {
	RequestProtocol  protocol.ID
	ResponseProtocol protocol.ID
	RequestBufSize   int
}

func checkOptConfAndGetInnerConf(optConf *opt.Conf) (inner *conf, err error) {
	if optConf.ProtocolPrefix == "" {
		err = fmt.Errorf("unexpected nil Prefix")
	}
	if optConf.RequestCacheSize < 0 {
		err = fmt.Errorf("unexpected negetive RequestBufSize")
	}
	if err == nil {
		inner = &conf{
			RequestProtocol:  protocol.ID(optConf.ProtocolPrefix + "/request"),
			ResponseProtocol: protocol.ID(optConf.ProtocolPrefix + "/response"),
			RequestBufSize:   optConf.RequestCacheSize,
		}
	}
	return
}
