package collect

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync/atomic"

	"bdware.org/libp2p/go-libp2p-collect/pb"
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
	apubsub *AsyncPubSub

	reqHandlers *requestHandlersMap
	// RequestCache is a cache of requests.
	// We don't know when there is no incoming response for a certain request.
	// We have to eliminate the out-dated request resource.
	// After elimination, the response related to this request will be ignored.
	rcache *requestCache
	ridgen ReqIDGenerator
}

// NewBasicPubSubCollector returns a new BasicPubSubCollector
func NewBasicPubSubCollector(h host.Host, opts ...InitOpt) (bpsc *BasicPubSubCollector, err error) {

	var (
		initopts *InitOpts
		c        *conf
		apubsub  *AsyncPubSub
		thandles *requestHandlersMap
		rcache   *requestCache
	)
	{
		initopts, err = NewInitOpts(opts)
	}
	if err == nil {
		c, err = checkOptConfAndGetInnerConf(initopts.Conf)
	}
	if err == nil {
		apubsub, err = NewAsyncPubSub(
			h,
			WithSelfNotif(true),
			WithCustomPubSubFactory(
				func(h host.Host) (*pubsub.PubSub, error) {
					return pubsub.NewRandomSub(
						// TODO: add context in initopts
						context.TODO(),
						h,
						// we do the pubsub with conf.RequestProtocol
						pubsub.WithCustomProtocols([]protocol.ID{c.requestProtocol}),
					)
				}),
		)
	}
	if err == nil {
		rcache, err = newRequestCache(c.requestCacheSize)
	}
	if err == nil {
		thandles = newRequestHandlerMap()
	}
	if err == nil {
		bpsc = &BasicPubSubCollector{
			conf:        c,
			seqno:       rand.Uint64(),
			host:        h,
			apubsub:     apubsub,
			reqHandlers: thandles,
			rcache:      rcache,
			ridgen:      initopts.IDGenerator,
		}
	}

	return
}

// Join the overlay network defined by topic
// Join the same topic is allowed here.
// Rejoin will refresh the requestHandler.
func (bpsc *BasicPubSubCollector) Join(topic string, opts ...JoinOpt) (err error) {
	var options *JoinOpts
	{
		options, err = NewJoinOptions(opts)
	}
	// subscribe the topic
	if err == nil {
		err = bpsc.apubsub.Subscribe(topic, bpsc.topicHandle)
	}

	// register request handler
	if err == nil {
		bpsc.reqHandlers.addOrReplaceReqHandler(topic, options.RequestHandler)
	}

	return
}

// Publish a serilized request payload.
func (bpsc *BasicPubSubCollector) Publish(topic string, payload []byte, opts ...PubOpt) (err error) {
	var (
		root    []byte
		rqID    string
		options *PubOpts
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
		options = NewPublishOptions(opts)

		// register notif handler
		bpsc.rcache.AddReqItem(rqID, &ReqItem{
			RecvRecvHandle: options.FinalRespHandle,
			Topic:          topic,
			Cancel:         options.Cancel,
		})

		// add stream handler when responses return
		bpsc.host.SetStreamHandler(bpsc.conf.responseProtocol, bpsc.streamHandler)

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
	bpsc.reqHandlers.delReqHandler(topic)
	bpsc.rcache.RemoveTopic(topic)
	return
}

// Close the BasicPubSubCollector.
func (bpsc *BasicPubSubCollector) Close() error {
	bpsc.reqHandlers.removeAll()
	bpsc.rcache.RemoveAll()
	return bpsc.apubsub.Close()
}

// topicHandle will be called when a request arrived.
func (bpsc *BasicPubSubCollector) topicHandle(topic string, msg *Message) {
	var (
		err       error
		ok        bool
		rqhandle  RequestHandler
		rqresult  *pb.Intermediate
		rqID      string
		rootID    peer.ID
		resp      *pb.Response
		respBytes []byte
		s         network.Stream
	)
	// unmarshal the received data into request struct
	req := &pb.Request{}
	err = req.Unmarshal(msg.Data)

	if err == nil {
		rqID = bpsc.ridgen(req)
		// Dispatch request to relative topic request handler,
		// which should be initialized in join function
		rqhandle, ok = bpsc.reqHandlers.getReqHandler(topic)
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
		bpsc.rcache.AddReqItem(rqID, &ReqItem{
			Topic:  topic,
			Cancel: cc,
		})
		// clean up later
		defer bpsc.rcache.RemoveReqItem(rqID)
		s, err = bpsc.host.NewStream(ctx, rootID, bpsc.conf.responseProtocol)

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
		reqItem *ReqItem
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
