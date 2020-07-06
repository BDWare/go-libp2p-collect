package collect

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync/atomic"

	"bdware.org/libp2p/go-libp2p-collect/pb"
	"bdware.org/libp2p/go-libp2p-collect/pubsub"
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
	apsub *AsyncPubSub

	// RequestCache is a cache of requests.
	// We don't know when there is no incoming response for a certain request.
	// We have to eliminate the out-dated request resource.
	// After elimination, the response related to this request will be ignored.
	reqCache *requestCache
	ridgen   ReqIDGenerator
	logger   *standardLogger
}

// NewBasicPubSubCollector returns a new BasicPubSubCollector
func NewBasicPubSubCollector(h host.Host, options ...InitOpt) (bpsc *BasicPubSubCollector, err error) {

	var (
		opts     *InitOpts
		c        *conf
		apsub    *AsyncPubSub
		reqCache *requestCache
	)
	{
		opts, err = NewInitOpts(options)
	}
	if err == nil {
		c, err = checkOptConfAndGetInnerConf(&opts.Conf)
	}
	if err == nil {
		apsub, err = NewAsyncPubSub(
			h,
			WithSelfNotif(true),
			WithCustomPubSubFactory(
				func(h host.Host) (*pubsub.PubSub, error) {
					return pubsub.NewRandomSub(
						// TODO: add context in initopts
						context.TODO(),
						h,
						defaultRandomSubSize,
						// we do the pubsub with conf.RequestProtocol
						pubsub.WithCustomProtocols([]protocol.ID{c.requestProtocol}),
					)
				}),
		)
	}
	if err == nil {
		reqCache, err = newRequestCache(c.requestCacheSize)
	}
	if err == nil {
		bpsc = &BasicPubSubCollector{
			conf:     c,
			seqno:    rand.Uint64(),
			host:     h,
			apsub:    apsub,
			reqCache: reqCache,
			ridgen:   opts.IDGenerator,
			logger:   &standardLogger{opts.Logger},
		}

		// add stream handler when responses return
		bpsc.host.SetStreamHandler(bpsc.conf.responseProtocol, bpsc.streamHandler)
	}

	return
}

// Join the overlay network defined by topic
// Join the same topic is allowed here.
// Rejoin will refresh the requestHandler.
func (bpsc *BasicPubSubCollector) Join(topic string, opts ...JoinOpt) (err error) {

	bpsc.logger.funcCall("debug", "Join",
		map[string]interface{}{
			"topic": topic,
		})

	var options *JoinOpts
	{
		options, err = NewJoinOptions(opts)
	}
	// subscribe the topic
	if err == nil {
		err = bpsc.apsub.Subscribe(topic, bpsc.topicHandle)
	}

	// register request handler
	if err == nil {
		err = bpsc.apsub.SetTopicItem(topic, requestHandlerKey, options.RequestHandler)
	}

	if err != nil {
		bpsc.logger.error(err)
	}

	return
}

// Publish a serilized request payload.
func (bpsc *BasicPubSubCollector) Publish(topic string, payload []byte, opts ...PubOpt) (err error) {

	bpsc.logger.funcCall("debug", "publish", map[string]interface{}{
		"topic": topic,
	})

	var (
		rqID    RequestID
		options *PubOpts
		tosend  []byte
	)
	{
		options, err = NewPublishOptions(opts)
	}
	if err == nil {
		myself := bpsc.host.ID()
		req := &Request{
			Control: pb.RequestControl{
				Requester: myself,
				Sender:    myself,
				Seqno:     atomic.AddUint64(&(bpsc.seqno), 1),
			},
			Payload: payload,
		}
		rqID = bpsc.ridgen(req)

		tosend, err = req.Marshal()
	}
	if err == nil {
		// register notif handler
		bpsc.reqCache.AddReqItem(options.RequestContext, rqID, &reqItem{
			finalHandler: options.FinalRespHandle,
			topic:        topic,
		})

		//  publish marshaled request
		err = bpsc.apsub.Publish(options.RequestContext, topic, tosend)

	}

	if err != nil {
		bpsc.logger.error(err)
	}

	return
}

// Leave the overlay.
// The registered topichandles and responseHandlers will be closed.
func (bpsc *BasicPubSubCollector) Leave(topic string) (err error) {

	bpsc.logger.funcCall("debug", "leave", map[string]interface{}{
		"topic": topic,
	})

	err = bpsc.apsub.Unsubscribe(topic)
	bpsc.reqCache.RemoveTopic(topic)

	if err != nil {
		bpsc.logger.error(err)
	}
	return
}

// Close the BasicPubSubCollector.
func (bpsc *BasicPubSubCollector) Close() (err error) {

	bpsc.logger.funcCall("debug", "close", nil)

	bpsc.reqCache.RemoveAll()
	err = bpsc.apsub.Close()

	if err != nil {
		bpsc.logger.error(err)
	}
	return
}

// topicHandle will be called when a request arrived.
func (bpsc *BasicPubSubCollector) topicHandle(topic string, msg *Message) {

	bpsc.logger.funcCall("debug", "topicHandle", map[string]interface{}{
		"topic": topic,
		"receive-from": func() string {
			if msg == nil {
				return ""
			}
			return msg.ReceivedFrom.Pretty()
		}(),
		"from": func() string {
			if msg == nil {
				return ""
			}
			pid, err := peer.IDFromBytes(msg.From)
			if err != nil {
				return ""
			}
			return pid.Pretty()
		}(),
	})

	var err error
	if msg == nil {
		err = fmt.Errorf("unexpected nil msg")
	}

	var (
		ok          bool
		req         *Request
		rqhandleRaw interface{}
		rqhandle    RequestHandler
		rqresult    *Intermediate
		rqID        RequestID
		resp        *Response
		respBytes   []byte
		s           network.Stream
		ctx         context.Context
	)
	{
		ctx = context.TODO()
		// unmarshal the received data into request struct
		req = &Request{}
		err = req.Unmarshal(msg.Data)
	}
	if err == nil {
		rqID = bpsc.ridgen(req)

		// not self-publish, add a reqItem
		if msg.ReceivedFrom != bpsc.host.ID() {
			bpsc.reqCache.AddReqItem(context.Background(), rqID, &reqItem{
				msg:   msg,
				topic: topic,
			})
			// clean up later
			defer bpsc.reqCache.RemoveReqItem(rqID)
		}
	}

	if err == nil {
		// Dispatch request to relative topic request handler,
		// which should be initialized in join function
		rqhandleRaw, err = bpsc.apsub.LoadTopicItem(topic, requestHandlerKey)
		if err != nil {
			err = fmt.Errorf("cannot find request handler:%w", err)
		} else {
			rqhandle, ok = rqhandleRaw.(RequestHandler)
		}
		if err == nil && !ok {
			err = fmt.Errorf("unexpected request handler type")
		}
	}

	if err == nil {
		// handle request
		rqresult = rqhandle(ctx, req)

		// After request is processed, we will have a Intermediate.
		// We send the response to the root node directly if sendback is set to true.
		// Another protocol will be used to inform the root node.
		if rqresult == nil || !rqresult.Sendback {
			// drop any response if sendback is false or sendback == nil
			bpsc.logger.message("info", "not sendback")

			goto handleEnd
		}

		// assemble the response
		resp = &Response{
			Control: pb.ResponseControl{
				RequestId: rqID,
			},
			Payload: rqresult.Payload,
		}
		respBytes, err = resp.Marshal()
	}
	if err == nil {
		// receive self-published message
		if req.Control.Requester == bpsc.host.ID() {

			bpsc.logger.message("info", "self publish")

			err = bpsc.handleFinalResponse(resp)
			goto handleEnd
		}

		bpsc.logger.message("debug", "answering response")

		s, err = bpsc.host.NewStream(ctx, req.Control.Requester, bpsc.conf.responseProtocol)

	}
	// everything is done, send payload by write to stream
	if err == nil {
		// don't forget to close the stream
		defer s.Close()
		_, err = s.Write(respBytes)
	}

handleEnd:
	if err != nil {
		bpsc.logger.error(err)
	}
}

// streamHandler reads response from stream, and calls related notifHandler
func (bpsc *BasicPubSubCollector) streamHandler(s network.Stream) {

	bpsc.logger.funcCall("debug", "streamHandler", map[string]interface{}{
		"from": s.Conn().RemotePeer().Pretty(),
	})
	var (
		respBytes []byte
		err       error
	)
	defer s.Close()
	respBytes, err = ioutil.ReadAll(s)
	if err == nil {
		err = bpsc.handleResponseBytes(respBytes)
	}

	if err != nil {
		bpsc.logger.error(err)
	}

}

// handleResponsePayload unmarshals the ResponseBytes, and calls the notifHandler
func (bpsc *BasicPubSubCollector) handleResponseBytes(respBytes []byte) (err error) {
	var (
		resp *Response
	)
	if err == nil {
		resp = &Response{}
		err = resp.Unmarshal(respBytes)
	}
	if err == nil {
		err = bpsc.handleFinalResponse(resp)
	}
	return
}

// handleFinalResponse calls finalResponseHandler
func (bpsc *BasicPubSubCollector) handleFinalResponse(resp *Response) (err error) {

	bpsc.logger.funcCall("debug", "handleFinalResponse", map[string]interface{}{
		"receive-from": func() string {
			if resp == nil {
				return ""
			}
			return resp.Control.Sender.Pretty()
		}(),
	})
	if resp == nil {
		err = fmt.Errorf("unexpect nil response")
	}

	var (
		reqID RequestID
		item  *reqItem
		ok    bool
	)

	if err == nil {
		reqID = resp.Control.RequestId
		item, ok, _ = bpsc.reqCache.GetReqItem(reqID)
		if !ok {
			err = fmt.Errorf("cannot find reqitem for request %s", reqID)
		}
	}
	if err == nil {
		// TODO: add context
		item.finalHandler(context.TODO(), resp)
	}

	if err != nil {
		bpsc.logger.error(err)
	}
	return err
}
