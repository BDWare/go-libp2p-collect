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
	apsub *AsyncPubSub

	// RequestCache is a cache of requests.
	// We don't know when there is no incoming response for a certain request.
	// We have to eliminate the out-dated request resource.
	// After elimination, the response related to this request will be ignored.
	reqCache *requestCache
	ridgen   ReqIDGenerator
	logger   Logger
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
			logger:   opts.Logger,
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

	bpsc.logger.Logf("debug", "basic join: %s", topic)

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
		bpsc.logger.Logf("error", "join: %v", err)
	}

	return
}

// Publish a serilized request payload.
func (bpsc *BasicPubSubCollector) Publish(topic string, payload []byte, opts ...PubOpt) (err error) {

	bpsc.logger.Logf("debug", "basic publish: %s", topic)

	var (
		root    []byte
		rqID    string
		options *PubOpts
		tosend  []byte
	)
	{
		options, err = NewPublishOptions(opts)
	}
	if err == nil {
		// assemble the request struct
		root, err = bpsc.host.ID().MarshalBinary()
	}
	if err == nil {
		req := &pb.Request{
			Control: pb.RequestControl{
				Root:  root,
				Seqno: atomic.AddUint64(&(bpsc.seqno), 1),
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
		bpsc.logger.Logf("error", "publish: %v", err)
	}

	return
}

// Leave the overlay.
// The registered topichandles and responseHandlers will be closed.
func (bpsc *BasicPubSubCollector) Leave(topic string) (err error) {

	bpsc.logger.Logf("debug", "basic leave: %s", topic)

	err = bpsc.apsub.Unsubscribe(topic)
	bpsc.reqCache.RemoveTopic(topic)

	if err != nil {
		bpsc.logger.Logf("error", "leave: %v", err)
	}
	return
}

// Close the BasicPubSubCollector.
func (bpsc *BasicPubSubCollector) Close() (err error) {

	bpsc.logger.Logf("debug", "basic close")

	bpsc.reqCache.RemoveAll()
	err = bpsc.apsub.Close()

	if err != nil {
		bpsc.logger.Logf("error", "basic close: %v", err)
	}
	return
}

// topicHandle will be called when a request arrived.
func (bpsc *BasicPubSubCollector) topicHandle(topic string, msg *Message) {

	var err error
	if msg == nil {
		err = fmt.Errorf("unexpected nil msg")
	}

	if err == nil {
		bpsc.logger.Logf(
			"info",
			`basic topicHandle:
			topic: %s,
			msg.from: %s,
			msg.recvFrom: %s,`,
			topic,
			peer.ID(msg.From).ShortString(),
			msg.ReceivedFrom.ShortString(),
		)
	}

	var (
		ok          bool
		req         *pb.Request
		rqhandleRaw interface{}
		rqhandle    RequestHandler
		rqresult    *pb.Intermediate
		rqID        string
		rootID      peer.ID
		resp        *pb.Response
		respBytes   []byte
		s           network.Stream
		ctx         context.Context
	)
	{
		// unmarshal the received data into request struct
		req = &pb.Request{}
		err = req.Unmarshal(msg.Data)
	}
	if err == nil {
		rqID = bpsc.ridgen(req)

		bpsc.logger.Logf(
			"info",
			`basic topicHandle:
			req_id: %s,
			`,
			rqID,
		)

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
			bpsc.logger.Logf(
				"info",
				`basic topicHandle:
				req_id: %s,
				rqresult: %+v,
				message: not sendback due to nil rqresult or sendback is set to false`,
				rqID,
				rqresult,
			)

			goto handleEnd
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

			bpsc.logger.Logf(
				"info",
				`basic topicHandle:
				req_id: %s,
				message: receive self-published message`,
				rqID,
			)

			err = bpsc.handleFinalResponse(resp)
			goto handleEnd
		}

		bpsc.logger.Logf(
			"info",
			`basic topicHandle:
			req_id: %s,
			to: %s,
			message: answer response`,
			rqID,
			rootID.ShortString(),
		)

		s, err = bpsc.host.NewStream(ctx, rootID, bpsc.conf.responseProtocol)

	}
	// everything is done, send payload by write to stream
	if err == nil {
		// don't forget to close the stream
		defer s.Close()
		_, err = s.Write(respBytes)
	}

handleEnd:
	if err != nil {
		bpsc.logger.Logf(
			"error",
			`basic topichandle: 
			topic:%s, 
			error:%+v,`,
			topic,
			err,
		)
	}
}

// streamHandler reads response from stream, and calls related notifHandler
func (bpsc *BasicPubSubCollector) streamHandler(s network.Stream) {

	bpsc.logger.Logf("info", "bpsc streamHandler: from: %s", s.Conn().RemotePeer().ShortString())
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
		bpsc.logger.Logf(
			"error",
			`bpsc streamHandler: 
			from: %s,
			error: %v,`,
			s.Conn().RemotePeer().ShortString(),
			err,
		)
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
		err = bpsc.handleFinalResponse(resp)
	}
	return
}

// handleFinalResponse calls finalResponseHandler
func (bpsc *BasicPubSubCollector) handleFinalResponse(resp *pb.Response) (err error) {

	if resp == nil {
		err = fmt.Errorf("unexpect nil response")
	}

	if err == nil {
		bpsc.logger.Logf(
			"debug",
			`bpsc handleFinalResponse: 
			request_id: %s,
			from: %s,
			root: %s,
			`,
			resp.Control.RequestId,
			peer.ID(resp.Control.From).ShortString(),
			peer.ID(resp.Control.Root).ShortString(),
		)
	}

	var (
		reqID string
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
		bpsc.logger.Logf(
			"error",
			`bpsc handleFinalResponse: 
			error: %v,`,
			err,
		)
	}
	return err
}
