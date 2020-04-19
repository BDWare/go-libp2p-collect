package collect

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync/atomic"

	"bdware.org/libp2p/go-libp2p-collect/pb"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

type deduplicator interface {
	// markSeen returns false if resp has been seen;
	// return true if resp hasn't been seen before.
	// The response will be marked seen after markSeen callation.
	markSeen(resp *pb.Response) bool
}

// RelayPubSubCollector .
type RelayPubSubCollector struct {
	conf     *conf
	host     host.Host
	seqno    uint64
	apubsub  *AsyncPubSub
	reqCache *requestCache
	dedup    deduplicator
	ridgen   ReqIDGenerator
}

// NewRelayPubSubCollector .
func NewRelayPubSubCollector(h host.Host, options ...InitOpt) (r *RelayPubSubCollector, err error) {
	// TODO: add lifetime control for randomSub
	var (
		opts      *InitOpts
		conf      *conf
		reqCache  *requestCache
		respCache *responseCache
		ap        *AsyncPubSub
	)
	{
		opts, err = NewInitOpts(options)
	}
	if err == nil {
		conf, err = checkOptConfAndGetInnerConf(opts.Conf)
	}
	if err == nil {
		reqCache, err = newRequestCache(conf.requestCacheSize)
	}
	if err == nil {
		respCache, err = newResponseCache(conf.requestCacheSize)
	}
	if err == nil {
		r = &RelayPubSubCollector{
			conf:     conf,
			host:     h,
			seqno:    rand.Uint64(),
			reqCache: reqCache,
			dedup:    respCache,
			ridgen:   opts.IDGenerator,
		}
		ap, err = NewAsyncPubSub(
			h,
			WithSelfNotif(true),
			WithCustomPubSubFactory(func(h host.Host) (*pubsub.PubSub, error) {
				return pubsub.NewRandomSub(
					context.Background(),
					h,
					pubsub.WithCustomProtocols([]protocol.ID{conf.requestProtocol}),
					pubsub.WithEventTracer((*tracer)(r)),
				)
			}),
		)
	}
	if err == nil {
		r.apubsub = ap
		r.host.SetStreamHandler(r.conf.responseProtocol, r.responseStreamHandler)

	} else { // err != nil
		r = nil
	}

	return
}

// Join the overlay network defined by topic.
// Register RequestHandle and ResponseHandle in opts.
func (r *RelayPubSubCollector) Join(topic string, options ...JoinOpt) (err error) {
	var opts *JoinOpts
	{
		opts, err = NewJoinOptions(options)
	}
	// subscribe the topic
	if err == nil {
		err = r.apubsub.Subscribe(topic, r.topicHandle)
	}

	// register request handler
	if err == nil {
		err = r.apubsub.SetTopicItem(topic, requestHandlerKey, opts.RequestHandler)
	}

	// register request handler
	if err == nil {
		err = r.apubsub.SetTopicItem(topic, responseHandlerKey, opts.ResponseHandler)
	}
	return
}

// Publish a serialized request. Request should be encasulated in data argument.
func (r *RelayPubSubCollector) Publish(topic string, data []byte, opts ...PubOpt) (err error) {
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
		root, err = r.host.ID().MarshalBinary()
	}
	if err == nil {
		req := &pb.Request{
			Control: pb.RequestControl{
				Root:  root,
				Seqno: atomic.AddUint64(&(r.seqno), 1),
			},
			Payload: data,
		}
		rqID = r.ridgen(req)

		tosend, err = req.Marshal()
	}
	if err == nil {
		// register notif handler
		r.reqCache.AddReqItem(options.RequestContext, rqID, &reqItem{
			finalHandler: options.FinalRespHandle,
			topic:        topic,
		})

		//  publish marshaled request
		err = r.apubsub.Publish(options.RequestContext, topic, tosend)

	}
	return
}

// Leave the overlay
func (r *RelayPubSubCollector) Leave(topic string) (err error) {
	err = r.apubsub.Unsubscribe(topic)
	r.reqCache.RemoveTopic(topic)
	return
}

func (r *RelayPubSubCollector) topicHandle(topic string, msg *Message) {

	var (
		req *pb.Request
		err error
	)
	{
		// unmarshal the received data into request struct
		req = &pb.Request{}
		err = req.Unmarshal(msg.Data)
	}
	var (
		ok          bool
		rqhandleRaw interface{}
		rqhandle    RequestHandler
		rqID        string
	)
	if err == nil {
		rqID = r.ridgen(req)
		// Dispatch request to relative topic request handler,
		// which should be initialized in join function
		rqhandleRaw, err = r.apubsub.LoadTopicItem(topic, requestHandlerKey)

		if err != nil {
			err = fmt.Errorf("cannot find request handler:%w", err)
		} else {
			rqhandle, ok = rqhandleRaw.(RequestHandler)
			if !ok {
				err = fmt.Errorf("unexpected request handler type")
			}
		}
	}

	var (
		item *reqItem
		resp *pb.Response
		ctx  context.Context
	)
	if err == nil {
		// send payload
		ctx = context.Background()
		item, ok = r.reqCache.GetReqItem(rqID)
		if !ok {
			item = &reqItem{
				finalHandler: func(context.Context, *pb.Response) {},
				topic:        topic,
				msg:          msg,
			}
			r.reqCache.AddReqItem(ctx, rqID, item)
		} else {
			item.msg = msg
		}

		// handle request
		// TODO: add timeout
		rqresult := rqhandle(ctx, req)

		// After request is processed, we will have a Intermediate.
		// We send the response to the root node directly if sendback is set to true.
		// Another protocol will be used to inform the root node.
		if !rqresult.Sendback {
			// drop any response if sendback is false
			return
		}

		// assemble the response
		resp = &pb.Response{
			Control: pb.ResponseControl{
				RequestId: rqID,
				Root:      req.Control.Root,
				From:      req.Control.From,
			},
			Payload: rqresult.Payload,
		}

		// receive self-published message
		if peer.ID(req.Control.Root) == r.host.ID() {
			r.handleFinalResponse(ctx, resp)
		} else {
			r.handleAndForwardResponse(ctx, resp)
		}

	}

	return
}

func (r *RelayPubSubCollector) responseStreamHandler(s network.Stream) {
	var (
		respBytes []byte
		err       error
		resp      *pb.Response
	)
	respBytes, err = ioutil.ReadAll(s)
	s.Close()
	if err == nil {
		resp = &pb.Response{}
		err = resp.Unmarshal(respBytes)
	}
	if err == nil {
		if peer.ID(resp.Control.Root) == r.host.ID() {
			err = r.handleFinalResponse(context.Background(), resp)
		} else {
			err = r.handleAndForwardResponse(context.Background(), resp)
		}
	}
}

func (r *RelayPubSubCollector) handleAndForwardResponse(ctx context.Context, recv *pb.Response) (err error) {
	// check response cache, deduplicate and forward
	var (
		reqID string
		item  *reqItem
		ok    bool
	)
	reqID = recv.Control.RequestId
	item, ok = r.reqCache.GetReqItem(reqID)
	if !ok {
		err = fmt.Errorf("cannot find reqItem for response ID: %s", reqID)
	}

	var (
		s         network.Stream
		respBytes []byte
		from      peer.ID
	)
	if err == nil && r.dedup.markSeen(recv) {
		// send back the first seen response
		{
			respBytes, err = recv.Marshal()
		}
		if err == nil {
			from = peer.ID(item.msg.From)
			s, err = r.host.NewStream(context.Background(), from, r.conf.responseProtocol)
		}
		if err == nil {
			defer s.Close()
			_, err = s.Write(respBytes)
		}
	}

	return
}

// only called in root node
func (r *RelayPubSubCollector) handleFinalResponse(ctx context.Context, recv *pb.Response) (err error) {
	var (
		reqID string
		item  *reqItem
		ok    bool
	)
	reqID = recv.Control.RequestId
	item, ok = r.reqCache.GetReqItem(reqID)
	if !ok {
		err = fmt.Errorf("cannot find reqItem for response ID: %s", reqID)
	}
	if err == nil && r.dedup.markSeen(recv) {
		item.finalHandler(ctx, recv)
	}

	return
}

type tracer RelayPubSubCollector

func (t *tracer) Trace(evt *pubsub_pb.TraceEvent) {

}
