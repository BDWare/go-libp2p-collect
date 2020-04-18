package collect

import (
	"context"
	"fmt"

	"bdware.org/libp2p/go-libp2p-collect/pb"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// RelayPubSubCollector .
type RelayPubSubCollector struct {
	conf      *conf
	host      host.Host
	apubsub   *AsyncPubSub
	respCache *responseCache
	reqCache  *requestCache
	ridgen    ReqIDGenerator
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
			conf:      conf,
			host:      h,
			reqCache:  reqCache,
			respCache: respCache,
			ridgen:    opts.IDGenerator,
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
		r.host.SetStreamHandler(r.conf.responseProtocol, r.streamHandler)

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
func (r *RelayPubSubCollector) Publish(topic string, data []byte, opts ...PubOpt) error {
	panic("not implemented")
}

// Leave the overlay
func (r *RelayPubSubCollector) Leave(topic string) error {
	panic("not implemented")
}

func (r *RelayPubSubCollector) topicHandle(topic string, msg *Message) {
	var (
		err         error
		ok          bool
		rqhandleRaw interface{}
		rqhandle    RequestHandler
		rqresult    *pb.Intermediate
		rqID        string
		fromID      peer.ID
		resp        *pb.Response
		respBytes   []byte
		s           network.Stream
	)
	// unmarshal the received data into request struct
	req := &pb.Request{}
	err = req.Unmarshal(msg.Data)

	if err == nil {
		rqID = r.ridgen(req)
		// Dispatch request to relative topic request handler,
		// which should be initialized in join function
		rqhandleRaw, err = r.apubsub.LoadTopicItem(topic, requestHandlerKey)

		if err != nil {
			err = fmt.Errorf("cannot find request handler:%w", err)
		}
	}
	if err == nil {
		rqhandle, ok = rqhandleRaw.(RequestHandler)
		if !ok {
			err = fmt.Errorf("unexpected request handler type")
		}
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
		respBytes, err = resp.Marshal()
	}
	if err == nil {
		// find the from peer_id
		fromID = peer.ID(req.Control.From)

		// receive self-published message
		if fromID == r.host.ID() {
			r.handleResponse(resp)
			return
		}

		// send payload
		ctx, cc := context.WithCancel(context.Background())
		r.reqCache.AddReqItem(rqID, &reqItem{
			topic:  topic,
			cancel: cc,
		})
		// clean up later
		defer r.reqCache.RemoveReqItem(rqID)
		s, err = r.host.NewStream(ctx, fromID, r.conf.responseProtocol)

	}
	// everything is done, send payload by write to stream
	if err == nil {
		// don't forget to close the stream
		defer s.Close()
		_, err = s.Write(respBytes)
	}

	return
}

func (r *RelayPubSubCollector) streamHandler(s network.Stream) {

}

func (r *RelayPubSubCollector) handleResponse(resp *pb.Response) (err error) {
	// call the responseHandler

	return
}

func (r *RelayPubSubCollector) handleFinalResponse(resp *pb.Response) (err error) {
	return
}

type tracer RelayPubSubCollector

func (t *tracer) Trace(evt *pubsub_pb.TraceEvent) {

}
