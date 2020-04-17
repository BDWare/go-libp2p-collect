package collect

import (
	"context"
	"fmt"

	host "github.com/libp2p/go-libp2p-core/host"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// RelayPubSubCollector .
type RelayPubSubCollector struct {
	conf         *conf
	apubsub      *AsyncPubSub
	respCache    *responseCache
	requestCache *requestCache
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
		conf, err = checkOptConfAndGetConf(opts.Conf)
	}
	if err == nil {
		reqCache, err = newRequestCache(conf.requestCacheSize)
	}
	if err == nil {
		respCache, err = newResponseCache(conf.requestCacheSize)
	}
	if err == nil {
		r = &RelayPubSubCollector{
			conf:         conf,
			requestCache: reqCache,
			respCache:    respCache,
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

	} else { // err != nil
		r = nil
	}

	return
}

// Join the overlay network defined by topic.
// Register RequestHandle and ResponseHandle in opts.
func (r *RelayPubSubCollector) Join(topic string, opts ...JoinOpt) (err error) {
	var options *JoinOpts
	{
		options, err = NewJoinOptions(opts)
	}
	// subscribe the topic
	if err == nil {
		err = r.apubsub.Subscribe(topic, r.topicHandle)
	}

	// register request handler
	if err == nil {
		err = r.apubsub.SetTopicItem(topic, requestHandlerKey, options.RequestHandler)
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

}

func checkOptConfAndGetConf(optConf *Conf) (c *conf, err error) {
	if optConf.ProtocolPrefix == "" {
		err = fmt.Errorf("unexpected nil Prefix")
	}
	if optConf.RequestCacheSize < 0 {
		err = fmt.Errorf("unexpected negetive RequestBufSize")
	}
	if err == nil {
		c = &conf{
			requestProtocol:  protocol.ID(optConf.ProtocolPrefix + "/relay/request"),
			responseProtocol: protocol.ID(optConf.ProtocolPrefix + "/relay/response"),
			requestCacheSize: optConf.RequestCacheSize,
		}
	}
	return
}

type tracer RelayPubSubCollector

func (t *tracer) Trace(evt *pb.TraceEvent) {

}
