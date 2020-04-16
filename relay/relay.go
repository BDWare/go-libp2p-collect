package relaypsc

import (
	"context"
	"fmt"

	"bdware.org/libp2p/go-libp2p-collect/apsub"
	"bdware.org/libp2p/go-libp2p-collect/opt"
	rc "bdware.org/libp2p/go-libp2p-collect/rcache"
	pubsub "bdware.org/libp2p/go-libp2p-pubsub"
	pb "bdware.org/libp2p/go-libp2p-pubsub/pb"
	host "github.com/libp2p/go-libp2p-core/host"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
)

// conf is the static configuration of relay pubsubcollector
type conf struct {
	requestProtocol  protocol.ID
	responseProtocol protocol.ID
	requestCacheSize int
}

// RelayPubSubCollector .
type RelayPubSubCollector struct {
	conf         *conf
	apubsub      *apsub.AsyncPubSub
	respCache    *rc.ResponseCache
	requestCache *rc.RequestCache
}

// NewRelayPubSubCollector .
func NewRelayPubSubCollector(h host.Host, options ...opt.InitOpt) (r *RelayPubSubCollector, err error) {
	// TODO: add lifetime control for randomSub
	var (
		opts      *opt.InitOpts
		conf      *conf
		reqCache  *rc.RequestCache
		respCache *rc.ResponseCache
		ap        *apsub.AsyncPubSub
	)
	{
		opts, err = opt.NewInitOpts(options)
	}
	if err == nil {
		conf, err = checkOptConfAndGetConf(opts.Conf)
	}
	if err == nil {
		reqCache, err = rc.NewRequestCache(conf.requestCacheSize)
	}
	if err == nil {
		respCache, err = rc.NewResponseCache(conf.requestCacheSize)
	}
	if err == nil {
		r = &RelayPubSubCollector{
			conf:         conf,
			requestCache: reqCache,
			respCache:    respCache,
		}
		ap, err = apsub.NewAsyncPubSub(
			h,
			apsub.WithSelfNotif(true),
			apsub.WithCustomPubSubFactory(func(h host.Host) (*pubsub.PubSub, error) {
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
func (r *RelayPubSubCollector) Join(topic string, opts ...opt.JoinOpt) (err error) {
	return
}

// Publish a serialized request. Request should be encasulated in data argument.
func (r *RelayPubSubCollector) Publish(topic string, data []byte, opts ...opt.PubOpt) error {
	panic("not implemented")
}

// Leave the overlay
func (r *RelayPubSubCollector) Leave(topic string) error {
	panic("not implemented")
}

func (r *RelayPubSubCollector) topicHandle(topic string, msg *apsub.Message) {

}

func checkOptConfAndGetConf(optConf *opt.Conf) (c *conf, err error) {
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
