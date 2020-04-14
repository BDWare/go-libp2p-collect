package relaypsc

import (
	"fmt"

	"bdware.org/libp2p/go-libp2p-collect/apsub"
	"bdware.org/libp2p/go-libp2p-collect/opt"
	rc "bdware.org/libp2p/go-libp2p-collect/rcache"
	pb "bdware.org/libp2p/go-libp2p-pubsub/pb"
	host "github.com/libp2p/go-libp2p-core/host"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
)

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

	return
}

// Join the overlay network defined by topic.
// Register RequestHandle and ResponseHandle in opts.
func (r *RelayPubSubCollector) Join(topic string, opts ...opt.JoinOpt) error {
	panic("not implemented")
}

// Publish a serialized request. Request should be encasulated in data argument.
func (r *RelayPubSubCollector) Publish(topic string, data []byte, opts ...opt.PubOpt) error {
	panic("not implemented")
}

// Leave the overlay
func (r *RelayPubSubCollector) Leave(topic string) error {
	panic("not implemented")
}

type tracer RelayPubSubCollector

func (t *tracer) Trace(evt *pb.TraceEvent) {

}

// conf is the static configuration of relay pubsubcollector
type conf struct {
	requestProtocol  protocol.ID
	responseProtocol protocol.ID
	requestCacheSize int
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
