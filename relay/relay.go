package relaypsc

import (
	psc "bdware.org/libp2p/go-libp2p-collect"
	pubsub "bdware.org/libp2p/go-libp2p-pubsub"
	pb "bdware.org/libp2p/go-libp2p-pubsub/pb"
	host "github.com/libp2p/go-libp2p-core/host"
)

// RelayPubSubCollector .
type RelayPubSubCollector struct {
	psubs *pubsub.PubSub
}

// NewRelayPubSubCollector .
func NewRelayPubSubCollector(h host.Host, opts ...Option) (r *RelayPubSubCollector, err error) {
	// TODO: add lifetime control for randomSub

	return
}

// Join the overlay network defined by topic.
// Register RequestHandle and ResponseHandle in opts.
func (r *RelayPubSubCollector) Join(topic string, opts ...psc.JoinOpt) error {
	panic("not implemented")
}

// Publish a serialized request. Request should be encasulated in data argument.
func (r *RelayPubSubCollector) Publish(topic string, data []byte, opts ...psc.PubOpt) error {
	panic("not implemented")
}

// Leave the overlay
func (r *RelayPubSubCollector) Leave(topic string) error {
	panic("not implemented")
}

type tracer RelayPubSubCollector

func (t *tracer) Trace(evt *pb.TraceEvent) {

}

type Option struct{}
