package pubsub

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

//Message is type alias.
type Message = pubsub.Message

//PubSub is type alias.
type PubSub = pubsub.PubSub

//Topic is type alias.
type Topic = pubsub.Topic

//Subscription is type alias.
type Subscription = pubsub.Subscription

//TraceEvent is type alias.
type TraceEvent = pubsub_pb.TraceEvent

// function aliases
var (
	NewRandomSub        = pubsub.NewRandomSub
	WithCustomProtocols = pubsub.WithCustomProtocols
	WithEventTracer     = pubsub.WithEventTracer
)
