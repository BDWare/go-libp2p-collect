package pubsub

import (
	pubsub "github.com/bdware/go-libp2p-pubsub"
	pubsub_pb "github.com/bdware/go-libp2p-pubsub/pb"
)

//Message is type alias.
type Message = pubsub.Message

//PbMessage is type alias.
type PbMessage = pubsub_pb.Message

//PubSub is type alias.
type PubSub = pubsub.PubSub

//Topic is type alias.
type Topic = pubsub.Topic

//Subscription is type alias.
type Subscription = pubsub.Subscription

//TraceEvent is type alias.
type TraceEvent = pubsub_pb.TraceEvent

// Notif is type alias.
type Notif = pubsub.PubSubNotif

// MsgIDFn is type alias.
type MsgIDFn = pubsub.MsgIdFunction

type TopicWireListener = pubsub.TopicWireListener

type TopicMsgHandler = pubsub.TopicMsgHandler

// function aliases
var (
	NewRandomSub        = pubsub.NewRandomSub
	NewGossipSub        = pubsub.NewGossipSub
	NewTopicWires       = pubsub.NewPubSubTopicWires
	WithCustomProtocols = pubsub.WithCustomProtocols
	WithEventTracer     = pubsub.WithEventTracer
	WithMessageIDFn     = pubsub.WithMessageIdFn
)
