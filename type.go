package collect

import (
	"io"

	"github.com/bdware/go-libp2p-collect/pb"
	"github.com/bdware/go-libp2p-collect/pubsub"
	"github.com/libp2p/go-libp2p-core/peer"
)

// PubSubCollector is a group communication module on topic-based overlay network.
// It helps to dispatch request, and wait for corresponding responses.
// In relay mode, PubSubCollector can also help to reduce the response.
type PubSubCollector interface {

	// Join the overlay network defined by topic.
	// Register RequestHandle and ResponseHandle in opts.
	Join(topic string, opts ...JoinOpt) error

	// Publish a serialized request. Request should be encasulated in data argument.
	Publish(topic string, data []byte, opts ...PubOpt) error

	// Leave the overlay
	Leave(topic string) error

	io.Closer
}

// Request type alias
type Request = pb.Request

// Response type alias
type Response = pb.Response

// Intermediate type alias
type Intermediate = pb.Intermediate

// RequestID type alias
type RequestID = pb.RequestID

// TopicWireListener .
type TopicWireListener = pubsub.TopicWireListener

// TopicMsgHandler .
type TopicMsgHandler = pubsub.TopicMsgHandler

type TopicWires interface {
	ID() peer.ID
	Join(topic string) error
	Leave(topic string) error
	Topics() []string
	Neighbors(topic string) []peer.ID
	SetListener(twn TopicWireListener)
	SendMsg(topic string, to peer.ID, data []byte) error
	SetTopicMsgHandler(th TopicMsgHandler)
	io.Closer
}

type Wires interface {
	ID() peer.ID
	Neighbors() []peer.ID
	SetListener(wn WireListener)
	SendMsg(to peer.ID, data []byte) error
	SetMsgHandler(h MsgHandler)
}

// MsgHandler .
type MsgHandler func(from peer.ID, data []byte)

type WireListener interface {
	HandlePeerUp(p peer.ID)
	HandlePeerDown(p peer.ID)
}
