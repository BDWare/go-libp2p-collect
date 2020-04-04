package basicpsc

import (
	"context"
	"encoding/binary"
	"fmt"

	"bdware.org/libp2p/go-libp2p-collect/pb"
	"bdware.org/libp2p/go-libp2p-collect/apsub"
	host "github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/protocol"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Option is options used in NewBasicPubSubCollector
type Option func(*BasicPubSubCollector) error

// Conf is the static configuration of BasicPubSubCollector
// TODO: add durability for conf
type Conf struct {
	//
	ProtocolPrefix string
	// RequestBufSize restricts request buffer size.
	// Once a node sends or receives a request, a buffer slot is occupied.
	RequestBufSize int
}

type innerConf struct {
	RequestProtocol  protocol.ID
	ResponseProtocol protocol.ID
	RequestBufSize   int
}

// MakeDefaultConf returns a default Conf instance
func MakeDefaultConf() Conf {
	return Conf{
		ProtocolPrefix: "/basicpsc",
		RequestBufSize: 512,
	}
}

func newDefaultInnerConf() innerConf {
	conf := MakeDefaultConf()
	inner, _ := checkConf(&conf)
	return inner
}

func checkConf(conf *Conf) (inner innerConf, err error) {
	if conf.ProtocolPrefix == "" {
		err = fmt.Errorf("unexpected nil Prefix")
	}
	if conf.RequestBufSize < 0 {
		err = fmt.Errorf("unexpected negetive RequestBufSize")
	}
	if err == nil {
		inner.RequestProtocol = protocol.ID(conf.ProtocolPrefix + "/request")
		inner.ResponseProtocol = protocol.ID(conf.ProtocolPrefix + "/response")
		inner.RequestBufSize = conf.RequestBufSize
	}
	return
}

func WithConf(conf *Conf) Option {
	return func(bpsc *BasicPubSubCollector) error {
		inner, err := checkConf(conf)
		if err == nil {
			bpsc.conf = inner
		}
		return err
	}
}

// ReqIDGenerator is used to generate id for each request
type ReqIDGenerator func(*pb.Request) string

func defaultReqIDGenerator() ReqIDGenerator {
	return func(rq *pb.Request) string {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, rq.Control.Seqno)
		// string(rq.Control.Seqno) is not workable here
		return string(rq.Control.Root) + string(bs)
	}
}

func WithRequestIdGenerator() Option {
	panic("unimplement")
}

func newPubsubFactoryWithProtocol(protoc protocol.ID) apsub.PubSubFactory {
	return func(h host.Host) (psub *pubsub.PubSub, err error) {
		// TODO: how can I change the protocol used in other pubsub?
		return pubsub.NewFloodsubWithProtocols(
			context.Background(),
			h,
			[]protocol.ID{protoc},
		)
	}
}
