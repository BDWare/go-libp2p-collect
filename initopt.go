package collect

import (
	"encoding/binary"
	"fmt"

	"bdware.org/libp2p/go-libp2p-collect/pb"
)

// InitOpt is options used in NewBasicPubSubCollector
type InitOpt func(*InitOpts) error

// InitOpts is options used in NewBasicPubSubCollector
type InitOpts struct {
	Conf        *Conf
	IDGenerator ReqIDGenerator
}

// NewInitOpts returns initopts
func NewInitOpts(opts []InitOpt) (out *InitOpts, err error) {
	conf := MakeDefaultConf()
	out = &InitOpts{
		Conf:        &conf,
		IDGenerator: MakeDefaultReqIDGenerator(),
	}
	for _, opt := range opts {
		if err == nil {
			err = opt(out)
		}
	}
	if err != nil {
		out = nil
	}
	return
}

// WithConf specifies configuration of basic pubsubcollector
func WithConf(conf *Conf) InitOpt {
	return func(opts *InitOpts) error {
		if conf == nil {
			return fmt.Errorf("unexpected nil conf")
		}
		opts.Conf = conf
		return nil
	}
}

// WithRequestIDGenerator .
func WithRequestIDGenerator(idgen ReqIDGenerator) InitOpt {
	return func(opts *InitOpts) error {
		if idgen == nil {
			return fmt.Errorf("unexpected nil ReqIDGenerator")
		}
		opts.IDGenerator = idgen
		return nil
	}
}

// Conf is the static configuration of BasicPubSubCollector
// TODO: add durability for conf
type Conf struct {
	// ProtocolPrefix is the protocol name prefix
	ProtocolPrefix string
	// RequestCacheSize .
	// RequestCache is used to store the request control message,
	// which is for response routing.
	RequestCacheSize int
	// ResponseCacheSize .
	// ResponseCache is used to deduplicate the response.
	ResponseCacheSize int
}

// MakeDefaultConf returns a default Conf instance
func MakeDefaultConf() Conf {
	return Conf{
		ProtocolPrefix:   "/basicpsc",
		RequestCacheSize: 512,
	}
}

// ReqIDGenerator is used to generate id for each request
type ReqIDGenerator func(*pb.Request) string

// MakeDefaultReqIDGenerator returns default ReqIDGenerator
func MakeDefaultReqIDGenerator() ReqIDGenerator {
	return func(rq *pb.Request) string {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, rq.Control.Seqno)
		// string(rq.Control.Seqno) is not workable here
		return string(rq.Control.Root) + string(bs)
	}
}
