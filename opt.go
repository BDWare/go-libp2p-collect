package collect

import (
	"context"
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

// JoinOpt is optional options in PubSubCollector.Join
type JoinOpt func(*JoinOpts) error

// JoinOpts is the aggregated options
type JoinOpts struct {
	RequestHandler
	ResponseHandler
}

// NewJoinOptions returns an option collection
func NewJoinOptions(opts []JoinOpt) (out *JoinOpts, err error) {
	out = &JoinOpts{
		RequestHandler:  defaultRequestHandler,
		ResponseHandler: defaultResponseHandler,
	}
	for _, opt := range opts {
		if err == nil {
			err = opt(out)
		}
	}
	return
}

// RequestHandler is the callback function when receiving a request.
// It will be called in every node joined the network.
// The return value will be sent to the root (directly or relayedly).
type RequestHandler func(ctx context.Context, req *pb.Request) *pb.Intermediate

// WithRequestHandler registers request handler
func WithRequestHandler(rqhandle RequestHandler) JoinOpt {
	return func(opts *JoinOpts) error {
		opts.RequestHandler = rqhandle
		return nil
	}
}

func defaultRequestHandler(context.Context, *pb.Request) *pb.Intermediate {
	return &pb.Intermediate{
		Sendback: false,
		Payload:  []byte{},
	}
}

// ResponseHandler is the callback when a node receive a response.
// the sendback in Intermediate will decide whether the response will be sent back to the root.
type ResponseHandler func(context.Context, *pb.Response) *pb.Intermediate

// WithResponseHandler registers response handler
func WithResponseHandler(handler ResponseHandler) JoinOpt {
	return func(opts *JoinOpts) error {
		opts.ResponseHandler = handler
		return nil
	}
}

func defaultResponseHandler(context.Context, *pb.Response) *pb.Intermediate {
	return &pb.Intermediate{
		Sendback: true,
		Payload:  []byte{},
	}
}

// PubOpt is optional options in PubSubCollector.Publish
type PubOpt func(*PubOpts) error

// PubOpts is the aggregated options
type PubOpts struct {
	RequestContext  context.Context
	FinalRespHandle FinalRespHandler
}

// NewPublishOptions returns an option collection
func NewPublishOptions(opts []PubOpt) (out *PubOpts, err error) {
	out = &PubOpts{
		RequestContext:  context.TODO(),
		FinalRespHandle: func(context.Context, *pb.Response) {},
	}
	for _, opt := range opts {
		if err == nil {
			err = opt(out)
		}
	}
	return
}

// FinalRespHandler is the callback function when the root node receiving a response.
// It will be called only in the root node.
// It will be called more than one time when the number of responses is larger than one.
type FinalRespHandler func(context.Context, *pb.Response)

// WithFinalRespHandler registers notifHandler
func WithFinalRespHandler(handler FinalRespHandler) PubOpt {
	return func(pubopts *PubOpts) error {
		pubopts.FinalRespHandle = handler
		return nil
	}
}

// WithRequestContext adds cancellation or timeout for a request
// default is withCancel. (ctx will be cancelled when request is closed)
func WithRequestContext(ctx context.Context) PubOpt {
	return func(pubopts *PubOpts) error {
		pubopts.RequestContext = ctx
		return nil
	}
}
