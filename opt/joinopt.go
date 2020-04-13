package opt

import (
	"context"

	"bdware.org/libp2p/go-libp2p-collect/pb"
)

// JoinOpt is optional options in PubSubCollector.Join
type JoinOpt func(*JoinOpts) error

// JoinOpts is the aggregated options
type JoinOpts struct {
	RequestHandler
}

// NewJoinOptions returns an option collection
func NewJoinOptions(opts []JoinOpt) (out *JoinOpts, err error) {
	out = &JoinOpts{
		RequestHandler: func(context.Context, *pb.Request) *pb.Intermediate {
			return &pb.Intermediate{
				Sendback: false,
				Payload:  []byte{},
			}
		},
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

// WithRequestCacheSize .
func WithRequestCacheSize(n uint) JoinOpt {
	panic("not implemented")
}
