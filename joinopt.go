package collect

import (
	"context"

	"bdware.org/libp2p/go-libp2p-collect/pb"
)

// JoinOpt is optional options in PubSubCollector.Join
type JoinOpt func(*JoinOptions) error

// JoinOptions is the aggregated options
type JoinOptions struct {
	RequestHandler
}

// NewJoinOptions returns an option collection
func NewJoinOptions(opts []JoinOpt) (out *JoinOptions) {
	out = &JoinOptions{}
	for _, opt := range opts {
		opt(out)
	}
	//set default value
	if out.RequestHandler == nil {
		out.RequestHandler = func(context.Context, *pb.Request) *pb.Intermediate {
			return &pb.Intermediate{
				Sendback: false,
				Payload:  []byte{},
			}
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
	return func(opts *JoinOptions) error {
		opts.RequestHandler = rqhandle
		return nil
	}
}

// WithRequestCacheSize .
func WithRequestCacheSize(n uint) JoinOpt {
	panic("not implemented")
}
