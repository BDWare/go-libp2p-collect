package collect

import (
	"context"

	"bdware.org/libp2p/go-libp2p-collect/pb"
)

// PubOpt is optional options in PubSubCollector.Publish
type PubOpt func(*PubOpts) error

// PubOpts is the aggregated options
type PubOpts struct {
	RequestContext  context.Context
	FinalRespHandle FinalRespHandler
	Cancel          func()
}

// NewPublishOptions returns an option collection
func NewPublishOptions(opts []PubOpt) (out *PubOpts) {
	out = &PubOpts{}
	for _, opt := range opts {
		opt(out)
	}
	// set default value
	if out.RequestContext == nil {
		out.RequestContext, out.Cancel = context.WithCancel(context.Background())
	}
	if out.FinalRespHandle == nil {
		out.FinalRespHandle = func(*pb.Response) {}
	}
	return
}

// FinalRespHandler is the callback function when the root node receiving a response.
// It will be called only in the root node.
// It will be called more than one time when the number of responses is larger than one.
type FinalRespHandler func(rp *pb.Response)

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
		pubopts.RequestContext, pubopts.Cancel = context.WithCancel(ctx)
		return nil
	}
}
