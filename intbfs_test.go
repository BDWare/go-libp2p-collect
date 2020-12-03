package collect

import (
	"context"
	"testing"
	"time"

	"github.com/bdware/go-libp2p-collect/mock"
	"github.com/stretchr/testify/assert"
)

func mustNewIntBFS(mnet *mock.Net, opts *IntBFSOptions) *IntBFS {
	host := mnet.MustNewLinkedPeer()
	wire := NewHostWires(host)
	intbfs, err := NewIntBFS(wire, opts)
	if err != nil {
		panic(err)
	}
	err = mnet.LinkAllButSelf(host)
	if err != nil {
		panic(err)
	}
	return intbfs
}

func TestIntBFSReqSendRecv(t *testing.T) {
	mnet := mock.NewMockNet()
	pubCh := make(chan struct{}, 1)
	subCh := make(chan struct{}, 1)
	data := []byte{1, 2, 3}
	pubOpts := &IntBFSOptions{
		ProfileFactory: defaultProfileFactory,
		RequestHandler: func(ctx context.Context, req *Request) *Intermediate {
			assert.Equal(t, data, req.Payload)
			pubCh <- struct{}{}
			return nil
		},
	}
	subOpts := &IntBFSOptions{
		ProfileFactory: defaultProfileFactory,
		RequestHandler: func(ctx context.Context, req *Request) *Intermediate {
			assert.Equal(t, data, req.Payload)
			subCh <- struct{}{}
			return nil
		},
	}

	pubIntBFS := mustNewIntBFS(mnet, pubOpts)
	subIntBFS := mustNewIntBFS(mnet, subOpts)

	defer pubIntBFS.Close()
	defer subIntBFS.Close()

	var err error
	err = pubIntBFS.Publish(data)
	if err != nil {
		panic(err)
	}
	select {
	case <-pubCh:
	case <-time.After(1 * time.Second):
		t.Fatal("pub cannot receive data")
	}
	select {
	case <-subCh:
	case <-time.After(1 * time.Second):
		t.Fatal("sub cannot receive data")
	}

}

func TestIntBFSRespSendRecv(t *testing.T) {
	mnet := mock.NewMockNet()
	okCh := make(chan struct{}, 1)
	data := []byte{1, 2, 3}
	pubOpts := &IntBFSOptions{
		ProfileFactory: defaultProfileFactory,
		RequestHandler: func(ctx context.Context, req *Request) *Intermediate {
			return &Intermediate{
				Hit: false,
			}
		},
	}
	subOpts := &IntBFSOptions{
		ProfileFactory: defaultProfileFactory,
		RequestHandler: func(ctx context.Context, req *Request) *Intermediate {
			return &Intermediate{
				Hit:     true,
				Payload: req.Payload,
			}
		},
	}

	pubIntBFS := mustNewIntBFS(mnet, pubOpts)
	subIntBFS := mustNewIntBFS(mnet, subOpts)
	defer pubIntBFS.Close()
	defer subIntBFS.Close()

	var err error
	err = pubIntBFS.Publish(data, WithFinalRespHandler(
		func(c context.Context, r *Response) {
			assert.Equal(t, data, r.Payload)
			okCh <- struct{}{}
		}))
	if err != nil {
		panic(err)
	}
	select {
	case <-okCh:
	case <-time.After(1 * time.Second):
		t.Fatal("pub cannot receive data")
	}

}
