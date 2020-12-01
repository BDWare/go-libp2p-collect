package collect

import (
	"testing"

	"github.com/bdware/go-libp2p-collect/mock"
	"github.com/libp2p/go-libp2p-core/peer"
)

type mockProfile struct{}

func (m *mockProfile) Insert(from peer.ID, req []byte) error {
	return nil
}
func (m *mockProfile) Less(that Profile, req []byte) bool {
	return true
}

func NewMockProfile() (Profile, error) {
	return &mockProfile{}, nil
}

func NewMockIntBFS(net *mock.Net) *IntBFS {
	h, err := net.NewLinkedPeer()
	if err != nil {
		panic(err)
	}
	return
}

// func TestIntBFSSendRecv(t *testing.T) {
// 	mnet := mock.NewMockNet()
// 	pubhost, err := mnet.NewLinkedPeer()
// 	assert.NoError(t, err)
// 	subhost, err := mnet.NewLinkedPeer()
// 	assert.NoError(t, err)

// 	// Even if hosts are connected,
// 	// the topics may not find the pre-exist connections.
// 	// We establish connections after topics are created.
// 	pub, err := NewIntBFSCollector(pubhost)
// 	assert.NoError(t, err)
// 	sub, err := NewIntBFSCollector(subhost)
// 	assert.NoError(t, err)
// 	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

// 	// time to connect
// 	time.Sleep(50 * time.Millisecond)

// 	topic := "test-topic"
// 	payload := []byte{1, 2, 3}

// 	// handlePub and handleSub is both request handle,
// 	// but handleSub will send back the response
// 	handlePub := func(ctx context.Context, r *Request) *Intermediate {
// 		assert.Equal(t, payload, r.Payload)
// 		assert.Equal(t, pubhost.ID(), r.Control.Requester)
// 		out := &Intermediate{
// 			Hit:     false,
// 			Payload: payload,
// 		}
// 		return out
// 	}
// 	handleSub := func(ctx context.Context, r *Request) *Intermediate {
// 		assert.Equal(t, payload, r.Payload)
// 		assert.Equal(t, pubhost.ID(), r.Control.Requester)
// 		out := &Intermediate{
// 			Hit:     true,
// 			Payload: payload,
// 		}
// 		return out
// 	}

// 	err = pub.Join(topic,
// 		WithRequestHandler(handlePub),
// 		WithProfileFactory(NewMockProfile),
// 	)
// 	assert.NoError(t, err)
// 	err = sub.Join(topic,
// 		WithRequestHandler(handleSub),
// 		WithProfileFactory(NewMockProfile),
// 	)
// 	assert.NoError(t, err)

// 	// time to join
// 	time.Sleep(50 * time.Millisecond)

// 	okch := make(chan struct{})
// 	notif := func(ctx context.Context, rp *Response) {
// 		assert.Equal(t, payload, rp.Payload)
// 		okch <- struct{}{}
// 	}
// 	err = pub.Publish(topic, payload, WithFinalRespHandler(notif))
// 	assert.NoError(t, err)

// 	// after 2 seconds, test will failed
// 	select {
// 	case <-time.After(2 * time.Second):
// 		assert.Fail(t, "we don't receive enough response in 2s")
// 	case <-okch:
// 	}
// }

func TestIntBFSSendRecv(t *testing.T) {
	// 	mnet := mock.NewMockNet()
	// pubhost, err := mnet.NewLinkedPeer()
	// assert.NoError(t, err)
	// subhost, err := mnet.NewLinkedPeer()
	// assert.NoError(t, err)
}
