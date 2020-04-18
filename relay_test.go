package collect

import (
	"context"
	"testing"
	"time"

	"bdware.org/libp2p/go-libp2p-collect/mock"
	"bdware.org/libp2p/go-libp2p-collect/pb"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"
)

func TestRelaySendRecv(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewRelayPubSubCollector(pubhost)
	assert.NoError(t, err)
	sub, err := NewRelayPubSubCollector(subhost)
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *pb.Request) *pb.Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))
		out := &pb.Intermediate{
			Sendback: false,
			Payload:  payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *pb.Request) *pb.Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))
		out := &pb.Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}

	err = pub.Join(topic, WithRequestHandler(handlePub))
	assert.NoError(t, err)
	err = sub.Join(topic, WithRequestHandler(handleSub))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	okch := make(chan struct{})
	notif := func(ctx context.Context, rp *pb.Response) {
		assert.Equal(t, payload, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(notif))
	assert.NoError(t, err)

	// after 2 seconds, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

func TestDeduplication(t *testing.T) {
	// A -- B -- C
	// A broadcasts, then B and C response to it with the same content;
	// B should be able to intercept the response from C, because it knows
	// it is a duplicated response by checking its response cache.
}

func TestNoRequestIDForResponse(t *testing.T) {
	// A -- B -- C
	// A broadcasts a request R, and B should know R's route;
	// what will happen if R expired in B,
	// and at the same time C send R's response to B?
	// We have some strategy to skip the forgetful node fault.
	// for example, we can forward this response to a random node.
	// But now, we just drop it.
}
