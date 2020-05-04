package collect

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/network"

	"sync/atomic"

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
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))
		out := &Intermediate{
			Sendback: false,
			Payload:  payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))
		out := &Intermediate{
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
	notif := func(ctx context.Context, rp *Response) {
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

func TestRelaySelfPub(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pub, err := NewRelayPubSubCollector(pubhost)
	assert.NoError(t, err)

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))
		out := &Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}

	err = pub.Join(topic, WithRequestHandler(handlePub))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	okch := make(chan struct{})
	notif := func(ctx context.Context, rp *Response) {
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

func TestRelayRejoin(t *testing.T) {
	mnet := mock.NewMockNet()
	h, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pub, err := NewRelayPubSubCollector(h)
	assert.NoError(t, err)

	topic := "test-topic"
	payload := []byte{1, 2, 3}
	another := []byte{4, 5, 6}

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, h.ID(), peer.ID(r.Control.Root))
		out := &Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}
	anotherHandle := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, h.ID(), peer.ID(r.Control.Root))
		out := &Intermediate{
			Sendback: true,
			Payload:  another,
		}
		return out
	}

	err = pub.Join(topic, WithRequestHandler(handlePub))
	assert.NoError(t, err)

	// We join the same topic for a second time.
	err = pub.Join(topic, WithRequestHandler(anotherHandle))
	assert.NoError(t, err)

	okch := make(chan struct{})
	notif := func(ctx context.Context, rp *Response) {
		assert.Equal(t, another, rp.Payload)
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

func TestRelayLeaveAndJoin(t *testing.T) {
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
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))
		out := &Intermediate{
			Sendback: false,
			Payload:  payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))
		out := &Intermediate{
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

	notifOK := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(notifOK))
	assert.NoError(t, err)

	// after 2 seconds, if okch receive nothing, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}

	// let sub leave the topic
	err = sub.Leave(topic)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	notifFail := func(ctx context.Context, rp *Response) {
		assert.FailNow(t, "should not recv message")
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(notifFail))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// let sub join again
	err = sub.Join(topic, WithRequestHandler(handleSub))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	err = pub.Publish(topic, payload, WithFinalRespHandler(notifOK))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// after 2 seconds, if okch receive nothing, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

func TestRequestContextExpired(t *testing.T) {
	// FinalResponseHandler shouldn't be called after the request context expires.
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pub, err := NewRelayPubSubCollector(pubhost)
	assert.NoError(t, err)
	sub, err := NewRelayPubSubCollector(subhost)
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	cctx, cc := context.WithCancel(context.Background())
	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *Request) *Intermediate {
		out := &Intermediate{
			Sendback: false,
			Payload:  payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *Request) *Intermediate {
		// cancel the request context
		cc()
		// wait for cancellation
		time.Sleep(50 * time.Millisecond)

		out := &Intermediate{
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

	notif := func(ctx context.Context, rp *Response) {
		assert.Fail(t, "unexpected callation of finalRespHandler")
	}

	err = pub.Publish(
		topic,
		payload,
		WithFinalRespHandler(notif),
		WithRequestContext(cctx))
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

}

func TestFinalDeduplication(t *testing.T) {
	// A -- B
	// B returns 2 response to A, A drop the second one.
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

	// when B handles the request, B sends two responses to A directly, not using Intermediate
	handleSub := func(ctx context.Context, req *Request) *Intermediate {
		assert.Equal(t, payload, req.Payload)
		assert.Equal(t, pubhost.ID(), peer.ID(req.Control.Root))
		rqID := sub.ridgen(req)
		resp := &Response{
			Control: pb.ResponseControl{
				RequestId: rqID,
				Root:      req.Control.Root,
				From:      req.Control.From,
			},
			Payload: payload,
		}
		var (
			s         network.Stream
			respBytes []byte
			from      peer.ID
		)
		respBytes, _ = resp.Marshal()
		from = peer.ID(req.Control.Root)
		s, err = sub.host.NewStream(context.Background(), from, sub.conf.responseProtocol)
		assert.NoError(t, err)
		_, err = s.Write(respBytes)
		s.Close()

		s, err = sub.host.NewStream(context.Background(), from, sub.conf.responseProtocol)
		assert.NoError(t, err)
		_, err = s.Write(respBytes)
		s.Close()

		out := &Intermediate{
			Sendback: false,
			Payload:  payload,
		}
		return out
	}

	err = pub.Join(topic)
	assert.NoError(t, err)
	err = sub.Join(topic, WithRequestHandler(handleSub))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	// add count by 1 when final handler is called
	count := int32(0)
	finalHandler := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		atomic.AddInt32(&count, 1)
	}
	err = pub.Publish(topic, payload, WithFinalRespHandler(finalHandler))
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1))
}

// tuple nodes' tests

func TestDeduplication(t *testing.T) {
	// A -- B -- C
	// A broadcasts, then B and C response to it with the same content;
	// B should be able to intercept the response from C, because it knows
	// it is a duplicated response by checking its response cache.
	mnet := mock.NewMockNet()
	hostA, err := mnet.GenPeerWithMarshalablePrivKey()
	assert.NoError(t, err)

	hostC, err := mnet.GenPeerWithMarshalablePrivKey()
	assert.NoError(t, err)
	hostB, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pscA, err := NewRelayPubSubCollector(hostA)
	assert.NoError(t, err)
	pscB, err := NewRelayPubSubCollector(hostB)
	assert.NoError(t, err)
	pscC, err := NewRelayPubSubCollector(hostC)
	assert.NoError(t, err)
	_, err = mnet.ConnectPeers(hostA.ID(), hostB.ID())
	assert.NoError(t, err)
	_, err = mnet.ConnectPeers(hostB.ID(), hostC.ID())
	assert.NoError(t, err)

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	recvB := int32(0)
	handlerForB := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, hostA.ID(), peer.ID(r.Control.Root))
		assert.Equal(t, hostA.ID(), peer.ID(r.Control.From))
		atomic.StoreInt32(&recvB, 1)
		out := &Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}

	recvC := int32(0)
	handlerForC := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, hostA.ID(), peer.ID(r.Control.Root))
		if hostA.ID() == peer.ID(r.Control.From) {
			assert.FailNow(t, "C isn't expected to receive req from A")
			return nil
		}
		assert.Equal(t, hostB.ID(), peer.ID(r.Control.From))
		atomic.StoreInt32(&recvC, 1)
		out := &Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}

	err = pscA.Join(topic)
	assert.NoError(t, err)
	err = pscB.Join(topic, WithRequestHandler(handlerForB))
	assert.NoError(t, err)
	err = pscC.Join(topic, WithRequestHandler(handlerForC))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	// add count by 1 when final handler is called
	count := int32(0)
	finalHandler := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		atomic.AddInt32(&count, 1)
	}
	err = pscA.Publish(topic, payload, WithFinalRespHandler(finalHandler))
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1))
	assert.Equal(t, atomic.LoadInt32(&recvB), int32(1))
	assert.Equal(t, atomic.LoadInt32(&recvC), int32(1))
}

func TestNoRequestIDForResponse(t *testing.T) {
	// A -- B -- C
	// A broadcasts a request R, and B should know R's route;
	// what will happen if R expired in B,
	// and at the same time C send R's response to B?
	// We have some strategy to skip the forgetful node.
	// for example, we can forward this response to a random node.
	// But now, we just drop it.
	mnet := mock.NewMockNet()
	hostA, err := mnet.GenPeerWithMarshalablePrivKey()
	assert.NoError(t, err)
	hostC, err := mnet.GenPeerWithMarshalablePrivKey()
	assert.NoError(t, err)
	hostB, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pscA, err := NewRelayPubSubCollector(hostA)
	assert.NoError(t, err)
	pscB, err := NewRelayPubSubCollector(hostB)
	assert.NoError(t, err)
	pscC, err := NewRelayPubSubCollector(hostC)
	assert.NoError(t, err)
	_, err = mnet.ConnectPeers(hostA.ID(), hostB.ID())
	assert.NoError(t, err)
	_, err = mnet.ConnectPeers(hostB.ID(), hostC.ID())
	assert.NoError(t, err)

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}
	payloadB := []byte{1, 2, 3}
	payloadC := []byte{3, 2, 1}

	recvB := int32(0)
	handlerForB := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, hostA.ID(), peer.ID(r.Control.Root))
		assert.Equal(t, hostA.ID(), peer.ID(r.Control.From), "B is expected to receive req from A")
		atomic.StoreInt32(&recvB, 1)
		out := &Intermediate{
			Sendback: true,
			Payload:  payloadB,
		}
		return out
	}
	okch := make(chan struct{}, 1)

	recvC := int32(0)
	handlerForC := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, hostA.ID(), peer.ID(r.Control.Root))
		assert.Equal(t, hostB.ID(), peer.ID(r.Control.From), "C is expected to receive req from B")
		atomic.StoreInt32(&recvC, 1)
		out := &Intermediate{
			Sendback: true,
			Payload:  payloadC,
		}
		// wait until A has received response from B, now it's safe to clear B's request cache
		// B will not forward C's response to A
		<-okch
		pscB.reqCache.RemoveAll()
		return out
	}

	err = pscA.Join(topic)
	assert.NoError(t, err)
	err = pscB.Join(topic, WithRequestHandler(handlerForB))
	assert.NoError(t, err)
	err = pscC.Join(topic, WithRequestHandler(handlerForC))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	// add count by 1 when final handler is called
	count := int32(0)
	finalHandler := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payloadB, rp.Payload)
		atomic.AddInt32(&count, 1)
		okch <- struct{}{}
	}
	err = pscA.Publish(topic, payload, WithFinalRespHandler(finalHandler))
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1), "A should receive only 1 response")
	assert.Equal(t, atomic.LoadInt32(&recvB), int32(1))
	assert.Equal(t, atomic.LoadInt32(&recvC), int32(1))
}
