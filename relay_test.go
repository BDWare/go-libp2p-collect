package collect

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"sync/atomic"

	"github.com/bdware/go-libp2p-collect/mock"
	"github.com/bdware/go-libp2p-collect/pb"
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
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
		out := &Intermediate{
			Sendback: false,
			Payload:  payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
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
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
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
		assert.Equal(t, h.ID(), r.Control.Requester)
		out := &Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}
	anotherHandle := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, h.ID(), r.Control.Requester)
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
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
		out := &Intermediate{
			Sendback: false,
			Payload:  payload,
		}
		return out
	}
	handleSub := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, pubhost.ID(), r.Control.Requester)
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
	// |
	// C
	// B,C return the same response to A, A drop the second one.
	mnet := mock.NewMockNet()
	ahost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	bhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	chost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	a, err := NewRelayPubSubCollector(ahost)
	assert.NoError(t, err)
	b, err := NewRelayPubSubCollector(bhost)
	assert.NoError(t, err)
	c, err := NewRelayPubSubCollector(bhost)
	assert.NoError(t, err)

	mnet.ConnectPeers(ahost.ID(), bhost.ID())
	mnet.ConnectPeers(ahost.ID(), chost.ID())

	// time to connect
	time.Sleep(50 * time.Millisecond)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	handle := func(ctx context.Context, req *Request) *Intermediate {
		out := &Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}

	err = a.Join(topic)
	assert.NoError(t, err)
	err = b.Join(topic, WithRequestHandler(handle))
	assert.NoError(t, err)
	err = c.Join(topic, WithRequestHandler(handle))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	// add count by 1 when final handler is called
	count := int32(0)
	finalHandler := func(ctx context.Context, rp *Response) {
		assert.Equal(t, payload, rp.Payload)
		atomic.AddInt32(&count, 1)
	}
	err = a.Publish(topic, payload, WithFinalRespHandler(finalHandler))
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1))
}

// tuple nodes' tests

func TestDeduplication(t *testing.T) {
	// A -- B -- C
	// A broadcasts, then B and C response to it with the same content;
	// B should be able to intercept the response from C, because it knows
	// it is a duplicated response by checking its response cache.
	mnet := mock.NewMockNet()
	hostA, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	hostB, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	hostC, err := mnet.NewLinkedPeer()
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

	handle := func(ctx context.Context, r *Request) *Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, hostA.ID(), r.Control.Requester)
		out := &Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}

	err = pscA.Join(topic)
	assert.NoError(t, err)
	err = pscB.Join(topic, WithRequestHandler(handle))
	assert.NoError(t, err)
	err = pscC.Join(topic, WithRequestHandler(handle))
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

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1))
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
	hostA, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	hostC, err := mnet.NewLinkedPeer()
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
		assert.Equal(t, hostA.ID(), r.Control.Requester)
		assert.Equal(t, hostA.ID(), r.Control.Sender, "B is expected to receive req from A")
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
		assert.Equal(t, hostA.ID(), r.Control.Requester)
		assert.Equal(t, hostB.ID(), r.Control.Sender, "C is expected to receive req from B")
		atomic.StoreInt32(&recvC, 1)
		out := &Intermediate{
			Sendback: true,
			Payload:  payloadC,
		}
		// wait until A has received response from B, now it's safe to clear B's request cache
		// B will not forward C's response to A
		<-okch
		pscB.reqWorkerPool.RemoveAll()
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

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, atomic.LoadInt32(&count), int32(1), "A should receive only 1 response")
	assert.Equal(t, atomic.LoadInt32(&recvB), int32(1))
	assert.Equal(t, atomic.LoadInt32(&recvC), int32(1))
}

func TestNoMoreIncoming(t *testing.T) {
	// A -- B
	// |
	// C
	mnet := mock.NewMockNet()

	childrenCnt := 1
	topic := "test-topic"
	payload := []byte{1, 2, 3}
	handleCnt := int32(0)
	handler := func(ctx context.Context, req *Request) *Intermediate {
		randPayload := make([]byte, 10)
		rand.Read(randPayload)
		atomic.AddInt32(&handleCnt, 1)
		return &pb.Intermediate{
			Sendback: true,
			Payload:  randPayload,
		}
	}

	hostA, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	pscA, err := NewRelayPubSubCollector(hostA)
	assert.NoError(t, err)
	err = pscA.Join(topic, WithRequestHandler(handler))
	assert.NoError(t, err)

	for i := 0; i < childrenCnt; i++ {
		childHost, err := mnet.NewLinkedPeer()
		assert.NoError(t, err)
		childPSC, err := NewRelayPubSubCollector(childHost)
		assert.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
		_, err = mnet.ConnectPeers(hostA.ID(), childHost.ID())
		assert.NoError(t, err)
		err = childPSC.Join(topic, WithRequestHandler(handler))
		assert.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)
	falseCnt := int32(0)
	trueCnt := int32(0)
	final := func(ctx context.Context, resp *Response) {
		if !resp.Control.NoMoreIncoming {
			atomic.AddInt32(&falseCnt, 1)
		} else {
			atomic.AddInt32(&trueCnt, 1)
		}
	}
	pscA.Publish(topic, payload, WithFinalRespHandler(final))
	// time to wait for event handling
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&trueCnt))
	assert.Equal(t, int32(childrenCnt), atomic.LoadInt32(&falseCnt))
	assert.Equal(t, int32(childrenCnt+1), atomic.LoadInt32(&handleCnt))
}

type testLogger testing.T

func (l *testLogger) Logf(level, format string, args ...interface{}) {
	(*testing.T)(l).Logf("%s:", level)
	(*testing.T)(l).Logf(format, args...)
}
