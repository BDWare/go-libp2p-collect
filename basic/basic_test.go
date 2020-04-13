package basicpsc

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"

	"bdware.org/libp2p/go-libp2p-collect/mock"
	"bdware.org/libp2p/go-libp2p-collect/opt"
	"bdware.org/libp2p/go-libp2p-collect/pb"
)

func TestSendRecv(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewBasicPubSubCollector(pubhost)
	assert.NoError(t, err)
	sub, err := NewBasicPubSubCollector(subhost)
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

	err = pub.Join(topic, opt.WithRequestHandler(handlePub))
	assert.NoError(t, err)
	err = sub.Join(topic, opt.WithRequestHandler(handleSub))
	assert.NoError(t, err)

	// time to join
	time.Sleep(50 * time.Millisecond)

	okch := make(chan struct{})
	notif := func(rp *pb.Response) {
		assert.Equal(t, payload, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, opt.WithRecvRespHandler(notif))
	assert.NoError(t, err)

	// after 2 seconds, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

// func TestMaxRequestSize(t *testing.T) {
// 	mnet := mock.NewMockNet()
// 	pubhost, err := mnet.NewLinkedPeer()
// 	assert.NoError(t, err)
// 	subhost, err := mnet.NewLinkedPeer()
// 	assert.NoError(t, err)

// 	// we set up a limit for request number.
// 	// Once a node has send or receive requests that reach the limit,
// 	// we use a strategy (lru，et al.) to restrict the request.
// 	var (
// 		limit = uint32(100)
// 	)

// 	limitedConf := MakeDefaultConf()
// 	limitedConf.RequestBufSize = int(limit)

// 	// Even if hosts are connected,
// 	// the topics may not find the pre-exist connections.
// 	// We establish connections after pscs are created.
// 	pub, err := NewBasicPubSubCollector(pubhost, WithConf(&limitedConf))
// 	assert.NoError(t, err)
// 	sub, err := NewBasicPubSubCollector(subhost)
// 	assert.NoError(t, err)
// 	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

// 	time.Sleep(50 * time.Millisecond)

// 	topic := "test-topic"
// 	payload := []byte{1, 2, 3}

// 	// handlePub and handleSub is both request handle,
// 	// but handleSub will send back the response
// 	handlePub := func(ctx context.Context, r *pb.Request) *pb.Intermediate {
// 		assert.Equal(t, payload, r.Payload)
// 		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))
// 		out := &pb.Intermediate{
// 			Sendback: false,
// 			Payload:  payload,
// 		}
// 		return out
// 	}

// 	var requests []*pb.Request
// 	// lock protects requests
// 	var lock sync.Mutex

// 	handleSub := func(ctx context.Context, r *pb.Request) *pb.Intermediate {
// 		assert.Equal(t, payload, r.Payload)
// 		assert.Equal(t, pubhost.ID(), peer.ID(r.Control.Root))

// 		// records requests in requests.
// 		// after we check the counter, we will replay the response
// 		lock.Lock()
// 		requests = append(requests, r)
// 		lock.Unlock()

// 		out := &pb.Intermediate{
// 			Sendback: true,
// 			Payload:  payload,
// 		}
// 		return out
// 	}

// 	err = pub.Join(topic, opt.WithRequestHandler(handlePub))
// 	assert.NoError(t, err)
// 	err = sub.Join(topic, opt.WithRequestHandler(handleSub))
// 	assert.NoError(t, err)

// 	// time to join
// 	time.Sleep(50 * time.Millisecond)

// 	cnt := uint32(0)
// 	notif := func(rp *pb.Response) {
// 		atomic.AddUint32(&cnt, 1)
// 	}

// 	for i := uint32(0); i < 2*limit; i++ {
// 		err = pub.Publish(topic, payload, opt.WithRecvRespHandler(notif))
// 		assert.NoError(t, err)
// 	}

// 	// time to wait for handle
// 	time.Sleep(200 * time.Millisecond)

// 	assert.Equal(t, int(2*limit), int(cnt))
// 	assert.Equal(t, int(2*limit), len(requests))
// 	assert.Equal(t, int(limit), pub.rcache.Cache.Len())
// 	// clear cnt
// 	cnt = uint32(0)

// 	// we try to find out whether the requestIDs(seqnos) are still answerable
// 	// if the request size is confined, we will see some of the notif will not be called.
// 	idgen := defaultReqIDGenerator()
// 	for _, r := range requests {
// 		// contruct response once again
// 		resp := &pb.Response{
// 			Control: &pb.ResponseControl{
// 				RequestId: idgen(r),
// 			},
// 			Payload: payload,
// 		}
// 		data, err := resp.Marshal()
// 		assert.NoError(t, err)

// 		// replay the response
// 		s, err := subhost.NewStream(
// 			context.Background(),
// 			pubhost.ID(),
// 			sub.conf.ResponseProtocol,
// 		)
// 		assert.NoError(t, err)
// 		_, err = s.Write(data)
// 		assert.NoError(t, err)
// 		err = s.Close()
// 		assert.NoError(t, err)
// 	}

// 	// time to wait for handling
// 	time.Sleep(200 * time.Millisecond)
// 	assert.Equal(t, int(limit), int(cnt))

// }

func TestLeaveAndJoin(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewBasicPubSubCollector(pubhost)
	assert.NoError(t, err)
	sub, err := NewBasicPubSubCollector(subhost)
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

	err = pub.Join(topic, opt.WithRequestHandler(handlePub))
	assert.NoError(t, err)
	err = sub.Join(topic, opt.WithRequestHandler(handleSub))
	assert.NoError(t, err)
	// time to join
	time.Sleep(50 * time.Millisecond)

	okch := make(chan struct{})

	notifOK := func(rp *pb.Response) {
		assert.Equal(t, payload, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, opt.WithRecvRespHandler(notifOK))
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

	notifFail := func(rp *pb.Response) {
		assert.FailNow(t, "should not recv message")
	}
	err = pub.Publish(topic, payload, opt.WithRecvRespHandler(notifFail))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// let sub join again
	err = sub.Join(topic, opt.WithRequestHandler(handleSub))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	err = pub.Publish(topic, payload, opt.WithRecvRespHandler(notifOK))
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	// after 2 seconds, if okch receive nothing, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

func TestSelfNotif(t *testing.T) {
	mnet := mock.NewMockNet()
	h, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewBasicPubSubCollector(h)
	assert.NoError(t, err)

	topic := "test-topic"
	payload := []byte{1, 2, 3}

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *pb.Request) *pb.Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, h.ID(), peer.ID(r.Control.Root))
		out := &pb.Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}

	err = pub.Join(topic, opt.WithRequestHandler(handlePub))
	assert.NoError(t, err)

	okch := make(chan struct{})
	notif := func(rp *pb.Response) {
		assert.Equal(t, payload, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, opt.WithRecvRespHandler(notif))
	assert.NoError(t, err)

	// after 2 seconds, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}

func TestRejoin(t *testing.T) {
	mnet := mock.NewMockNet()
	h, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	pub, err := NewBasicPubSubCollector(h)
	assert.NoError(t, err)

	topic := "test-topic"
	payload := []byte{1, 2, 3}
	another := []byte{4, 5, 6}

	// handlePub and handleSub is both request handle,
	// but handleSub will send back the response
	handlePub := func(ctx context.Context, r *pb.Request) *pb.Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, h.ID(), peer.ID(r.Control.Root))
		out := &pb.Intermediate{
			Sendback: true,
			Payload:  payload,
		}
		return out
	}
	anotherHandle := func(ctx context.Context, r *pb.Request) *pb.Intermediate {
		assert.Equal(t, payload, r.Payload)
		assert.Equal(t, h.ID(), peer.ID(r.Control.Root))
		out := &pb.Intermediate{
			Sendback: true,
			Payload:  another,
		}
		return out
	}

	err = pub.Join(topic, opt.WithRequestHandler(handlePub))
	assert.NoError(t, err)

	// We join the same topic for a second time.
	err = pub.Join(topic, opt.WithRequestHandler(anotherHandle))
	assert.NoError(t, err)

	okch := make(chan struct{})
	notif := func(rp *pb.Response) {
		assert.Equal(t, another, rp.Payload)
		okch <- struct{}{}
	}
	err = pub.Publish(topic, payload, opt.WithRecvRespHandler(notif))
	assert.NoError(t, err)

	// after 2 seconds, test will failed
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "we don't receive enough response in 2s")
	case <-okch:
	}
}
