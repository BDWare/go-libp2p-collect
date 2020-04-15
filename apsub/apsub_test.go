package apsub_test

import (
	"context"
	"testing"
	"time"

	. "bdware.org/libp2p/go-libp2p-collect/apsub"
	"bdware.org/libp2p/go-libp2p-collect/mock"
	"github.com/stretchr/testify/assert"
)

func TestPubSub(t *testing.T) {
	mnet := mock.NewMockNet()
	pubhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)
	subhost, err := mnet.NewLinkedPeer()
	assert.NoError(t, err)

	// Even if hosts are connected,
	// the topics may not find the pre-exist connections.
	// We establish connections after topics are created.
	pub, err := NewAsyncPubSub(pubhost)
	assert.NoError(t, err)
	sub, err := NewAsyncPubSub(subhost)
	assert.NoError(t, err)
	mnet.ConnectPeers(pubhost.ID(), subhost.ID())

	// wait for discovery
	time.Sleep(50 * time.Millisecond)

	expecttopic := "test-topic"
	expectdata := []byte{1, 2, 3}
	okch := make(chan struct{})
	subhandle := func(topic string, msg *Message) {
		assert.Equal(t, expecttopic, topic)
		assert.Equal(t, expectdata, msg.Data)
		// to make sure handle is called
		okch <- struct{}{}
	}
	err = sub.Subscribe(expecttopic, subhandle)
	assert.NoError(t, err)
	// wait for subscription
	time.Sleep(50 * time.Millisecond)
	err = pub.Publish(context.TODO(), expecttopic, expectdata)
	assert.NoError(t, err)
	select {
	case <-time.After(1 * time.Second):
		assert.Fail(t, "subhandle is not called in 1s")
	case <-okch:
	}
}
