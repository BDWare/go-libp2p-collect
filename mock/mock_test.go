package mock_test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"bdware.org/libp2p/go-libp2p-collect/mock"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/stretchr/testify/assert"
)

func TestSendAndRecv(t *testing.T) {
	mnet := mock.NewMockNet()
	send, err := mnet.NewConnectedPeer()
	assert.NoError(t, err)
	recv, err := mnet.NewConnectedPeer()
	assert.NoError(t, err)
	protoc := protocol.ID("test-protocol")
	expect := []byte{1, 2, 3}
	okch := make(chan struct{})
	recv.SetStreamHandler(protoc, func(s network.Stream) {
		actual, err := ioutil.ReadAll(s)
		s.Close()
		assert.NoError(t, err)
		assert.Equal(t, expect, actual)
		okch <- struct{}{}
	})
	s, err := send.NewStream(context.Background(), recv.ID(), protoc)
	assert.NoError(t, err)
	_, err = s.Write(expect)
	assert.NoError(t, err)
	err = s.Close()
	assert.NoError(t, err)
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "handler is not called")
	case <-okch:
	}
}
