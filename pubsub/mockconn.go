package pubsub

import (
	ic "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"

	ma "github.com/multiformats/go-multiaddr"
)

//
type MockConn struct {
	Remote peer.ID
}

// NewStream constructs a new Stream over this conn.
func (mc *MockConn) NewStream() (network.Stream, error) {
	panic("not implemented")
}

// GetStreams returns all open streams over this conn.
func (mc *MockConn) GetStreams() []network.Stream {
	panic("not implemented")
}

// Stat stores metadata pertaining to this conn.
func (mc *MockConn) Stat() network.Stat {
	panic("not implemented")
}

// LocalPeer returns our peer ID
func (mc *MockConn) LocalPeer() peer.ID {
	panic("not implemented")
}

// LocalPrivateKey returns our private key
func (mc *MockConn) LocalPrivateKey() ic.PrivKey {
	panic("not implemented")
}

// RemotePeer returns the peer ID of the remote peer.
func (mc *MockConn) RemotePeer() peer.ID {
	return mc.Remote
}

// RemotePublicKey returns the public key of the remote peer.
func (mc *MockConn) RemotePublicKey() ic.PubKey {
	panic("not implemented")
}

// LocalMultiaddr returns the local Multiaddr associated
// with this connection
func (mc *MockConn) LocalMultiaddr() ma.Multiaddr {
	panic("not implemented")
}

// RemoteMultiaddr returns the remote Multiaddr associated
// with this connection
func (mc *MockConn) RemoteMultiaddr() ma.Multiaddr {
	panic("not implemented")
}

func (mc *MockConn) Close() error {
	panic("not implemented")
}
