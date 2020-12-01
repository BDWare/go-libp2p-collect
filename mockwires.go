package collect

import (
	"context"
	"io/ioutil"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	mockWireProtocolID = protocol.ID("/mock/1.0.0")
)

type mockWires struct {
	h  host.Host
	wn WireListener
}

func (w *mockWires) ID() peer.ID {
	return w.h.ID()
}
func (w *mockWires) Neighbors() []peer.ID {
	return w.h.Network().Peers()
}
func (w *mockWires) SetListener(wn WireListener) {
	w.wn = wn
}
func (w *mockWires) SendMsg(to peer.ID, data []byte) error {
	s, err := w.h.NewStream(context.Background(), to, mockWireProtocolID)
	if err != nil {
		return err
	}
	_, err = s.Write(data)
	defer s.Close()
	if err != nil {
		return err
	}
	return nil
}
func (w *mockWires) SetMsgHandler(handle MsgHandler) {
	w.h.SetStreamHandler(mockWireProtocolID, func(s network.Stream) {
		bin, err := ioutil.ReadAll(s)
		if err != nil {
			panic(err)
		}
		handle(s.Conn().RemotePeer(), bin)
	})
}

func (w *mockWires) Listen(network.Network, ma.Multiaddr)      {}
func (w *mockWires) ListenClose(network.Network, ma.Multiaddr) {}
func (w *mockWires) Connected(net network.Network, conn network.Conn) {
	w.wn.HandlePeerUp(conn.RemotePeer())
}
func (w *mockWires) Disconnected(net network.Network, conn network.Conn) {
	w.wn.HandlePeerDown(conn.RemotePeer())
}
func (w *mockWires) OpenedStream(network.Network, network.Stream) {}
func (w *mockWires) ClosedStream(network.Network, network.Stream) {}

func NewWiresFromHost(h host.Host) Wires {
	return &mockWires{
		h:  h,
		wn: &defaultWireNotifiee{},
	}
}
