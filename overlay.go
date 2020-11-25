package collect

import (
	"context"
	"sync"

	"github.com/bdware/go-libp2p-collect/pb"
	"github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	msgio "github.com/libp2p/go-msgio"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	ProtocolID = protocol.ID("/topicOverlay/1.0.0")
)

type TopicBasedOverlay struct {
	rw     sync.RWMutex
	h      host.Host
	joined map[string]struct{}
	topics map[string]*PeerSet
	notif  Notifiee
	log    Logger
}

func NewTopicBasedOverlay(h host.Host) (*TopicBasedOverlay, error) {

	t := &TopicBasedOverlay{
		rw:     sync.RWMutex{},
		h:      h,
		joined: make(map[string]struct{}),
		topics: make(map[string]*PeerSet),
		notif:  defaultNotif{},
	}

	t.h.SetStreamHandler(ProtocolID, t.handleStream)
	h.Network().Notify((*TopicBasedOverlayNotif)(t))
	return t, nil
}

func (t *TopicBasedOverlay) Join(topic string) error {
	t.rw.Lock()
	defer t.rw.Unlock()
	// add topic
	t.joined[topic] = struct{}{}

	// send peer
	for _, peerid := range t.h.Network().Peers() {
		err := t.sendMsg(peerid, []string{topic}, pb.Message_JOIN)
		if err != nil {
			// TODO: log
		}
	}

	// notify
	for _, peerid := range t.topics[topic].Slice() {
		go t.notif.HandleNeighUp(peerid, topic)
	}

	return nil
}

func (t *TopicBasedOverlay) Leave(topic string) error {
	t.rw.Lock()
	defer t.rw.Unlock()

	// notify our neighbors
	neighs, ok := t.topics[topic]
	if !ok {
		// have left
		return nil
	}
	for _, p := range neighs.Slice() {
		go func(p peer.ID) {
			err := t.sendMsg(p, []string{topic}, pb.Message_LEAVE)
			if err != nil {
				// TODO: log
			}
		}(p)
		go t.notif.HandleNeighDown(p, topic)
	}
	// delete topic
	delete(t.joined, topic)
	delete(t.topics, topic)

	return nil
}

func (t *TopicBasedOverlay) Neighbors(topic string) []peer.ID {
	t.rw.RLock()
	defer t.rw.RUnlock()
	ps, ok := t.topics[topic]
	if !ok {
		return []peer.ID{}
	}
	return ps.Slice()
}

func (t *TopicBasedOverlay) Connected() []peer.ID {
	t.rw.RLock()
	defer t.rw.RUnlock()
	return t.h.Network().Peers()
}

func (t *TopicBasedOverlay) Topics() []string {
	t.rw.RLock()
	defer t.rw.RUnlock()
	return t.joinedTopics()
}

func (t *TopicBasedOverlay) SetNotifiee(notifiee Notifiee) {
	t.rw.Lock()
	defer t.rw.Unlock()
	t.notif = notifiee
}

func (t *TopicBasedOverlay) Close() error {
	t.rw.Lock()
	defer t.rw.Unlock()
	t.h.Network().StopNotify((*TopicBasedOverlayNotif)(t))
	topics := make([]string, 0, len(t.joined))
	for topic := range t.joined {
		topics = append(topics, topic)
	}
	// send peer
	for _, peerid := range t.h.Network().Peers() {
		err := t.sendMsg(peerid, topics, pb.Message_JOIN)
		if err != nil {
			// TODO: log
		}
	}
	return nil
}

/*===========================================================================*/

func (t *TopicBasedOverlay) joinedTopics() []string {
	out := make([]string, 0, len(t.joined))
	for topic := range t.joined {
		out = append(out, topic)
	}
	return out
}

func (t *TopicBasedOverlay) sendMsg(to peer.ID, topics []string, msgType pb.Message_MsgType) error {
	msg := &pb.Message{
		MsgType: msgType,
		Topics:  topics,
	}
	msgBin, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	s, err := t.h.NewStream(context.Background(), to, ProtocolID)
	if err != nil {
		return err
	}
	mrw := msgio.NewReadWriter(s)
	defer mrw.Close()
	err = mrw.WriteMsg(msgBin)
	if err != nil {
		return err
	}
	return nil
}

/*===========================================================================*/

func (t *TopicBasedOverlay) handleStream(s network.Stream) {
	msg := &pb.Message{}
	mrw := msgio.NewReadWriter(s)
	defer s.Close()
	bin, err := mrw.ReadMsg()
	if err != nil {
		return
	}
	err = proto.Unmarshal(bin, msg)
	if err != nil {
		return
	}
	from := s.Conn().RemotePeer()
	switch msg.MsgType {
	case pb.Message_JOIN:

		t.handlePeerJoin(from, msg.Topics)
	case pb.Message_LEAVE:
		t.handlePeerLeave(from, msg.Topics)
	}
}

func (t *TopicBasedOverlay) handlePeerDown(p peer.ID) {
	t.rw.Lock()
	defer t.rw.Unlock()
	for topic, ps := range t.topics {
		ps.Del(p)
		go t.notif.HandleNeighDown(p, topic)
	}
}

func (t *TopicBasedOverlay) handlePeerJoin(p peer.ID, topics []string) {
	t.rw.Lock()
	defer t.rw.Unlock()
	for _, topic := range topics {
		ps, ok := t.topics[topic]
		if !ok {
			ps = NewPeerSet()
			t.topics[topic] = ps
		}
		ps.Add(p)
		if _, ok := t.joined[topic]; !ok {
			// received join request from p in a non-exist topic
			// add p in topics, but don't bother notifiee
			continue
		}
		go t.notif.HandleNeighUp(p, topic)
	}
}

func (t *TopicBasedOverlay) handlePeerLeave(p peer.ID, topics []string) {
	t.rw.Lock()
	defer t.rw.Unlock()
	for _, topic := range topics {
		ps, ok := t.topics[topic]
		if !ok {
			// p has left
			continue
		}
		ps.Del(p)
		go t.notif.HandleNeighDown(p, topic)
	}
}

/*===========================================================================*/

type Notifiee interface {
	HandleNeighUp(neigh peer.ID, topic string)
	HandleNeighDown(neigh peer.ID, topic string)
}

type defaultNotif struct{}

func (d defaultNotif) HandleNeighUp(neigh peer.ID, topic string)   {}
func (d defaultNotif) HandleNeighDown(neigh peer.ID, topic string) {}

/*===========================================================================*/
// TopicBasedOverlay as network notifiee

type TopicBasedOverlayNotif TopicBasedOverlay

func (tn *TopicBasedOverlayNotif) OpenedStream(n network.Network, s network.Stream) {}
func (tn *TopicBasedOverlayNotif) ClosedStream(n network.Network, s network.Stream) {}
func (tn *TopicBasedOverlayNotif) Listen(n network.Network, a ma.Multiaddr)         {}
func (tn *TopicBasedOverlayNotif) ListenClose(n network.Network, a ma.Multiaddr)    {}
func (tn *TopicBasedOverlayNotif) Connected(n network.Network, c network.Conn) {
	t := ((*TopicBasedOverlay)(tn))
	t.rw.RLock()
	defer t.rw.RUnlock()
	go func() {
		err := t.sendMsg(c.RemotePeer(), t.joinedTopics(), pb.Message_JOIN)
		if err != nil {
			// log
			t.log.Logf("error", "send msg:err=%v", err)
		}
	}()
}

func (tn *TopicBasedOverlayNotif) Disconnected(n network.Network, c network.Conn) {
	((*TopicBasedOverlay)(tn)).handlePeerDown(c.RemotePeer())
}
