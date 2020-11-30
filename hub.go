package collect

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
)

type TopicHub struct {
	tw        TopicWires
	rw        sync.RWMutex
	outbounds map[string]*outbound
}

func NewTopicHub(tw TopicWires) *TopicHub {
	out := &TopicHub{
		tw:        tw,
		rw:        sync.RWMutex{},
		outbounds: make(map[string]*outbound),
	}
	tw.SetListener(out)
	return out
}

func (th *TopicHub) Wires(topic string) Wires {
	th.rw.RLock()
	defer th.rw.RUnlock()
	return th.outbounds[topic]
}

func (th *TopicHub) Join(topic string) error {
	return th.JoinWithNotifiee(topic, &defaultWireNotifiee{})
}

func (th *TopicHub) JoinWithNotifiee(topic string, wn WireListener) error {
	th.rw.Lock()
	defer th.rw.Unlock()
	if _, ok := th.outbounds[topic]; ok {
		return ErrTopicJoined
	}
	o := th.newOutbound(topic)
	o.SetListener(wn)
	th.outbounds[topic] = o
	return th.tw.Join(topic)
}

func (th *TopicHub) Leave(topic string) error {
	th.rw.Lock()
	defer th.rw.Unlock()
	return th.leave(topic)
}

func (th *TopicHub) Close() error {
	th.rw.Lock()
	defer th.rw.Unlock()
	for topic := range th.outbounds {
		if err := th.leave(topic); err != nil {
			// TODO: log
			continue
		}
	}
	return nil
}

func (th *TopicHub) leave(topic string) error {
	if _, ok := th.outbounds[topic]; !ok {
		return ErrTopicNotJoined
	}
	delete(th.outbounds, topic)
	return th.tw.Leave(topic)
}

func (th *TopicHub) HandlePeerUp(p peer.ID, topic string) {
	th.rw.RLock()
	defer th.rw.RUnlock()
	o, ok := th.outbounds[topic]
	if !ok {
		// TODO
		return
	}
	o.listener.HandlePeerUp(p)
}

func (th *TopicHub) HandlePeerDown(p peer.ID, topic string) {
	th.rw.RLock()
	defer th.rw.RUnlock()
	o, ok := th.outbounds[topic]
	if !ok {
		// TODO
		return
	}
	o.listener.HandlePeerDown(p)
}

func (th *TopicHub) handleMsg(topic string, from peer.ID, data []byte) {
	th.rw.RLock()
	defer th.rw.RUnlock()
	o, ok := th.outbounds[topic]
	if !ok {
		//TODO: receive unsubscribed topic
		return
	}
	o.handler(from, data)
}

func (th *TopicHub) newOutbound(topic string) *outbound {
	return &outbound{
		hub:      th,
		topic:    topic,
		listener: &defaultWireNotifiee{},
		handler:  func(from peer.ID, data []byte) {},
	}
}

type outbound struct {
	hub      *TopicHub
	topic    string
	listener WireListener
	handler  MsgHandler
}

func (o *outbound) Neighbors() []peer.ID {
	return o.hub.tw.Neighbors(o.topic)
}

func (o *outbound) SetListener(wl WireListener) {
	o.listener = wl
}

func (o *outbound) SendMsg(to peer.ID, data []byte) error {
	return o.hub.tw.SendMsg(o.topic, to, data)
}
func (o *outbound) SetMsgHandler(h MsgHandler) {
	o.handler = h
}

type defaultWireNotifiee struct{}

func (d *defaultWireNotifiee) HandlePeerUp(p peer.ID)   {}
func (d *defaultWireNotifiee) HandlePeerDown(p peer.ID) {}
