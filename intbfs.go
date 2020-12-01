package collect

import (
	"context"
	"math/rand"
	"sort"
	"sync"

	"github.com/bdware/go-libp2p-collect/pb"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const (
	IntBFSProtocolID = protocol.ID("/intbfs/1.0.0")
)

// IntBFSCollector .
// Unlike basic and relay pubsubCollector, IntBFSCollector isn't based on go-libp2p-pubsub.
// We used a topic-defined overlay here.
type IntBFSCollector struct {
	rw     sync.RWMutex
	host   host.Host
	topics map[string]*IntBFS
	hub    *TopicHub
	log    Logger
}

func NewIntBFSCollector(h host.Host, opts ...InitOpt) (*IntBFSCollector, error) {
	initOpts, err := NewInitOpts(opts)
	if err != nil {
		return nil, err
	}
	if initOpts.Wires == nil {
		return nil, ErrNilTopicWires
	}
	ic := &IntBFSCollector{
		rw:     sync.RWMutex{},
		host:   h,
		topics: make(map[string]*IntBFS),
		hub:    NewTopicHub(initOpts.Wires),
		log:    initOpts.Logger,
	}
	return ic, nil
}

func (ic *IntBFSCollector) Join(topic string, opts ...JoinOpt) (err error) {
	ic.rw.Lock()
	defer ic.rw.Unlock()

	if _, ok := ic.topics[topic]; ok {
		ic.log.Logf("error", "Join: topic %s joined", topic)
		return ErrTopicJoined
	}

	err = ic.hub.Join(topic)
	if err != nil {
		return err
	}

	wires := ic.hub.Wires(topic)

	intbfs, err := NewIntBFS(
		wires,
		nil, // TODO
	)
	if err != nil {
		return err
	}

	wires.SetListener(intbfs)
	wires.SetMsgHandler(intbfs.HandleMsg)

	ic.topics[topic] = intbfs

	return nil
}

func (ic *IntBFSCollector) Publish(topic string, data []byte, opts ...PubOpt) error {
	ic.rw.RLock()
	defer ic.rw.RUnlock()
	intbfs, ok := ic.topics[topic]
	if !ok {
		ic.log.Logf("error", "Publish: topic %s not joined", topic)
		return ErrTopicNotJoined
	}
	return intbfs.Publish(data, opts...)
}

func (ic *IntBFSCollector) Leave(topic string) (err error) {
	ic.rw.Lock()
	defer ic.rw.Unlock()
	_, ok := ic.topics[topic]
	if !ok {
		ic.log.Logf("warn", "Leave: %s has left", topic)
	}
	err = ic.hub.Leave(topic)
	if err != nil {
		return err
	}
	delete(ic.topics, topic)
	return nil
}

func (ic *IntBFSCollector) Close() error {
	ic.rw.Lock()
	defer ic.rw.Unlock()
	return ic.hub.Close()
}

/*===========================================================================*/

// IntBFS don't care about topic.
// IntBFS contains 5 parts:
// 1. query machanism
// 2. peer profiles
// 3. peer ranking
// 4. distance function
// 5. random perturbation

const (
	k = 5 // fanout
	r = 1 // random perturbation
)

type IntBFS struct {
	rw          sync.RWMutex
	wires       Wires
	profFactory ProfileFactory
	reqHandler  RequestHandler
	profiles    map[peer.ID]Profile
	log         Logger
	seqno       uint64
	pool        requestWorkerPool
}

func NewIntBFS(wires Wires, opts *IntBFSOptions) (*IntBFS, error) {

	out := &IntBFS{
		rw:          sync.RWMutex{},
		wires:       wires,
		profFactory: nil,
		profiles:    make(map[peer.ID]Profile),
		seqno:       rand.Uint64(),
	}
	return out, nil
}

func (ib *IntBFS) Publish(data []byte, opts ...PubOpt) error {
	// po, err := NewPublishOptions(opts)
	// if err != nil {
	// 	return err
	// }
	// // assemble the request
	// myself := ib.wires.ID()
	// req := &Request{
	// 	Control: pb.RequestControl{
	// 		Requester: myself,
	// 		Sender:    myself,
	// 		Seqno:     atomic.AddUint64(&(ib.seqno), 1),
	// 	},
	// 	Payload: data,
	// }

	// set finalHandler

	// handle

	panic("not implemented")
}

func (ib *IntBFS) Close() error {
	panic("not implemented")
}

/*===========================================================================*/

func (ib *IntBFS) HandleMsg(from peer.ID, data []byte) {
	// decode msg
	msg := &pb.Msg{}
	err := msg.Unmarshal(data)
	if err != nil {
		ib.log.Logf("info", "msg unmarshal error:%v", err)
	}

	ib.rw.Lock()
	defer ib.rw.Unlock()
	// dispatch msg type
	switch msg.Type {
	case pb.Msg_Request:
		ib.handleIncomingRequest(from, msg.Request)
	case pb.Msg_Response:
		ib.handleIncomingResponse(from, msg.Response)
	case pb.Msg_Unknown:
		fallthrough
	default:
		ib.log.Logf("info", "unknown msg type")
		return
	}
}

func (ib *IntBFS) HandlePeerDown(p peer.ID) {
	ib.rw.Lock()
	defer ib.rw.Unlock()
	delete(ib.profiles, p)
}

func (ib *IntBFS) HandlePeerUp(p peer.ID) {
	ib.rw.Lock()
	defer ib.rw.Unlock()
	if _, ok := ib.profiles[p]; ok {
		ib.log.Logf("warn", "HandlePeerUp: %s profile exists", p.ShortString())
		return
	}
	prof, err := ib.profFactory()
	if err != nil {
		ib.log.Logf("error", "HandlePeerUp: init profile error:%v", err)
		return
	}
	ib.profiles[p] = prof
}

func (ib *IntBFS) handleIncomingRequest(from peer.ID, req *Request) error {
	// call request handler
	m := ib.reqHandler(context.TODO(), req)

	// check hit
	if m.Hit {
		err := ib.handleHit(from, req, m)
		if err != nil {
			return err
		}
	}

	return ib.handleForward(from, req)
}

func (ib *IntBFS) handleForward(from peer.ID, req *Request) error {
	// find k highest peerProfile peers
	// then find r random peers not within the previous k-set
	peers := ib.ranks(req.Payload)
	bound := k + r
	if len(peers) <= bound {
		bound = len(peers)
	}
	for i := k; i < bound; i++ {
		// swap peers between [k, len(peers))
		j := k + rand.Intn(len(peers)-k)
		peers[i], peers[j] = peers[j], peers[i]
	}
	// tosend is peers[0:k] + peers[k:bound]
	for _, to := range peers[:bound] {
		go func(to peer.ID, req *Request) {
			if err := ib.sendRequest(to, req); err != nil {
				ib.log.Logf("error", "handleForward: %v", err)
			}
		}(to, req)
	}
	return nil
}

func (ib *IntBFS) handleHit(from peer.ID, req *Request, intm *Intermediate) error {
	// insert req into profile
	// send control message to neighbors, tell them that you're hit.
	// send back response
	panic("not implemented")
}

func (ib *IntBFS) handleIncomingResponse(from peer.ID, resp *Response) error {
	// store query message according to cached content.
	// reply to father node.
	// if it is rooted node, reply to user.
	panic("not implemented")
}

/*===========================================================================*/
// helper
/*===========================================================================*/

func (ib *IntBFS) sendRequest(to peer.ID, req *Request) error {
	msg := &pb.Msg{
		Type:     pb.Msg_Request,
		Request:  req,
		Response: nil,
	}
	data, err := msg.Marshal()
	if err != nil {
		ib.log.Logf("error", "request marshal error:%v", err)
		return err
	}
	return ib.wires.SendMsg(to, data)
}

func (ib *IntBFS) sendResponse(to peer.ID, resp *Response) error {
	msg := &pb.Msg{
		Type:     pb.Msg_Response,
		Request:  nil,
		Response: resp,
	}
	data, err := msg.Marshal()
	if err != nil {
		ib.log.Logf("error", "response marshal error:%v", err)
		return err
	}
	return ib.wires.SendMsg(to, data)
}

type profileElement struct {
	p   peer.ID
	pro Profile
}

func (ib *IntBFS) ranks(reqPayload []byte) []peer.ID {
	elems := make([]profileElement, 0, len(ib.profiles))
	out := make([]peer.ID, 0, len(ib.profiles))
	for p, pro := range ib.profiles {
		elems = append(elems, profileElement{
			p:   p,
			pro: pro,
		})
	}
	sort.Slice(elems, func(i, j int) bool {
		return elems[i].pro.Less(elems[j].pro, reqPayload)
	})
	for i := range elems {
		out = append(out, elems[i].p)
	}
	return out
}

/*===========================================================================*/
// profiles
/*===========================================================================*/

// ProfileFactory generates a Profile
type ProfileFactory func() (Profile, error)

// Profile stores query profiles
type Profile interface {
	Insert(from peer.ID, reqPayload []byte) error
	//
	Less(that Profile, reqPayload []byte) bool
}

type IntBFSOptions struct {
}
