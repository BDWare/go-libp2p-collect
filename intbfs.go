package collect

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bdware/go-libp2p-collect/pb"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const (
	IntBFSProtocolID = protocol.ID("/intbfs/1.0.0")
)

// IntBFSCollector .
// We used a topic-defined overlay here.
type IntBFSCollector struct {
	rw     sync.RWMutex
	conf   *Conf
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
	wires := initOpts.Wires
	if wires == nil {
		tw, err := newTopicWiresAdapter(h)
		if err != nil {
			return nil, err
		}
		wires = tw
	}

	ic := &IntBFSCollector{
		rw:     sync.RWMutex{},
		conf:   &initOpts.Conf,
		topics: make(map[string]*IntBFS),
		hub:    NewTopicHub(wires),
		log:    initOpts.Logger,
	}
	return ic, nil
}

func (ic *IntBFSCollector) Join(topic string, opts ...JoinOpt) (err error) {

	jo, err := NewJoinOptions(opts)
	if err != nil {
		return err
	}

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
		&IntBFSOptions{
			Fanout:         ic.conf.Fanout,
			RandomFanout:   ic.conf.RandomFanout,
			MaxHitsToSend:  ic.conf.MaxHitsToSend,
			ProfileFactory: jo.ProfileFactory,
			RequestHandler: jo.RequestHandler,
			ReqIDFn:        DefaultReqIDFn,
			Logger:         ic.log,
			Topic:          topic,
		},
	)
	if err != nil {
		return err
	}

	wires.SetListener(intbfs)
	wires.SetMsgHandler(intbfs.handleMsgData)

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

const (
	incomingBufferSize = 10240 // incoming message buffer size
	outgoingBufferSize = 1024  // outgoing message buffer size
	publishBufferSize  = 1024  // publish request buffer size
	cacheSize          = 1024  // request cache size
	resultBufferSize   = 1024  // request handle result buffer size
)

type Msg struct {
	peer peer.ID
	*pb.Msg
}

type PubReq struct {
	req   *Request
	po    *PubOpts
	errCh chan error
}

type RequestResult struct {
	req *Request
	itm *Intermediate
}

// IntBFS don't care about topic.
// IntBFS contains 5 parts:
// 1. query machanism
// 2. peer profiles
// 3. peer ranking
// 4. distance function
// 5. random perturbation
type IntBFS struct {
	rw       sync.RWMutex
	wires    Wires
	profiles map[peer.ID]Profile
	log      Logger
	topic    string
	profact  ProfileFactory
	reqidfn  ReqIDFn
	reqhndl  RequestHandler
	seqno    uint64
	cache    *requestWorkerPool
	opts     *IntBFSOptions
	//
	incomingCh      chan *Msg
	outgoingCh      chan *Msg
	peerUpCh        chan peer.ID
	peerDownCh      chan peer.ID
	publishCh       chan *PubReq
	requestResultCh chan *RequestResult
	closeCh         chan struct{}
}

func NewIntBFS(wires Wires, opts *IntBFSOptions) (*IntBFS, error) {
	if err := checkIntBFSOpitons(opts); err != nil {
		return nil, err
	}
	cache, err := newRequestWorkerPool(cacheSize)
	if err != nil {
		return nil, err
	}
	out := &IntBFS{
		rw:       sync.RWMutex{},
		wires:    wires,
		profiles: make(map[peer.ID]Profile),
		log:      opts.Logger,
		topic:    opts.Topic,
		profact:  opts.ProfileFactory,
		reqidfn:  opts.ReqIDFn,
		reqhndl:  opts.RequestHandler,
		seqno:    rand.Uint64(),
		cache:    cache,
		opts:     opts,

		incomingCh:      make(chan *Msg, incomingBufferSize),
		outgoingCh:      make(chan *Msg, outgoingBufferSize),
		peerUpCh:        make(chan peer.ID, 1),
		peerDownCh:      make(chan peer.ID, 1),
		publishCh:       make(chan *PubReq, publishBufferSize),
		requestResultCh: make(chan *RequestResult, resultBufferSize),
		closeCh:         make(chan struct{}),
	}
	wires.SetListener(out)
	wires.SetMsgHandler(out.handleMsgData)
	go out.loop(context.TODO())
	return out, nil
}

func (ib *IntBFS) Publish(data []byte, opts ...PubOpt) error {

	po, err := NewPublishOptions(opts)
	if err != nil {
		return err
	}
	// assemble the request
	myself := ib.wires.ID()
	req := &Request{
		Control: pb.RequestControl{
			Requester: myself,
			Sender:    myself,
			Seqno:     atomic.AddUint64(&(ib.seqno), 1),
			Topic:     ib.topic,
		},
		Payload: data,
	}

	pubReq := &PubReq{
		req:   req,
		po:    po,
		errCh: make(chan error, 1),
	}
	ib.log.Logf("debug", "begin send pubreq: req.seqno=%d", req.Control.Seqno)
	ib.publishCh <- pubReq
	ib.log.Logf("debug", "wait for err: req.seqno=%d", req.Control.Seqno)
	err = <-pubReq.errCh
	ib.log.Logf("debug", "err return: req.seqno=%d", req.Control.Seqno)
	return err

	// return ib.handleIncomingRequest(ib.wires.ID(), req, po.FinalRespHandle)
}

func (ib *IntBFS) Close() error {
	close(ib.closeCh)
	return nil
}

/*===========================================================================*/

func (ib *IntBFS) loop(ctx context.Context) {
	for {
		select {
		case <-ib.closeCh:
		case <-ctx.Done():
			return
		case msg := <-ib.incomingCh:
			ib.handleIncomingMsg(msg.peer, msg.Msg)
		case peer := <-ib.peerUpCh:
			ib.handlePeerUp(peer)
		case peer := <-ib.peerDownCh:
			ib.handlePeerDown(peer)
		case pub := <-ib.publishCh:
			ib.handlePublish(pub)
		case res := <-ib.requestResultCh:
			ib.handleRequestResult(res)
		}
	}
}

func (ib *IntBFS) handlePeerDown(p peer.ID) {
	delete(ib.profiles, p)
}

func (ib *IntBFS) handlePeerUp(p peer.ID) {
	if _, ok := ib.profiles[p]; ok {
		ib.log.Logf("warn", "HandlePeerUp: %s profile exists", p.ShortString())
		return
	}
	ib.profiles[p] = ib.profact()
}

func (ib *IntBFS) handleIncomingMsg(from peer.ID, msg *pb.Msg) {
	// dispatch msg type
	switch msg.Type {
	case pb.Msg_Request:
		ib.handleIncomingRequest(from, msg.Request)
	case pb.Msg_Response:
		ib.handleIncomingResponse(from, msg.Response)
	case pb.Msg_Hit:
		ib.handleIncomingHit(from, msg.Request, msg.Response)
	case pb.Msg_Unknown:
		fallthrough
	default:
		ib.log.Logf("info", "unknown msg type")
		return
	}
}

func (ib *IntBFS) handlePublish(pub *PubReq) {
	ib.log.Logf("info", "handlePublish: req.seqno=%d", pub.req.Control.Seqno)
	pub.errCh <- ib.handleRequest(
		pub.po.RequestContext,
		ib.wires.ID(),
		pub.req,
		pub.po.FinalRespHandle,
	)
}

func (ib *IntBFS) handleRequestResult(res *RequestResult) {
	ib.log.Logf("info", "handleRequestResult")
	req := res.req
	from := req.Control.Sender
	m := res.itm
	// check hit
	if m != nil && m.Hit {
		err := ib.handleHit(from, req, m)
		if err != nil {
			ib.log.Logf("error", "handleRequestResult: err=%v", err)
			return
		}
	}

}

/*===========================================================================*/

func (ib *IntBFS) handleMsgData(from peer.ID, data []byte) {
	// decode msg
	m := &pb.Msg{}
	err := m.Unmarshal(data)
	if err != nil {
		ib.log.Logf("info", "msg unmarshal error:%v", err)
		return
	}
	incoming := &Msg{
		peer: from,
		Msg:  m,
	}
	select {
	case ib.incomingCh <- incoming:
	default:
		ib.log.Logf("warn", "incoming message full, dropping msg from %s", from.ShortString())
	}

}

func (ib *IntBFS) HandlePeerDown(p peer.ID) {
	ib.peerDownCh <- p
}

func (ib *IntBFS) HandlePeerUp(p peer.ID) {
	ib.peerUpCh <- p
}

func (ib *IntBFS) handleRequest(ctx context.Context, from peer.ID, req *Request, finalHandler FinalRespHandler) error {
	ib.log.Logf("debug", "begin get req id")
	reqID := ib.reqidfn(req)
	if _, ok, _ := ib.cache.GetReqItem(reqID); ok {
		// msg has seen
		ib.log.Logf("info", "handleRequest: have seen reqid=%s", reqID)
		return nil
	}
	ib.log.Logf("debug", "begin add req item")
	// insert request in cache
	ib.cache.AddReqItem(ctx, reqID, &reqItem{
		finalHandler: finalHandler,
		topic:        ib.topic,
		req:          req,
	})
	ib.log.Logf("debug", "add req item done")
	// call request handler
	go ib.handleRequestAsync(ctx, req)

	err := ib.handleForward(from, req)
	if err != nil {
		ib.log.Logf("error", "handleRequestResult: handleForword: err=%v", err)
		return err
	}

	return nil
}

func (ib *IntBFS) handleRequestAsync(ctx context.Context, req *Request) {
	itm := ib.reqhndl(ctx, req)
	res := &RequestResult{
		req: req,
		itm: itm,
	}
	select {
	case ib.requestResultCh <- res:
	case <-ib.closeCh:
	}

}

func (ib *IntBFS) handleForward(from peer.ID, req *Request) error {

	// find k highest peerProfile peers
	// then find r random peers not within the previous k-set
	k := ib.opts.Fanout
	r := ib.opts.RandomFanout
	peers := ib.ranks(from, req)
	bound := k + r
	if len(peers) <= bound {
		bound = len(peers)
	}
	for i := k; i < bound; i++ {
		// swap peers between [k, len(peers))
		j := k + rand.Intn(len(peers)-k)
		peers[i], peers[j] = peers[j], peers[i]
	}
	tosend := peers[:bound]
	// tosend is peers[0:k] + peers[k:bound]
	ib.log.Logf("debug", "handleForward: tosend=%v", tosend)
	for _, to := range tosend {
		if to == ib.wires.ID() {
			ib.log.Logf("info", "handleForward:cannot send to myself")
			continue
		}
		if to == req.Control.Requester {
			ib.log.Logf("info", "handleForward:don't send to requester")
			continue
		}
		if to == req.Control.Sender {
			ib.log.Logf("info", "handleForward:don't send to sender")
			continue
		}

		go func(to peer.ID, req *Request) {
			if err := ib.sendRequest(to, req); err != nil {
				ib.log.Logf("error", "handleForward: %v", err)
			}
		}(to, req)
	}
	return nil
}

func (ib *IntBFS) handleHit(from peer.ID, req *Request, intm *Intermediate) error {
	reqID := ib.reqidfn(req)
	// assemble resp
	resp := &Response{
		Control: pb.ResponseControl{
			RequestId: reqID,
			Requester: req.Control.Requester,
			Responser: ib.wires.ID(),
			Sender:    ib.wires.ID(),
			Topic:     ib.topic,
		},
		Payload: intm.Payload,
		Error:   nil,
	}

	// no need to insert profile
	// send control message to neighbors, tell them that you're hit.
	neighs := ib.wires.Neighbors()
	if len(neighs) > ib.opts.MaxHitsToSend {
		rand.Shuffle(len(neighs), func(i, j int) {
			neighs[i], neighs[j] = neighs[j], neighs[i]
		})
		neighs = neighs[:ib.opts.MaxHitsToSend]
	}
	for _, peer := range neighs {
		go ib.sendHit(peer, req, resp)
	}

	ib.log.Logf("info", "%s: handleHit:from=%s", ib.wires.ID().ShortString(), from.ShortString())

	var err error
	if from == ib.wires.ID() {
		err = ib.handleLocalResponse(resp)
	} else {
		// send back response
		err = ib.sendResponse(from, resp)
	}
	return err

}

func (ib *IntBFS) handleIncomingRequest(from peer.ID, req *Request) (err error) {
	ib.log.Logf("debug", "handleIncomingRequest: from=%s, req=%+v", from, req)
	if req.Control.Requester == ib.wires.ID() {
		// receives self published request, drop it
		ib.log.Logf("info", "handleIncomingRequest: receive self published request, dropping")
		return fmt.Errorf("self published request")
	}
	return ib.handleRequest(context.TODO(), from, req, nil)
}

func (ib *IntBFS) handleLocalResponse(resp *Response) (err error) {
	item, ok, _ := ib.cache.GetReqItem(resp.Control.RequestId)
	if !ok {
		ib.log.Logf("info", "handleLocalResponse: cannot find reqItem for reqid=%s", resp.Control.RequestId)
		return nil
	}
	if item.finalHandler == nil {
		ib.log.Logf("error", "handleLocalResponse: nil finalHandler, reqid=%s",
			resp.Control.RequestId,
		)
		return fmt.Errorf("nil finalHandler")
	}
	ib.log.Logf("debug", "begin final handle reqid=%s", resp.Control.RequestId)
	go item.finalHandler(context.TODO(), resp)
	return nil
}

func (ib *IntBFS) handleIncomingResponse(from peer.ID, resp *Response) (err error) {
	item, ok, _ := ib.cache.GetReqItem(resp.Control.RequestId)
	if !ok {
		ib.log.Logf("info", "handleIncomingResponse: cannot find reqItem for reqid=%s", resp.Control.RequestId)
		// it is ok when we received a response without related reqItem.
		// This is because the request is cancelled before we receive the response.
		// TODO: update profiles without reqItem
		return nil
	}

	// store query message according to cached content.
	pro, ok := ib.profiles[from]
	if !ok {
		ib.log.Logf("warn", "cannot find profile for peer %s", from.ShortString())
		return fmt.Errorf("cannot find profile for peer %s", from.ShortString())
	}
	pro.Insert(item.req, resp)

	// if it is rooted node, reply to user.
	if item.req.Control.Requester == ib.wires.ID() {
		if item.finalHandler == nil {
			ib.log.Logf("error", "handleIncomingResponse: nil finalHandler, from=%s, reqid=%s",
				from.ShortString(),
				resp.Control.RequestId,
			)
			return fmt.Errorf("nil finalHandler")
		}
		ib.log.Logf("debug", "begin final handle reqid=%s", resp.Control.RequestId)
		go item.finalHandler(context.TODO(), resp)
		return nil
	}
	// reply to father node.
	resp.Control.Sender = ib.wires.ID()
	err = ib.sendResponse(item.req.Control.Sender, resp)
	if err != nil {
		return err
	}
	return nil
}

func (ib *IntBFS) handleIncomingHit(from peer.ID, req *Request, resp *Response) (err error) {
	// store query message according to cached content.
	pro, ok := ib.profiles[from]
	if !ok {
		// It is possible that from is not in profiles, which will happen when hit reached faster than request.
		ib.log.Logf("info", "handleIncomingHit: cannot find profile for peer %s", from.ShortString())
		pro = ib.profact()
		ib.profiles[from] = pro
	}
	pro.Insert(req, resp)
	return nil
}

/*===========================================================================*/
// helper
/*===========================================================================*/

func (ib *IntBFS) sendRequest(to peer.ID, req *Request) error {
	return ib.sendMsg(to, pb.Msg_Request, req, nil)
}

func (ib *IntBFS) sendResponse(to peer.ID, resp *Response) error {
	return ib.sendMsg(to, pb.Msg_Response, nil, resp)
}

func (ib *IntBFS) sendHit(to peer.ID, req *Request, resp *Response) error {
	return ib.sendMsg(to, pb.Msg_Hit, req, resp)
}

func (ib *IntBFS) sendMsg(to peer.ID, mtype pb.Msg_MsgType, req *Request, resp *Response) error {

	msg := &pb.Msg{
		Type:     mtype,
		Request:  req,
		Response: resp,
	}

	if ib.wires.ID() == to {
		// local msg
		localIncoming := &Msg{
			peer: ib.wires.ID(),
			Msg:  msg,
		}
		select {
		case ib.incomingCh <- localIncoming:
		default:
			ib.log.Logf("warn", "incoming message full, dropping msg from %s", to.ShortString())
		}

		return nil
	}

	data, err := msg.Marshal()
	if err != nil {
		ib.log.Logf("error", "response marshal error:%v", err)
		return err
	}
	return ib.wires.SendMsg(to, data)
}

/*===========================================================================*/
// profiles
/*===========================================================================*/

type profileElement struct {
	p   peer.ID
	pro Profile
}

type profileElements []profileElement

func (p profileElements) String() string {
	var buf strings.Builder
	buf.WriteString("[")
	for i := range p {
		buf.WriteString(fmt.Sprintf("{peers:%s, profile:%v},", p[i].p.ShortString(), p[i].pro))
	}
	buf.WriteString("]")
	return buf.String()
}

func (ib *IntBFS) ranks(from peer.ID, req *Request) []peer.ID {
	elems := make([]profileElement, 0, len(ib.profiles))
	out := make([]peer.ID, 0, len(ib.profiles))
	for p, pro := range ib.profiles {
		if p == from {
			// don't send back the request to the father node
			continue
		}
		elems = append(elems, profileElement{
			p:   p,
			pro: pro,
		})
	}
	sort.Slice(elems, func(i, j int) bool {
		return elems[i].pro.Less(elems[j].pro, req)
	})
	for i := range elems {
		out = append(out, elems[i].p)
	}
	return out
}

type defaultProfile struct{}

func (d *defaultProfile) Insert(req *Request, resp *Response) {
}
func (d *defaultProfile) Less(that Profile, req *Request) bool {
	// random true or false
	return (rand.Int() & 1) == 1
}

/*===========================================================================*/
// options
/*===========================================================================*/

// IntBFSOptions .
type IntBFSOptions struct {
	Fanout        int
	RandomFanout  int
	MaxHitsToSend int
	ProfileFactory
	RequestHandler
	ReqIDFn
	Logger
	Topic string
}

var defaultProfileFactory = func() Profile { return &defaultProfile{} }

// MakeDefaultIntBFSOptions .
func MakeDefaultIntBFSOptions() *IntBFSOptions {
	c := MakeDefaultConf()
	return &IntBFSOptions{
		Fanout:         c.Fanout,
		RandomFanout:   c.RandomFanout,
		MaxHitsToSend:  c.MaxHitsToSend,
		ProfileFactory: defaultProfileFactory,
		RequestHandler: defaultRequestHandler,
		ReqIDFn:        DefaultReqIDFn,
		Logger:         MakeDefaultLogger(),
		Topic:          "",
	}
}

func checkIntBFSOpitons(opts *IntBFSOptions) error {
	if opts == nil {
		opts = MakeDefaultIntBFSOptions()
	}
	if opts.Fanout < 0 {
		return fmt.Errorf("fanout is less than 0")
	}
	if opts.ProfileFactory == nil {
		return fmt.Errorf("nil profile factory")
	}
	if opts.RequestHandler == nil {
		return fmt.Errorf("nil request handler")
	}
	if opts.ReqIDFn == nil {
		return fmt.Errorf("nil ReqIDFn")
	}
	if opts.Logger == nil {
		return fmt.Errorf("nil Logger")
	}
	return nil
}
