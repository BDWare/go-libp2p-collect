package apsub

import (
	"context"
	"sync"

	pubsub "bdware.org/libp2p/go-libp2p-pubsub"
	host "github.com/libp2p/go-libp2p-core/host"
)

// TopicHandle is the handle function of subscription.
// WARNING: DO NOT change msg, if a msg includes multiple topics, they share a message.
type TopicHandle func(topic string, msg *pubsub.Message)

// Message is type alias
type Message = pubsub.Message

// AsyncPubSub encapsulates pubsub, provides async methods to get subscribe messages.
// AsyncPubSub also manages the joined topics
type AsyncPubSub struct {
	lock      sync.RWMutex
	items     map[string]topicitem
	pubs      *pubsub.PubSub
	host      host.Host
	selfNotif bool
}

type topicitem struct {
	name  string
	topic *pubsub.Topic
	// ctxcancel is called to cancel context
	ctxcancel func()
	// subcancel is called to cancel subscription
	subcancel func()
}

// TopicOpt is the option in NewTopics
type TopicOpt func(*AsyncPubSub) error

// PubSubFactory initializes a pubsub in Topics
type PubSubFactory func(h host.Host) (*pubsub.PubSub, error)

// WithCustomPubSubFactory initials the pubsub based on given factory
func WithCustomPubSubFactory(psubFact PubSubFactory) TopicOpt {
	return func(t *AsyncPubSub) (err error) {
		psub, err := psubFact(t.host)
		if err == nil {
			t.pubs = psub
		}
		return
	}
}

// WithSelfNotif decides whether a node receives self-published messages.
// If WithSelfNotif is set to true, the node will receive messages published by itself.
// Default is set to false.
func WithSelfNotif(enable bool) TopicOpt {
	return func(t *AsyncPubSub) (err error) {
		t.selfNotif = enable
		return
	}
}

// NewAsyncPubSub returns a new Topics instance.
// If WithCustomPubSubFactory is not set, a default randomsub will be used.
func NewAsyncPubSub(h host.Host, opts ...TopicOpt) (apsub *AsyncPubSub, err error) {
	t := &AsyncPubSub{
		lock:      sync.RWMutex{},
		items:     make(map[string]topicitem),
		host:      h,
		selfNotif: false,
	}
	for _, opt := range opts {
		if err == nil {
			err = opt(t)
		}
	}

	// if pubs is not set, use the default initialization
	if err == nil && t.pubs == nil {
		t.pubs, err = pubsub.NewRandomSub(context.Background(), h)
	}

	if err == nil {
		apsub = t
	}
	return
}

// Close the topics
func (ap *AsyncPubSub) Close() error {
	defer ap.lock.Unlock()
	/*_*/ ap.lock.Lock()

	for k := range ap.items {
		ap.unsubscribe(k)
	}
	return nil
}

// Publish a message with given topic
func (ap *AsyncPubSub) Publish(ctx context.Context, topic string, data []byte) (err error) {
	var ok bool
	if err == nil {
		ok, err = ap.fastPublish(ctx, topic, data)
	}
	if err == nil && !ok {
		err = ap.slowPublish(ctx, topic, data)
	}
	return
}

func (ap *AsyncPubSub) fastPublish(ctx context.Context, topic string, data []byte) (ok bool, err error) {
	defer ap.lock.RUnlock()
	/*_*/ ap.lock.RLock()

	var it topicitem
	it, ok = ap.items[topic]
	if ok {
		err = it.topic.Publish(ctx, data)
	}
	return
}

func (ap *AsyncPubSub) slowPublish(ctx context.Context, topic string, data []byte) (err error) {
	defer ap.lock.Unlock()
	/*_*/ ap.lock.Lock()

	var ok bool
	var it topicitem
	if err == nil {
		it, ok = ap.items[topic]
	}

	if err == nil && !ok {
		it.topic, err = ap.pubs.Join(topic)
		if err == nil {
			it.name = topic
			ap.items[topic] = it
		}
	}

	if err == nil {
		err = it.topic.Publish(ctx, data)
	}

	return
}

// Subscribe a topic
// Subscribe a same topic is ok.
func (ap *AsyncPubSub) Subscribe(topic string, handle TopicHandle) (err error) {
	defer ap.lock.Unlock()
	/*_*/ ap.lock.Lock()

	it, ok := ap.items[topic]
	if ok {
		// Close topic doesn't work here, use subscription cancel instead.
		if it.subcancel != nil {
			it.subcancel()
			it.subcancel = nil
		}
		if it.ctxcancel != nil {
			it.ctxcancel()
			it.ctxcancel = nil
		}
		it.topic.Close()
		it.topic = nil

	}

	if err == nil {
		// Join should be called only once
		it.topic, err = ap.pubs.Join(topic)
	}

	var sub *pubsub.Subscription
	if err == nil {
		sub, err = it.topic.Subscribe()
		it.subcancel = sub.Cancel
	}

	if err == nil {
		ctx := context.TODO()
		ctx, it.ctxcancel = context.WithCancel(ctx)
		it.name = topic

		ap.items[topic] = it
		go ap.forwardTopic(ctx, sub, topic, handle)
	}

	return
}

// Unsubscribe the given topic
func (ap *AsyncPubSub) Unsubscribe(topic string) (err error) {
	defer ap.lock.Unlock()
	/*_*/ ap.lock.Lock()
	return ap.unsubscribe(topic)
}

func (ap *AsyncPubSub) unsubscribe(topic string) (err error) {
	it, ok := ap.items[topic]
	if ok {
		if it.subcancel != nil {
			it.subcancel()
		}
		if it.ctxcancel != nil {
			it.ctxcancel()
		}
		// be careful here.
		// topic.Close() will return an error if there are active subscription.
		if it.topic != nil {
			err = it.topic.Close()
		}

		delete(ap.items, topic)
	}
	return
}

func (ap *AsyncPubSub) forwardTopic(ctx context.Context, sub *pubsub.Subscription, topic string, f TopicHandle) {
	for {
		msg, err := sub.Next(ctx)
		if err == nil {
			err = ctx.Err()
		}

		if err == nil {
			if msg.ReceivedFrom != ap.host.ID() || ap.selfNotif {
				go f(topic, msg)
			}
		} else {
			break
		}
	}
}
