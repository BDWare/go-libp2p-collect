package apsub

import (
	"context"
	"sync"

	host "github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p-core/peer"
)

// TopicHandle is the handle function of subscription.
type TopicHandle func(topic string, data []byte)

// Topics encapsulates pubsub, provides async methods to get subscribe messages.
// Topics also manages the joined topics
type Topics struct {
	lock      sync.RWMutex
	item      map[string]topicitem
	pubs      *pubsub.PubSub
	host      host.Host
	self 		peer.ID
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
type TopicOpt func(*Topics) error

// PubSubFactory initializes a pubsub in Topics
type PubSubFactory func(h host.Host) (*pubsub.PubSub, error)

// WithCustomPubSubFactory initials the pubsub based on given factory
func WithCustomPubSubFactory(psubFact PubSubFactory) TopicOpt {
	return func(t *Topics) (err error) {
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
	return func(t *Topics) (err error) {
		t.selfNotif = enable
		return
	}
}

// NewTopics returns a new Topics instance.
// If WithCustomPubSubFactory is not set, a default randomsub will be used.
func NewTopics(h host.Host, opts ...TopicOpt) (top *Topics, err error) {
	t := &Topics{
		lock:      sync.RWMutex{},
		item:      make(map[string]topicitem),
		host:      h,
		selfNotif: false,
	}
	for _, opt := range opts {
		if err == nil {
			err = opt(t)
		}
	}

	// if self is not set, use host.ID()
	if err == nil && t.self == "" {
		t.self = t.host.ID()
	}

	// if pubs is not set, use the default initialization
	if err == nil && t.pubs == nil {
		t.pubs, err = pubsub.NewRandomSub(context.Background(), h)
	}

	if err == nil {
		top = t
	}
	return
}

// Close the topics
func (t *Topics) Close() error {
	defer t.lock.Unlock()
	/*_*/ t.lock.Lock()

	for k := range t.item {
		t.unsubscribe(k)
	}
	return nil
}

// Publish a message with given topic
func (t *Topics) Publish(ctx context.Context, topic string, data []byte) (err error) {
	var ok bool
	if err == nil {
		ok, err = t.fastPublish(ctx, topic, data)
	}
	if err == nil && !ok {
		err = t.slowPublish(ctx, topic, data)
	}
	return
}

func (t *Topics) fastPublish(ctx context.Context, topic string, data []byte) (ok bool, err error) {
	defer t.lock.RUnlock()
	/*_*/ t.lock.RLock()

	var it topicitem
	it, ok = t.item[topic]
	if ok {
		err = it.topic.Publish(ctx, data)
	}
	return
}

func (t *Topics) slowPublish(ctx context.Context, topic string, data []byte) (err error) {
	defer t.lock.Unlock()
	/*_*/ t.lock.Lock()

	var ok bool
	var it topicitem
	if err == nil {
		it, ok = t.item[topic]
	}

	if err == nil && !ok {
		it.topic, err = t.pubs.Join(topic)
		if err == nil {
			it.name = topic
			t.item[topic] = it
		}
	}

	if err == nil {
		err = it.topic.Publish(ctx, data)
	}

	return
}

// Subscribe a topic
// Subscribe a same topic is ok.
func (t *Topics) Subscribe(topic string, handle TopicHandle) (err error) {
	defer t.lock.Unlock()
	/*_*/ t.lock.Lock()

	it, ok := t.item[topic]
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
		it.topic, err = t.pubs.Join(topic)
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

		t.item[topic] = it
		go t.forwardTopic(ctx, sub, topic, handle)
	}

	return
}

// Unsubscribe the given topic
func (t *Topics) Unsubscribe(topic string) (err error) {
	defer t.lock.Unlock()
	/*_*/ t.lock.Lock()
	return t.unsubscribe(topic)
}

func (t *Topics) unsubscribe(topic string) (err error) {
	it, ok := t.item[topic]
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

		delete(t.item, topic)
	}
	return
}

func (t *Topics) forwardTopic(ctx context.Context, sub *pubsub.Subscription, topic string, f TopicHandle) {
	for {
		msg, err := sub.Next(ctx)
		if err == nil {
			err = ctx.Err()
		}

		if err == nil {
			if msg.ReceivedFrom != t.self || t.selfNotif {
				go f(topic, msg.Data)
			}
		} else {
			break
		}
	}
}
