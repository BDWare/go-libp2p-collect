package collect

// PubSubCollector is a group communication module on topic-based overlay network.
// It helps to dispatch request, and wait for corresponding responses.
// In relay mode, PubSubCollector can also help to reduce the response.
type PubSubCollector interface {

	// Join the overlay network defined by topic.
	// Register RequestHandle and ResponseHandle in opts.
	Join(topic string, opts ...JoinOpt) error

	// Publish a serialized request. Request should be encasulated in data argument.
	Publish(topic string, data []byte, opts ...PubOpt) error

	// Leave the overlay
	Leave(topic string) error
}
