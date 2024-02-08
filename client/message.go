package client

// Message represents a message that can be published or consumed
type Message struct {
	Topic string `json:"topic"`
	Data  []byte `json:"data"`

	ack chan bool
}

// NewMessage creates a new message
func NewMessage(topic string, data []byte) *Message {
	return &Message{
		Topic: topic,
		Data:  data,
		ack:   make(chan bool),
	}
}

// Ack will send the provided value of the ack to the server
func (m *Message) Ack(ack bool) {
	m.ack <- ack
}
