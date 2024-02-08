package internal

// Message represents a message that can be sent / received
type Message struct {
	Data          []byte
	DeliveryCount int
}

// NewMessage intializes a new message
func NewMessage(data []byte) Message {
	return Message{Data: data, DeliveryCount: 1}
}
