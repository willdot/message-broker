package messagebroker

// Message represents a message that can be published or consumed
type Message struct {
	Topic string `json:"topic"`
	Data  []byte `json:"data"`
}
