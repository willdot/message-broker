package messagebroker

type Message struct {
	Topic string `json:"topic"`
	Data  []byte `json:"data"`
}
