package ack

const (
	ERR_ALREADY_REGISTERED   string = "ERR_ALREADY_REGISTERED"
	ERR_PRODUCER_UNAVAILABLE string = "ERR_PRODUCER_UNAVAILABLE"
)

type Registration struct {
	Type  string `json:"type"`
	Topic string `json:"topic"`
}

type Payload struct {
	Error   bool   `json:"error"`
	Payload string `json:"payload"`
}
