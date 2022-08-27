package ack

type RegistrationMessage struct {
	Type  string `json:"type"`
	Topic string `json:"topic"`
}

type PayloadMessage struct {
	Error   bool   `json:"error"`
	Payload string `json:"payload"`
}
