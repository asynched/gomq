package stream

import "gomq/ack"

type Consumer struct {
	Channel chan ack.Payload
}

func NewConsumer() Consumer {
	return Consumer{
		Channel: make(chan ack.Payload),
	}
}

func (c *Consumer) Handle(handler func(ack.Payload)) {
	for data := range c.Channel {
		handler(data)
	}
}
