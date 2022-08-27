package stream

import (
	"gomq/messages"
)

type Consumer struct {
	Channel chan messages.Payload
}

func NewConsumer() Consumer {
	return Consumer{
		Channel: make(chan messages.Payload),
	}
}

func (c *Consumer) Handle(handler func(messages.Payload)) {
	go func() {
		for data := range c.Channel {
			handler(data)
		}
	}()
}
