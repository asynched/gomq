package stream

import (
	"gomq/messages"
)

type Producer struct {
	Consumers []Consumer
}

func NewProducer() Producer {
	return Producer{
		Consumers: make([]Consumer, 0),
	}
}

func (p *Producer) Subscribe(consumer Consumer) {
	p.Consumers = append(p.Consumers, consumer)
}

func (p *Producer) Unsubscribe(consumer Consumer) {
	idx := -1

	for i, c := range p.Consumers {
		if consumer == c {
			idx = i
			break
		}
	}

	if idx == -1 {
		return
	}

	p.Consumers = append(p.Consumers[:idx], p.Consumers[idx+1:]...)
}

func (p *Producer) Push(data messages.Payload) {
	for _, consumer := range p.Consumers {
		consumer.Channel <- data
	}
}

func (p *Producer) Disconnect() {
	for _, consumer := range p.Consumers {
		close(consumer.Channel)
	}
}
