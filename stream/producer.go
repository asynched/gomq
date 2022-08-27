package stream

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

func (p *Producer) Push(data []byte) {
	for _, consumer := range p.Consumers {
		consumer.Channel <- data
	}
}

func (p *Producer) Disconnect() {
	for _, consumer := range p.Consumers {
		close(consumer.Channel)
	}
}
