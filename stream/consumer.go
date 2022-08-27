package stream

type Consumer struct {
	Channel chan []byte
}

func NewConsumer() Consumer {
	return Consumer{
		Channel: make(chan []byte),
	}
}

func (c *Consumer) Handle(handler func([]byte)) {
	for data := range c.Channel {
		handler(data)
	}
}
