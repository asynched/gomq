package main

import (
	"fmt"
	"gomq/ack"
	"gomq/concurrency"
	"gomq/serializers"
	"gomq/stream"
	"net"
	"time"
)

const SOCK_BUFF_SIZE = 512

type GlobalRegistry struct {
	Producers map[string]stream.Producer
}

var globalRegistry concurrency.Mutex[GlobalRegistry]

func main() {
	producer := stream.NewProducer()
	consumer := stream.NewConsumer()

	go consumer.Handle(func(message ack.Payload) {
		fmt.Println("Data:", message)
	})

	producer.Subscribe(consumer)

	for counter := 0; counter < 512; counter++ {
		producer.Push(ack.Payload{
			Error:   false,
			Payload: "Hello, world!",
		})
		time.Sleep(time.Millisecond * 250)
	}
}

func SetupNetworkLayer() error {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: 3333,
		IP:   []byte("127.0.0.1"),
	})

	if err != nil {
		return err
	}

	for {
		connection, err := listener.AcceptTCP()

		if err != nil {
			continue
		}

		go HandleConnection(connection)
	}
}

func HandleConnection(connection *net.TCPConn) {
	data := make([]byte, SOCK_BUFF_SIZE)
	_, err := connection.Read(data)

	if err != nil {
		return
	}

	message, err := serializers.FromJson[ack.Registration](data)

	if err != nil {
		return
	}

	switch message.Type {
	case "CONSUMER":
		HandleConsumer(connection, message)
	case "PRODUCER":
		HandleProducer(connection, message)
	}
}

func HandleConsumer(conn *net.TCPConn, message ack.Registration) {
	registry := globalRegistry.Lock()

	producer, ok := registry.Producers[message.Topic]
	globalRegistry.Unlock()

	if !ok {
		data, err := serializers.ToJson(ack.Payload{
			Error:   true,
			Payload: ack.ERR_ALREADY_REGISTERED,
		})

		if err != nil {
			return
		}

		conn.Write(data)
		conn.Close()
		return
	}

	for {
		data := make([]byte, SOCK_BUFF_SIZE)
		_, err := conn.Read(data)

		if err != nil {
			break
		}

		payload, err := serializers.FromJson[ack.Payload](data)

		if err != nil {
			break
		}

		producer.Push(payload)
	}
}

func HandleProducer(conn *net.TCPConn, message ack.Registration) {

}
