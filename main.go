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

type GlobalRegistry struct {
	Producers map[string]stream.Producer
}

var registry concurrency.Mutex[GlobalRegistry]

func main() {
	producer := stream.NewProducer()
	consumer := stream.NewConsumer()

	go consumer.Handle(func(data []byte) {
		fmt.Println("Data:", string(data))
	})

	producer.Subscribe(consumer)

	for counter := 0; counter < 512; counter++ {
		producer.Push([]byte("Hello, world!"))
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
	data := make([]byte, 512)
	_, err := connection.Read(data)

	if err != nil {
		return
	}

	message, err := serializers.FromJson[ack.RegistrationMessage](data)

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

func HandleConsumer(conn *net.TCPConn, message ack.RegistrationMessage) {

}

func HandleProducer(conn *net.TCPConn, message ack.RegistrationMessage) {

}
