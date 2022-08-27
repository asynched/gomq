package main

import (
	"gomq/concurrency"
	"gomq/messages"
	"gomq/serializers"
	"gomq/stream"
	"log"
	"net"
	"strings"
)

/*
TODO:
	1. Fix synchronization issues with consumers;
	2. Review mutex usage;
	3. Fix CPU usage, something is very wrong there, lol.
*/

const SOCK_BUFFER_SIZE = 512

type GlobalRegistry struct {
	Producers map[string]stream.Producer
}

var globalRegistry concurrency.Mutex[GlobalRegistry]

func main() {
	SetupGlobalRegistry()
	SetupLogger()
	log.Fatal(SetupNetworkLayer())
}

func getKeys[K string, T any](m map[K]T) []K {
	keys := make([]K, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	return keys
}

func SetupGlobalRegistry() {
	globalRegistry = concurrency.NewMutex(&GlobalRegistry{
		Producers: make(map[string]stream.Producer),
	})
}

func SetupLogger() {
	log.SetFlags(log.LUTC)
	log.SetPrefix("[GOMQ] ")
}

func SetupNetworkLayer() error {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: 3333,
		IP:   net.IPv4(127, 0, 0, 1),
	})

	log.Println("GOMQ server has started.")

	if err != nil {
		return err
	}

	for {
		conn, err := listener.AcceptTCP()

		if err != nil {
			continue
		}

		log.Printf("New connection established, IP: %s.\n", conn.LocalAddr().String())

		go HandleConnection(conn)
	}
}

func HandleConnection(conn *net.TCPConn) {
	defer conn.Close()
	defer log.Printf("Connection with IP: %s has been closed.\n", conn.LocalAddr().String())

	registry := globalRegistry.Lock()

	log.Println("Registered producers:", getKeys(registry.Producers))

	globalRegistry.Unlock()

	data := make([]byte, SOCK_BUFFER_SIZE)
	_, err := conn.Read(data)

	log.Printf("Clients registration payload: %s.\n", string(data))

	if err != nil {
		return
	}

	message, err := serializers.FromJson[messages.Registration]([]byte(strings.Trim(string(data), "\x00")))

	if err != nil {
		log.Println("Couldn't parse registration message, disconnecting client.")
		log.Printf("Error: '%s'\n", err)
		return
	}

	switch message.Type {
	case "CONSUMER":
		log.Printf("Consumer with topic '%s' registered.", message.Topic)
		HandleConsumer(conn, message)
		log.Printf("Consumer with topic '%s' disconnected.", message.Topic)
		break
	case "PRODUCER":
		log.Printf("Producer with topic '%s' registered.", message.Topic)
		HandleProducer(conn, message)
		log.Printf("Producer with topic '%s' disconnected.", message.Topic)
		break
	default:
		break
	}
}

func HandleConsumer(conn *net.TCPConn, message messages.Registration) {
	registry := globalRegistry.Lock()

	producer, ok := registry.Producers[message.Topic]

	if !ok {
		log.Printf("Producer with the topic '%s' didn't exist, closing connection.", message.Topic)
		data, err := serializers.ToJson(messages.Payload{
			Error:   true,
			Payload: messages.ERR_PRODUCER_UNAVAILABLE,
		})

		if err == nil {
			conn.Write(data)
		}

		globalRegistry.Unlock()
		return
	}

	consumer := stream.NewConsumer()

	go consumer.Handle(func(message messages.Payload) {
		data, err := serializers.ToJson(message)

		if err != nil {
			return
		}

		conn.Write(data)
	})

	producer.Subscribe(consumer)
	globalRegistry.Unlock()

	for {
		_, err := conn.Read(nil)

		if err != nil {
			producer.Unsubscribe(consumer)
			break
		}
	}
}

func HandleProducer(conn *net.TCPConn, message messages.Registration) {
	registry := globalRegistry.Lock()

	_, ok := registry.Producers[message.Topic]

	if ok {
		log.Printf("Producer already registered, topic: %s\n", message.Topic)
		data, err := serializers.ToJson(messages.Payload{
			Error:   true,
			Payload: messages.ERR_PRODUCER_ALREADY_REGISTERED,
		})

		if err == nil {
			conn.Write(data)
		}

		globalRegistry.Unlock()
		return
	}

	producer := stream.NewProducer()
	registry.Producers[message.Topic] = producer
	globalRegistry.Unlock()

	for {
		data := make([]byte, SOCK_BUFFER_SIZE)
		_, err := conn.Read(data)

		if err != nil {
			producer.Disconnect()

			registry := globalRegistry.Lock()
			delete(registry.Producers, message.Topic)
			globalRegistry.Unlock()

			return
		}

		log.Printf("Received '%s' from client\n", string(data))
		log.Printf("Streaming message to %d consumers.\n", len(producer.Consumers))
		producer.Push(messages.Payload{
			Error:   false,
			Payload: string(data),
		})
	}
}
