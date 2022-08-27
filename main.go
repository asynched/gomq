package main

import (
	"bytes"
	"gomq/concurrency"
	"gomq/messages"
	"gomq/serializers"
	"gomq/stream"
	"io"
	"log"
	"net"
	"os"
)

/*
TODO:
	1. Fix synchronization issues with consumers;
	2. Review mutex usage;
	3. Fix CPU usage, something is very wrong there, lol.
*/

const SOCK_BUFFER_SIZE = 512

type GlobalRegistry struct {
	Producers map[string]*stream.Producer
}

var globalRegistry concurrency.Mutex[GlobalRegistry]

func main() {
	SetupGlobalRegistry()

	if err := SetupLogger(); err != nil {
		log.Printf("Error: '%s'\n", err)
		return
	}

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
		Producers: make(map[string]*stream.Producer),
	})
}

func SetupLogger() error {
	file, err := os.OpenFile("gomq.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	if err != nil {
		return err
	}

	writer := io.MultiWriter(os.Stdout, file)

	log.SetOutput(writer)

	log.SetFlags(log.Ldate | log.Ltime)
	log.SetPrefix("[GOMQ] ")

	return nil
}

func SetupNetworkLayer() error {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: 3333,
		IP:   net.IPv4(127, 0, 0, 1),
	})

	log.Println("INFO Server has started.")

	if err != nil {
		return err
	}

	for {
		conn, err := listener.AcceptTCP()

		if err != nil {
			continue
		}

		log.Printf("INFO %s - New connection established.\n", conn.RemoteAddr().String())

		go HandleConnection(conn)
	}
}

func HandleConnection(conn *net.TCPConn) {
	addr := conn.RemoteAddr().String()

	defer conn.Close()
	defer log.Printf("INFO %s - Connection has been closed.\n", addr)

	data := make([]byte, SOCK_BUFFER_SIZE)
	_, err := conn.Read(data)
	data = bytes.Trim(data, "\x00")

	log.Printf("INFO %s - Client registration payload: '%s'.\n", addr, string(data))

	if err != nil {
		return
	}

	message, err := serializers.FromJson[messages.Registration](data)

	if err != nil {
		log.Println("ERROR Couldn't parse registration message, disconnecting client.")
		return
	}

	switch message.Type {
	case "CONSUMER":
		log.Printf("INFO %s - Consumer with topic '%s' registered.", addr, message.Topic)
		HandleConsumer(conn, message)
		log.Printf("INFO %s - Consumer with topic '%s' disconnected.", addr, message.Topic)
		break
	case "PRODUCER":
		log.Printf("INFO %s - Producer with topic '%s' registered.", addr, message.Topic)
		HandleProducer(conn, message)
		log.Printf("INFO %s - Producer with topic '%s' disconnected.", addr, message.Topic)
		break
	default:
		break
	}
}

func HandleConsumer(conn *net.TCPConn, message messages.Registration) {
	registry := globalRegistry.Lock()

	producer, ok := registry.Producers[message.Topic]

	if !ok {
		log.Printf("ERROR Producer with the topic '%s' didn't exist, closing connection.", message.Topic)
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
		log.Printf("ERROR Producer for topic '%s' is already registered.", message.Topic)
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
	registry.Producers[message.Topic] = &producer
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

		producer.Push(messages.Payload{
			Error:   false,
			Payload: string(bytes.Trim(data, "\x00")),
		})
	}
}
