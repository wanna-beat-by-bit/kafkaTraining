package main

import (
	"kafich/internal"
	"kafich/internal/storage/kafka"
	"log"
)

const produceTopic = "testTopic"
const kafkaBroker = "localhost:9092"

func main() {
	log.Println("producer")

	client, err := kafka.New([]string{kafkaBroker}, produceTopic)
	if err != nil {
		log.Fatalf("connecting to kafka: %v", err)
	}

	defer func() { _ = client.Close() }()

	message := internal.Order{
		ID:        "8",
		ProductID: "123213",
	}

	if err := client.PushOrder(message); err != nil {
		log.Fatalf("sending message to kafka: %v", err)
	}

}
