package main

import (
	"kafich/internal/shared"
	"kafich/internal/storage/kafka"
	"log"
)

const produceTopic = "testTopic"
const kafkaBroker = "localhost:29092"

func main() {
	log.Println("producer")

	client, err := kafka.NewProducer([]string{kafkaBroker}, produceTopic)
	if err != nil {
		log.Fatalf("connecting to kafka: %v", err)
	}

	defer func() { _ = client.Close() }()

	message := shared.Other{
		BID:     "one",
		Name:    "two",
		Articul: "three",
	}

	if err := client.PushOther(message); err != nil {
		log.Fatalf("sending message to kafka: %v", err)
	}

}
