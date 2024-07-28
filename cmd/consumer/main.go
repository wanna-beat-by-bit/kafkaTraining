package main

import (
	"kafich/internal/storage/kafka"
	"log"
	"time"
)

const produceTopic = "testTopic"
const kafkaBroker = "localhost:29092"

func main() {
	log.Println("consumer")
	kafka.NewConsumer([]string{kafkaBroker}, []string{produceTopic}, produceTopic+"-theGroup")
	log.Println("consumer ready")

	time.Sleep(time.Second * 1000)
	// fmt.Println("oldest offest: %v", client.OffsetOldest())

	// message := shared.Order{
	// 	ID:        "asdh",
	// 	ProductID: "myakish",
	// }
	// message := shared.Other{
	// 	BID:     "baba",
	// 	Name:    "Petya",
	// 	Articul: "vuba",
	// }

}
