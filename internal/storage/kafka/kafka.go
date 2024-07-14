package kafka

import (
	"encoding/json"
	"fmt"
	"kafich/internal"
	"log"

	"github.com/IBM/sarama"
)

type Kafi struct {
	client sarama.SyncProducer
	topic  string
}

func New(broker []string, topic string) (*Kafi, error) {
	scfg := sarama.NewConfig()
	scfg.Producer.Return.Successes = true
	scfg.Producer.RequiredAcks = sarama.WaitForAll
	scfg.Producer.Retry.Max = 5

	client, err := sarama.NewSyncProducer(broker, scfg)
	if err != nil {
		return nil, fmt.Errorf("creating kafka producer: %v", err)
	}

	return &Kafi{
		client: client,
		topic:  topic,
	}, nil
}

func (k *Kafi) PushOrder(message internal.Order) error {
	bMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshalling: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.StringEncoder(bMessage),
	}

	partition, offset, err := k.client.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("sending message to kafka: %w", err)
	}
	log.Printf("order is stored in topic '%s', partition '%d', offset '%d'\n", k.topic, partition, offset)

	return nil
}

func (k *Kafi) Close() error {
	return k.client.Close()
}
