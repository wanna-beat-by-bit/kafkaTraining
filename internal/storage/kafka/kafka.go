package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"kafich/internal/shared"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	client sarama.SyncProducer
	topic  string
}

func NewProducer(broker []string, topic string) (*KafkaProducer, error) {
	scfg := sarama.NewConfig()
	scfg.Producer.Return.Successes = true
	scfg.Producer.RequiredAcks = sarama.WaitForAll
	scfg.Producer.Retry.Max = 5

	client, err := sarama.NewSyncProducer(broker, scfg)
	if err != nil {
		return nil, fmt.Errorf("creating kafka producer: %v", err)
	}

	return &KafkaProducer{
		client: client,
		topic:  topic,
	}, nil
}

func (k *KafkaProducer) PushOrder(message shared.Order) error {
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

func (k *KafkaProducer) PushOther(message shared.Other) error {
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

func (k *KafkaProducer) Close() error {
	return k.client.Close()
}

type Consumer struct {
	client sarama.ConsumerGroup
}

func NewConsumer(brokers []string, topics []string, groupName string) {
	scfg := sarama.NewConfig()
	scfg.Consumer.Return.Errors = true
	scfg.Consumer.Offsets.AutoCommit.Enable = false
	scfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup(brokers, groupName, scfg)
	if err != nil {
		log.Panicf("создание клиента группы консьюмеров: %v", err)
	}

	group := NewConsumerGroup(1, time.Second*5)
	go func() {
		// Вызываем `Consume` внутри бесконечного цикла, чтобы запуститься
		// снова после ребалансировки
		for {
			err = client.Consume(context.Background(), topics, group)
			if err != nil {
				log.Panicf("невозможен Consumer: %v", err)
			}
			log.Println("ребалансировка...")
			group.makeReadyAgain()
		}
	}()

	log.Println("wating")
	<-group.Ready()
}
