package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

type user struct {
	ID        string `json:"id"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

// Producer : produces kafka messages
type Producer struct {
	Sync sarama.SyncProducer
}

// NewProducer : creates the kafka producer
func NewProducer(brokers []string) (Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	sync, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return Producer{
			Sync: sync,
		}, err
	}

	return Producer{
		Sync: sync,
	}, nil
}

// ProduceMessage : produces the kafka message
func (p Producer) ProduceMessage(topic string, key, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
		Key:   sarama.ByteEncoder(key),
	}

	_, _, err := p.Sync.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}

func main() {

	brokerPeers := "localhost:9092"

	// init Producer
	producer, _ := NewProducer(strings.Split(brokerPeers, ","))

	user := user{
		ID:        "1",
		FirstName: "Ray",
		LastName:  "MASSON",
	}

	jsonObject, _ := json.Marshal(&user)
	fmt.Println(string(jsonObject))

	producer.ProduceMessage("my-topic", []byte(user.ID), []byte(jsonObject))
}
