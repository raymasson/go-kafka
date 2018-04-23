package main

import (
	"context"
	"fmt"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	// make a new reader that consumes from topic-A, partition 0, at offset 0
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "topic-A",
		Partition: 3,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(-1)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}
