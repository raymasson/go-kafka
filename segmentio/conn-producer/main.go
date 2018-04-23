package main

import (
	"context"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	// to produce messages
	topic := "my-topic"
	partition := 0

	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	conn.WriteMessages(
		kafka.Message{Value: []byte("first message")},
		kafka.Message{Value: []byte("second message")},
		kafka.Message{Value: []byte("third message")},
	)

	conn.Close()
}
