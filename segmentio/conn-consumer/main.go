package main

import (
	"context"
	"fmt"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	// to consume messages
	topic := "my-topic"
	partition := 0

	// Set the connection
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

	// lock for 10s before consuming messages
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}

	batch.Close()
	conn.Close()
}
