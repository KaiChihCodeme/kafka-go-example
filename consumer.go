package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func NewConsumer(ctx context.Context) {
	msgChan := make(chan kafka.Message)

	// normal consumer
	go Consumer(ctx, msgChan)
	// sasl authentication consumer
	// go SASLConsumer(ctx, msgChan, "plain", "", "username", "password")
	// go SASLConsumer(ctx, msgChan, "SCRAM", "SHA512", "username", "password")

	for msg := range msgChan {
		fmt.Println("Received message:", string(msg.Value))
		// DO SOMETHING
	}

}

func Consumer(context context.Context, msgChan chan kafka.Message) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "my-group",
		Topic:    "test-topic",
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.ReadMessage(context)
		if err != nil {
			break
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		msgChan <- m
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func SASLConsumer(context context.Context, msgChan chan kafka.Message, authType string, authMechanism string, username string, password string) {
	var dialer *kafka.Dialer

	switch authType {
	case "plain":
		mechanism := plain.Mechanism{
			Username: username,
			Password: password,
		}

		dialer = &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: mechanism,
		}
	case "SCRAM":
		var algo scram.Algorithm
		if authMechanism == "SHA512" {
			algo = scram.SHA512
		} else if authMechanism == "SHA256" {
			algo = scram.SHA256
		}

		mechanism, err := scram.Mechanism(algo, username, password)
		if err != nil {
			log.Fatal("failed to create SCRAM mechanism:", err)
		}

		dialer = &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: mechanism,
		}
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "my-group", // if we have group id, we can consume all partitions, otherwise we can only consume one partition
		Topic:       "test-topic",
		MaxBytes:    10e6, // 10MB
		Dialer:      dialer,
		Logger:      kafka.LoggerFunc(logf), // add logger to kafka.Reader
		ErrorLogger: kafka.LoggerFunc(logf),
	})

	for {
		m, err := r.ReadMessage(context)
		if err != nil {
			break
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		msgChan <- m
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func SeperateCommitConsumer(context context.Context, msgChan chan kafka.Message) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "my-group",
		Topic:    "test-topic",
		MaxBytes: 10e6, // 10MB
	})

	for {
		m, err := r.FetchMessage(context)
		if err != nil {
			break
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		msgChan <- m

		if err := r.CommitMessages(context, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

}

func logf(ms string, a ...interface{}) {
	fmt.Printf(ms, a...)
	fmt.Println()
}
