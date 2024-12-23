package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Company struct {
	CompanyId string `json:"companyId"`
}

func simpleWriter(ctx context.Context) {
	// create a new kafka writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	})

	company := &Company{
		CompanyId: "12aaa3",
	}

	companyJson, _ := json.Marshal(company)

	// write a message
	w.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte("key"),
			Value: companyJson,
		},
	)

	// close the writer
	w.Close()
}

func generalWriter(ctx context.Context) {
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{}, // new msg will be sent to the partition with the least bytes of data for average distribution
	}

	err := w.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func autoCreateTopicWriter(ctx context.Context) {
	// Make a writer that publishes messages to topic-A.
	// The topic will be created if it is missing.
	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "topic-A",
		AllowAutoTopicCreation: true,
	}

	messages := []kafka.Message{
		{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	}

	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err = w.WriteMessages(ctx, messages...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Fatalf("unexpected error %v", err)
		}
		break
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func writeToMultipleTopics(ctx context.Context) {
	w := &kafka.Writer{
		Addr: kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		// NOTE: When Topic is not defined here, each Message must define it instead.
		Balancer: &kafka.LeastBytes{},
	}

	err := w.WriteMessages(context.Background(),
		// NOTE: Each Message has Topic defined, otherwise an error is returned.
		kafka.Message{
			Topic: "topic-A",
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Topic: "topic-B",
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Topic: "topic-C",
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func saslWriter(ctx context.Context, username string, password string) {
	mechanism, err := scram.Mechanism(scram.SHA512, username, password)
	if err != nil {
		panic(err)
	}

	// Transports are responsible for managing connection pools and other resources,
	// it's generally best to create a few of these and share them across your
	// application.
	sharedTransport := &kafka.Transport{
		SASL: mechanism,
	}

	w := kafka.Writer{
		Addr:      kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Topic:     "topic-A",
		Balancer:  &kafka.Hash{},
		Transport: sharedTransport,
	}

	messages := []kafka.Message{
		{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	}

	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err = w.WriteMessages(ctx, messages...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Fatalf("unexpected error %v", err)
		}
		break
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
