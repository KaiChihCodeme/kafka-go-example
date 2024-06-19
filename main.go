package main

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Company struct {
	CompanyId string `json:"companyId"`
}

func main() {
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
	w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("key"),
			Value: companyJson,
		},
	)

	// close the writer
	w.Close()
}
