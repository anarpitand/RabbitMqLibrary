package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"RabbitMqLibrary/rabbitmq"
)

func main() {
	ctx := context.Background()

	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{
			Host:  "localhost",
			VHost: "/",
		},
		Queues: []rabbitmq.QueueConfig{
			{Name: "example.basic", QueueType: rabbitmq.QueueKindClassic},
		},
	}

	client, err := rabbitmq.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	received := make(chan string, 1)
	if err := client.RegisterConsumer("example.basic", func(ctx context.Context, d rabbitmq.Delivery) error {
		received <- string(d.Body)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	payload, err := json.Marshal(map[string]string{"hello": "world"})
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Publish(ctx, payload, "example.basic", 0); err != nil {
		log.Fatal(err)
	}

	select {
	case msg := <-received:
		log.Printf("received: %s", msg)
	case <-time.After(5 * time.Second):
		log.Fatal("timeout waiting for message")
	}
}
