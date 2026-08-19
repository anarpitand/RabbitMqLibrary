package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	"RabbitMqLibrary/rabbitmq"
)

func main() {
	ctx := context.Background()

	configPath := filepath.Join("examples", "config-driven", "config.json")
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	client, err := rabbitmq.LoadClientFromFile(ctx, configPath)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close(ctx)

	received := make(chan string, 1)
	if err := client.RegisterConsumer("example.config", func(ctx context.Context, d rabbitmq.Delivery) error {
		received <- string(d.Body)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	payload, err := json.Marshal(map[string]string{"source": "config-driven"})
	if err != nil {
		log.Fatal(err)
	}

	if err := client.Publish(ctx, payload, "example.config", 0); err != nil {
		log.Fatal(err)
	}

	select {
	case msg := <-received:
		log.Printf("received: %s", msg)
	case <-time.After(5 * time.Second):
		log.Fatal("timeout waiting for message")
	}
}
