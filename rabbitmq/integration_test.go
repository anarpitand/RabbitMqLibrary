//go:build integration

// Package rabbitmq_test provides integration tests that require a live RabbitMQ broker.
package rabbitmq_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"RabbitMqLibrary/rabbitmq"
)

func TestIntegrationConnectAndHealth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{
			Host:        "localhost",
			VHost:       "/",
			QuorumVHost: "/quorum",
		},
		Queues: []rabbitmq.QueueConfig{
			{Name: "integration.test", QueueType: rabbitmq.QueueKindClassic},
			{Name: "integration.quorum", QueueType: rabbitmq.QueueKindQuorum},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := rabbitmq.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(ctx)

	if err := client.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestIntegrationTopologyDeclare(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{
			Host:        "localhost",
			VHost:       "/",
			QuorumVHost: "/quorum",
		},
		Queues: []rabbitmq.QueueConfig{
			{Name: "integration.topology.classic", QueueType: rabbitmq.QueueKindClassic, Priority: true, MaxPriority: intPtr(8)},
			{Name: "integration.topology.quorum", QueueType: rabbitmq.QueueKindQuorum},
			{Name: "integration.topology.publishonly", Role: rabbitmq.QueueRolePublishOnly, QueueType: rabbitmq.QueueKindClassic},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := rabbitmq.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(ctx)

	verifyQueue := func(vhost, queue string) {
		dialURL := fmt.Sprintf("amqp://guest:guest@localhost:5672/%s", url.PathEscape(vhost))
		conn, err := amqp.Dial(dialURL)
		if err != nil {
			t.Fatalf("dial %s: %v", vhost, err)
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("channel %s: %v", vhost, err)
		}
		defer ch.Close()

		if _, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil); err != nil {
			t.Fatalf("passive declare %s on %s: %v", queue, vhost, err)
		}
	}

	verifyQueue("/", "integration.topology.classic")
	verifyQueue("/", "integration.topology.classic.dlq")
	verifyQueue("/", "integration.topology.publishonly")
	verifyQueue("/quorum", "integration.topology.quorum")
	verifyQueue("/quorum", "integration.topology.quorum.dlq")
}

func TestIntegrationPublish(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{
			Host:        "localhost",
			VHost:       "/",
			QuorumVHost: "/quorum",
		},
		Queues: []rabbitmq.QueueConfig{
			{Name: "integration.publish.classic", QueueType: rabbitmq.QueueKindClassic, Priority: true, MaxPriority: intPtr(10)},
			{Name: "integration.publish.quorum", QueueType: rabbitmq.QueueKindQuorum},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := rabbitmq.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(ctx)

	if err := client.Publish(ctx, []byte(`{"event":"classic"}`), "integration.publish.classic", 5); err != nil {
		t.Fatalf("publish classic with priority: %v", err)
	}
	if err := client.Publish(ctx, []byte(`{"event":"classic-normal"}`), "integration.publish.classic", 0); err != nil {
		t.Fatalf("publish classic without priority: %v", err)
	}
	if err := client.Publish(ctx, []byte(`{"event":"quorum"}`), "integration.publish.quorum", 12); err != nil {
		t.Fatalf("publish quorum with priority: %v", err)
	}

	getMessage := func(vhost, queue string) string {
		dialURL := fmt.Sprintf("amqp://guest:guest@localhost:5672/%s", url.PathEscape(vhost))
		conn, err := amqp.Dial(dialURL)
		if err != nil {
			t.Fatalf("dial %s: %v", vhost, err)
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("channel %s: %v", vhost, err)
		}
		defer ch.Close()

		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("basic get %s on %s: %v", queue, vhost, err)
		}
		if !ok {
			t.Fatalf("no message on %s", queue)
		}
		return string(msg.Body)
	}

	if body := getMessage("/", "integration.publish.classic"); body != `{"event":"classic"}` {
		t.Fatalf("classic body: got %q", body)
	}
	if body := getMessage("/quorum", "integration.publish.quorum"); body != `{"event":"quorum"}` {
		t.Fatalf("quorum body: got %q", body)
	}
}

func TestIntegrationConsume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{
			Host:        "localhost",
			VHost:       "/",
			QuorumVHost: "/quorum",
		},
		Queues: []rabbitmq.QueueConfig{
			{Name: "integration.consume.classic", QueueType: rabbitmq.QueueKindClassic},
			{Name: "integration.consume.quorum", QueueType: rabbitmq.QueueKindQuorum},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	client, err := rabbitmq.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(ctx)

	received := make(chan string, 2)
	if err := client.RegisterConsumer("integration.consume.classic", func(ctx context.Context, d rabbitmq.Delivery) error {
		received <- string(d.Body)
		return nil
	}); err != nil {
		t.Fatalf("register classic: %v", err)
	}
	if err := client.RegisterConsumer("integration.consume.quorum", func(ctx context.Context, d rabbitmq.Delivery) error {
		received <- string(d.Body)
		return nil
	}); err != nil {
		t.Fatalf("register quorum: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if err := client.Publish(ctx, []byte("classic-msg"), "integration.consume.classic", 0); err != nil {
		t.Fatalf("publish classic: %v", err)
	}
	if err := client.Publish(ctx, []byte("quorum-msg"), "integration.consume.quorum", 0); err != nil {
		t.Fatalf("publish quorum: %v", err)
	}

	waitFor := func(want string) {
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("body: got %q want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout waiting for %q", want)
		}
	}

	waitFor("classic-msg")
	waitFor("quorum-msg")
}

func TestIntegrationGracefulShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{Host: "localhost", VHost: "/"},
		Queues:     []rabbitmq.QueueConfig{{Name: "integration.shutdown", QueueType: rabbitmq.QueueKindClassic}},
	}

	ctx := context.Background()

	client, err := rabbitmq.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	done := make(chan struct{})
	started := make(chan struct{})

	if err := client.RegisterConsumer("integration.shutdown", func(ctx context.Context, d rabbitmq.Delivery) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if err := client.Publish(ctx, []byte("shutdown-msg"), "integration.shutdown", 0); err != nil {
		t.Fatalf("publish: %v", err)
	}

	<-started
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not complete during graceful shutdown")
	}
}

func TestIntegrationDeadLetterRetryThenPark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	src := "integration.dl." + suffix
	dlq := src + ".dlq"
	one := 1

	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{Host: "localhost", VHost: "/"},
		Queues: []rabbitmq.QueueConfig{
			{
				Name:       src,
				QueueType:  rabbitmq.QueueKindClassic,
				DeadLetter: &rabbitmq.DeadLetterConfig{MaxRetries: &one},
			},
			{Name: dlq, QueueType: rabbitmq.QueueKindClassic},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := rabbitmq.New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close(ctx)

	parked := make(chan string, 1)
	if err := client.RegisterConsumer(src, func(ctx context.Context, d rabbitmq.Delivery) error {
		return fmt.Errorf("fail")
	}); err != nil {
		t.Fatalf("register source: %v", err)
	}
	if err := client.RegisterConsumer(dlq, func(ctx context.Context, d rabbitmq.Delivery) error {
		parked <- string(d.Body)
		return nil
	}); err != nil {
		t.Fatalf("register dlq: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	if err := client.Publish(ctx, []byte("poison-msg"), src, 0); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-parked:
		if got != "poison-msg" {
			t.Fatalf("dlq body: got %q", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for dead-lettered message")
	}
}

func intPtr(v int) *int {
	return &v
}
