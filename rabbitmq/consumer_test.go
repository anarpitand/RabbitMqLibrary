// Package rabbitmq tests unexported consumer helpers (white-box).
package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRegisterConsumerValidation(t *testing.T) {
	cfg := Config{
		Connection: ConnectionConfig{Host: "localhost", VHost: "/", QuorumVHost: "/quorum"},
		Queues: []QueueConfig{
			{Name: "subscriber", QueueType: QueueKindClassic},
			{Name: "publishonly", Role: QueueRolePublishOnly, QueueType: QueueKindClassic},
		},
	}
	cfg.ApplyDefaults()

	mgr := NewConsumerManager(nil, cfg, nil, nil)

	if err := mgr.RegisterConsumer("missing", func(ctx context.Context, d Delivery) error { return nil }); err != ErrQueueNotFound {
		t.Fatalf("expected ErrQueueNotFound, got %v", err)
	}

	if err := mgr.RegisterConsumer("publishonly", func(ctx context.Context, d Delivery) error { return nil }); err != ErrPublishOnlyQueue {
		t.Fatalf("expected ErrPublishOnlyQueue, got %v", err)
	}

	if err := mgr.RegisterConsumer("subscriber", nil); err == nil {
		t.Fatal("expected error for nil handler")
	}

	mgr.stopped = true
	if err := mgr.RegisterConsumer("subscriber", func(ctx context.Context, d Delivery) error { return nil }); err != ErrConsumerStopped {
		t.Fatalf("expected ErrConsumerStopped, got %v", err)
	}
}

func TestDefaultConsumerSettings(t *testing.T) {
	s := defaultConsumerSettings()
	if s.prefetch != defaultPrefetch {
		t.Fatalf("prefetch: got %d", s.prefetch)
	}
	if s.concurrency != defaultConcurrency {
		t.Fatalf("concurrency: got %d", s.concurrency)
	}

	WithPrefetch(50)(&s)
	WithConcurrency(4)(&s)
	if s.prefetch != 50 || s.concurrency != 4 {
		t.Fatal("options not applied")
	}
}

func TestNewDelivery(t *testing.T) {
	d := newDelivery("orders", amqp.Delivery{
		Body:        []byte("body"),
		ContentType: "json",
		Priority:    5,
		RoutingKey:  "orders",
		Exchange:    "orders.ex",
	})
	if d.QueueName != "orders" || string(d.Body) != "body" || d.ContentType != "json" || d.Priority != 5 {
		t.Fatalf("unexpected delivery: %+v", d)
	}
}

func TestRegisterConsumerDuplicate(t *testing.T) {
	cfg := Config{
		Connection: ConnectionConfig{Host: "localhost", VHost: "/"},
		Queues:     []QueueConfig{{Name: "q", QueueType: QueueKindClassic}},
	}
	cfg.ApplyDefaults()

	mgr := NewConsumerManager(nil, cfg, nil, nil)
	handler := func(ctx context.Context, d Delivery) error { return nil }

	mgr.runners["q"] = &consumerRunner{mgr: mgr, queueName: "q"}

	err := mgr.RegisterConsumer("q", handler)
	if err == nil {
		t.Fatal("expected duplicate registration error")
	}
	if !errors.Is(err, ErrConfigInvalid) {
		t.Fatalf("expected ErrConfigInvalid, got %v", err)
	}
}

func TestRegisterConsumerStopWaitsForRunner(t *testing.T) {
	cfg := Config{
		Connection: ConnectionConfig{
			Host:                     "localhost",
			VHost:                    "/",
			ReconnectIntervalSeconds: 1,
		},
		Queues: []QueueConfig{{Name: "q", QueueType: QueueKindClassic}},
	}
	cfg.ApplyDefaults()

	conn := NewConnectionManager(cfg, nil, nil, nil, nil)
	mgr := NewConsumerManager(conn, cfg, nil, nil)

	if err := mgr.RegisterConsumer("q", func(ctx context.Context, d Delivery) error {
		return nil
	}); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not complete; runnerWG likely missed registration")
	}
}

func TestRegisterConsumerStopRace(t *testing.T) {
	cfg := Config{
		Connection: ConnectionConfig{
			Host:                     "localhost",
			VHost:                    "/",
			ReconnectIntervalSeconds: 1,
		},
		Queues: []QueueConfig{{Name: "q", QueueType: QueueKindClassic}},
	}
	cfg.ApplyDefaults()

	const iterations = 50
	var wg sync.WaitGroup
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := NewConnectionManager(cfg, nil, nil, nil, nil)
			mgr := NewConsumerManager(conn, cfg, nil, nil)

			var inner sync.WaitGroup
			inner.Add(2)
			go func() {
				defer inner.Done()
				_ = mgr.RegisterConsumer("q", func(ctx context.Context, d Delivery) error {
					return nil
				})
			}()
			go func() {
				defer inner.Done()
				mgr.Stop()
			}()
			inner.Wait()
		}()
	}
	wg.Wait()
}

func TestDeadLetterTarget(t *testing.T) {
	one := 1
	q := QueueConfig{
		Name: "jobs",
		DeadLetter: &DeadLetterConfig{
			MaxRetries: &one,
		},
	}
	target, next := deadLetterTarget(q, 0)
	if target != "jobs.retry.0" || next != 1 {
		t.Fatalf("first nack: %s %d", target, next)
	}
	target, next = deadLetterTarget(q, 1)
	if target != "jobs.dlq" || next != 1 {
		t.Fatalf("park: %s %d", target, next)
	}
}
