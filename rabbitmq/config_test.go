// Package rabbitmq_test provides black-box tests for the public rabbitmq API.
package rabbitmq_test

import (
	"strings"
	"testing"

	"RabbitMqLibrary/rabbitmq"
)

func validConfig() rabbitmq.Config {
	return rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{
			Host:        "localhost",
			VHost:       "/",
			QuorumVHost: "/quorum",
		},
		Queues: []rabbitmq.QueueConfig{
			{Name: "orders.created", QueueType: rabbitmq.QueueKindQuorum},
			{Name: "orders.failed", QueueType: rabbitmq.QueueKindClassic},
		},
	}
}

func TestApplyDefaults(t *testing.T) {
	cfg := rabbitmq.Config{
		Queues: []rabbitmq.QueueConfig{
			{Name: "events", QueueType: rabbitmq.QueueKindClassic},
		},
	}
	cfg.ApplyDefaults()

	if cfg.Connection.Username != "guest" {
		t.Fatalf("username default: got %q", cfg.Connection.Username)
	}
	if cfg.Connection.Host != "localhost" {
		t.Fatalf("host default: got %q", cfg.Connection.Host)
	}
	if cfg.Connection.VHost != "/" {
		t.Fatalf("vhost default: got %q", cfg.Connection.VHost)
	}
	if cfg.Connection.Port != 5672 {
		t.Fatalf("port default: got %d", cfg.Connection.Port)
	}
	if !cfg.Connection.AutoReconnectOrDefault() {
		t.Fatal("auto_reconnect default should be true")
	}
	if cfg.Queues[0].Role != rabbitmq.QueueRoleSubscriber {
		t.Fatalf("role default: got %q", cfg.Queues[0].Role)
	}
	if cfg.Queues[0].Exchange != "events" {
		t.Fatalf("exchange default: got %q", cfg.Queues[0].Exchange)
	}
	if cfg.Queues[0].RoutingKey != "events" {
		t.Fatalf("routing_key default: got %q", cfg.Queues[0].RoutingKey)
	}
	if cfg.Queues[0].ExchangeType != "direct" {
		t.Fatalf("exchange_type default: got %q", cfg.Queues[0].ExchangeType)
	}
	dl := cfg.Queues[0].DeadLetter
	if dl == nil || dl.MaxRetriesOrDefault() != 3 || dl.InitialDelayMs != 1000 || dl.MaxDelayMs != 60000 {
		t.Fatalf("dead_letter defaults: %+v", dl)
	}
}

func TestValidateSuccess(t *testing.T) {
	cfg := validConfig()
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config: %v", err)
	}
}

func TestValidateErrors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*rabbitmq.Config)
		wantSub string
	}{
		{
			name: "empty queues",
			mutate: func(c *rabbitmq.Config) {
				c.Queues = nil
			},
			wantSub: "queues must not be empty",
		},
		{
			name: "duplicate queue name",
			mutate: func(c *rabbitmq.Config) {
				c.Queues = []rabbitmq.QueueConfig{
					{Name: "dup", QueueType: rabbitmq.QueueKindClassic},
					{Name: "dup", QueueType: rabbitmq.QueueKindClassic},
				}
			},
			wantSub: "duplicate queue name",
		},
		{
			name: "missing quorum vhost",
			mutate: func(c *rabbitmq.Config) {
				c.Connection.QuorumVHost = ""
				c.Queues = []rabbitmq.QueueConfig{{Name: "q", QueueType: rabbitmq.QueueKindQuorum}}
			},
			wantSub: "quorum_vhost is required",
		},
		{
			name: "quorum with priority flag",
			mutate: func(c *rabbitmq.Config) {
				c.Queues = []rabbitmq.QueueConfig{
					{Name: "q", QueueType: rabbitmq.QueueKindQuorum, Priority: true},
				}
			},
			wantSub: "priority is not supported on quorum",
		},
		{
			name: "classic max_priority without priority",
			mutate: func(c *rabbitmq.Config) {
				maxP := 5
				c.Queues = []rabbitmq.QueueConfig{
					{Name: "q", QueueType: rabbitmq.QueueKindClassic, MaxPriority: &maxP},
				}
			},
			wantSub: "max_priority cannot be set when priority is false",
		},
		{
			name: "classic invalid max_priority",
			mutate: func(c *rabbitmq.Config) {
				maxP := 11
				c.Queues = []rabbitmq.QueueConfig{
					{Name: "q", QueueType: rabbitmq.QueueKindClassic, Priority: true, MaxPriority: &maxP},
				}
			},
			wantSub: "max_priority must be between 1 and 10",
		},
		{
			name: "invalid role",
			mutate: func(c *rabbitmq.Config) {
				c.Queues[0].Role = "worker"
			},
			wantSub: "role must be subscriber or publishonly",
		},
		{
			name: "invalid exchange type",
			mutate: func(c *rabbitmq.Config) {
				c.Queues[0].ExchangeType = "headers"
			},
			wantSub: "exchange_type must be direct, topic, or fanout",
		},
		{
			name: "dead_letter on publishonly",
			mutate: func(c *rabbitmq.Config) {
				c.Queues = []rabbitmq.QueueConfig{
					{Name: "out", Role: rabbitmq.QueueRolePublishOnly, QueueType: rabbitmq.QueueKindClassic, DeadLetter: &rabbitmq.DeadLetterConfig{}},
				}
			},
			wantSub: "not allowed on publishonly",
		},
		{
			name: "dead_letter on park queue",
			mutate: func(c *rabbitmq.Config) {
				c.Queues = []rabbitmq.QueueConfig{
					{Name: "jobs", QueueType: rabbitmq.QueueKindClassic},
					{Name: "jobs.dlq", QueueType: rabbitmq.QueueKindClassic, DeadLetter: &rabbitmq.DeadLetterConfig{}},
				}
			},
			wantSub: "not allowed on a dead-letter park queue",
		},
		{
			name: "dead_letter park queue type mismatch",
			mutate: func(c *rabbitmq.Config) {
				c.Queues = []rabbitmq.QueueConfig{
					{Name: "jobs", QueueType: rabbitmq.QueueKindClassic},
					{Name: "jobs.dlq", QueueType: rabbitmq.QueueKindQuorum},
				}
				c.Connection.QuorumVHost = "/quorum"
			},
			wantSub: "must have the same queue_type",
		},
		{
			name: "retry wait name collision",
			mutate: func(c *rabbitmq.Config) {
				one := 1
				c.Queues = []rabbitmq.QueueConfig{
					{Name: "jobs", QueueType: rabbitmq.QueueKindClassic, DeadLetter: &rabbitmq.DeadLetterConfig{MaxRetries: &one}},
					{Name: "jobs.retry.0", QueueType: rabbitmq.QueueKindClassic},
				}
			},
			wantSub: "collides with retry wait queue",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.ApplyDefaults()
			tc.mutate(&cfg)
			cfg.ApplyDefaults()
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestDeadLetterDefaults(t *testing.T) {
	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{Host: "localhost", VHost: "/"},
		Queues: []rabbitmq.QueueConfig{
			{Name: "jobs", QueueType: rabbitmq.QueueKindClassic},
		},
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	dl := cfg.Queues[0].DeadLetter
	if dl.MaxRetriesOrDefault() != 3 || dl.InitialDelayMs != 1000 || dl.MaxDelayMs != 60000 {
		t.Fatalf("defaults: %+v", dl)
	}
}

func TestDeadLetterMaxRetriesZero(t *testing.T) {
	zero := 0
	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{Host: "localhost", VHost: "/"},
		Queues: []rabbitmq.QueueConfig{
			{Name: "jobs", QueueType: rabbitmq.QueueKindClassic, DeadLetter: &rabbitmq.DeadLetterConfig{MaxRetries: &zero}},
		},
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Queues[0].DeadLetter.MaxRetriesOrDefault() != 0 {
		t.Fatal("max_retries 0 must be kept")
	}
}

func TestDeadLetterParkQueueInConfig(t *testing.T) {
	cfg := rabbitmq.Config{
		Connection: rabbitmq.ConnectionConfig{Host: "localhost", VHost: "/"},
		Queues: []rabbitmq.QueueConfig{
			{Name: "jobs", QueueType: rabbitmq.QueueKindClassic},
			{Name: "jobs.dlq", QueueType: rabbitmq.QueueKindClassic},
		},
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Queues[0].DeadLetter == nil {
		t.Fatal("source should have dead_letter defaults")
	}
	if cfg.Queues[1].DeadLetter != nil {
		t.Fatal("park queue must not get dead_letter")
	}
}

func TestQueueByName(t *testing.T) {
	cfg := validConfig()
	cfg.ApplyDefaults()

	if q := cfg.QueueByName("orders.created"); q == nil || q.QueueType != rabbitmq.QueueKindQuorum {
		t.Fatal("expected quorum queue")
	}
	if cfg.QueueByName("missing") != nil {
		t.Fatal("expected nil for unknown queue")
	}
}
