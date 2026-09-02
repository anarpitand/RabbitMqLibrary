// Package rabbitmq_test provides black-box tests for the public rabbitmq API.
package rabbitmq_test

import (
	"strings"
	"testing"

	"RabbitMqLibrary/rabbitmq"
)

const sampleJSON = `{
  "connection": {
    "host": "localhost",
    "vhost": "/",
    "quorum_vhost": "/quorum"
  },
  "queues": [
    {
      "name": "orders.created",
      "queue_type": "quorum",
      "dead_letter": {
        "max_retries": 2
      }
    },
    {
      "name": "orders.created.dlq",
      "queue_type": "quorum"
    },
    {
      "name": "orders.failed",
      "queue_type": "classic",
      "priority": true,
      "max_priority": 8
    }
  ]
}`

const sampleYAML = `
connection:
  host: localhost
  vhost: /
  quorum_vhost: /quorum
queues:
  - name: orders.created
    queue_type: quorum
  - name: orders.outbound
    role: publishonly
    queue_type: classic
`

func TestLoadConfigFromJSON(t *testing.T) {
	cfg, err := rabbitmq.LoadConfigFromJSON(strings.NewReader(sampleJSON))
	if err != nil {
		t.Fatalf("LoadConfigFromJSON: %v", err)
	}
	if len(cfg.Queues) != 3 {
		t.Fatalf("queues len: got %d", len(cfg.Queues))
	}
	if cfg.Queues[0].DeadLetter == nil || cfg.Queues[0].DeadLetter.MaxRetriesOrDefault() != 2 {
		t.Fatal("expected dead_letter max_retries 2")
	}
	if cfg.Queues[1].DeadLetter != nil {
		t.Fatal("park queue must not have dead_letter")
	}
	if cfg.Queues[2].MaxPriorityOrDefault() != 8 {
		t.Fatalf("max_priority: got %d", cfg.Queues[2].MaxPriorityOrDefault())
	}
}

func TestLoadConfigFromYAML(t *testing.T) {
	cfg, err := rabbitmq.LoadConfigFromYAML(strings.NewReader(sampleYAML))
	if err != nil {
		t.Fatalf("LoadConfigFromYAML: %v", err)
	}
	if cfg.Queues[1].Role != rabbitmq.QueueRolePublishOnly {
		t.Fatalf("role: got %q", cfg.Queues[1].Role)
	}
	if cfg.Queues[0].DeadLetter == nil || cfg.Queues[0].DeadLetter.MaxRetriesOrDefault() != 3 {
		t.Fatal("expected default dead_letter on subscriber")
	}
}

func TestLoadConfigFromJSONUnknownField(t *testing.T) {
	_, err := rabbitmq.LoadConfigFromJSON(strings.NewReader(`{"unknown": true}`))
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
}
