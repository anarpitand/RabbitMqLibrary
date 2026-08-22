// Package rabbitmq tests unexported topology helpers (white-box).
package rabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestQueueDeclareArgs(t *testing.T) {
	tests := []struct {
		name string
		q    QueueConfig
		want amqp.Table
	}{
		{
			name: "classic without priority",
			q:    QueueConfig{Name: "events", QueueType: QueueKindClassic},
			want: nil,
		},
		{
			name: "classic with priority default max",
			q:    QueueConfig{Name: "events", QueueType: QueueKindClassic, Priority: true},
			want: amqp.Table{"x-max-priority": int32(10)},
		},
		{
			name: "classic with priority custom max",
			q: func() QueueConfig {
				maxP := 7
				return QueueConfig{Name: "events", QueueType: QueueKindClassic, Priority: true, MaxPriority: &maxP}
			}(),
			want: amqp.Table{"x-max-priority": int32(7)},
		},
		{
			name: "quorum",
			q:    QueueConfig{Name: "orders", QueueType: QueueKindQuorum},
			want: amqp.Table{"x-queue-type": "quorum"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := queueDeclareArgs(tc.q)
			if tableEqual(got, tc.want) {
				return
			}
			t.Fatalf("queueDeclareArgs() = %v, want %v", got, tc.want)
		})
	}
}

func TestWaitQueueArgs(t *testing.T) {
	q := QueueConfig{Name: "orders", QueueType: QueueKindClassic, Priority: true}
	got := waitQueueArgs(q, 1000)
	want := amqp.Table{
		"x-message-ttl":             int64(1000),
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": "orders",
		"x-max-priority":            int32(10),
	}
	if !tableEqual(got, want) {
		t.Fatalf("waitQueueArgs() = %v, want %v", got, want)
	}

	qq := QueueConfig{Name: "orders", QueueType: QueueKindQuorum}
	got = waitQueueArgs(qq, 2000)
	if got["x-queue-type"] != "quorum" {
		t.Fatalf("expected quorum type: %v", got)
	}
	if got["x-dead-letter-routing-key"] != "orders" {
		t.Fatal("DLX must target the source queue name, not the app exchange")
	}
}

func tableEqual(a, b amqp.Table) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if av != bv {
			return false
		}
	}
	return true
}
