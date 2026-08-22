// Package rabbitmq tests unexported publisher helpers (white-box).
package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestValidatePublishPriority(t *testing.T) {
	tests := []struct {
		name     string
		q        QueueConfig
		priority int
		wantErr  error
	}{
		{
			name:     "classic zero priority",
			q:        QueueConfig{Name: "q", QueueType: QueueKindClassic},
			priority: 0,
		},
		{
			name:     "classic with priority enabled",
			q:        QueueConfig{Name: "q", QueueType: QueueKindClassic, Priority: true},
			priority: 5,
		},
		{
			name:     "classic priority not supported",
			q:        QueueConfig{Name: "q", QueueType: QueueKindClassic},
			priority: 1,
			wantErr:  ErrPriorityNotSupported,
		},
		{
			name:     "classic priority too high",
			q:        QueueConfig{Name: "q", QueueType: QueueKindClassic, Priority: true},
			priority: 11,
			wantErr:  ErrInvalidPriority,
		},
		{
			name:     "negative priority",
			q:        QueueConfig{Name: "q", QueueType: QueueKindClassic},
			priority: -1,
			wantErr:  ErrInvalidPriority,
		},
		{
			name:     "quorum valid priority",
			q:        QueueConfig{Name: "q", QueueType: QueueKindQuorum},
			priority: 15,
		},
		{
			name:     "quorum priority too high",
			q:        QueueConfig{Name: "q", QueueType: QueueKindQuorum},
			priority: 32,
			wantErr:  ErrInvalidPriority,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePublishPriority(&tc.q, tc.priority)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != tc.wantErr {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestIsRetryablePublishError(t *testing.T) {
	if !isRetryablePublishError(ErrPublishNotConfirmed) {
		t.Fatal("expected retryable")
	}
	if !isRetryablePublishError(ErrNotConnected) {
		t.Fatal("expected retryable")
	}
	if isRetryablePublishError(ErrEmptyPayload) {
		t.Fatal("expected not retryable")
	}
	if isRetryablePublishError(ErrQueueNotFound) {
		t.Fatal("expected not retryable")
	}
	if isRetryablePublishError(&amqp.Error{Code: amqp.NotFound, Reason: "no queue"}) {
		t.Fatal("expected NotFound not retryable")
	}
	if isRetryablePublishError(&amqp.Error{Code: amqp.AccessRefused, Reason: "denied"}) {
		t.Fatal("expected AccessRefused not retryable")
	}
	if !isRetryablePublishError(&amqp.Error{Code: amqp.ChannelError, Reason: "channel"}) {
		t.Fatal("expected ChannelError retryable")
	}
}

func TestMapConfirmWaitError(t *testing.T) {
	parent := context.Background()

	if err := mapConfirmWaitError(parent, context.DeadlineExceeded); !errors.Is(err, ErrPublishNotConfirmed) {
		t.Fatalf("confirm timeout: got %v, want ErrPublishNotConfirmed", err)
	}
	if err := mapConfirmWaitError(parent, context.Canceled); !errors.Is(err, ErrPublishNotConfirmed) {
		t.Fatalf("canceled wait ctx: got %v, want ErrPublishNotConfirmed", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mapConfirmWaitError(canceled, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("parent canceled: got %v, want context.Canceled", err)
	}

	other := errors.New("other")
	if err := mapConfirmWaitError(parent, other); !errors.Is(err, other) {
		t.Fatalf("other error: got %v", err)
	}
}

func TestInvalidateKindLockedNoDeadlock(t *testing.T) {
	p := NewPublisher(nil, Config{
		Connection: ConnectionConfig{VHost: "/", QuorumVHost: "/quorum"},
	}, nil, time.Second, 0)

	done := make(chan struct{})
	go func() {
		p.classicMu.Lock()
		p.invalidateKindLocked(QueueKindClassic)
		p.classicMu.Unlock()

		p.quorumMu.Lock()
		p.invalidateKindLocked(QueueKindQuorum)
		p.quorumMu.Unlock()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("invalidateKindLocked deadlocked while mutex held")
	}
}

func TestDefaultClientPublishRetries(t *testing.T) {
	o := defaultClientOptions()
	if o.publishMaxRetries != defaultPublishRetries {
		t.Fatalf("publishMaxRetries: got %d, want %d", o.publishMaxRetries, defaultPublishRetries)
	}
}
