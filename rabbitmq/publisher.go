package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"RabbitMqLibrary/internal/backoff"
)

const (
	defaultConfirmTimeout = 30 * time.Second
	defaultPublishRetries = 3
)

// Publisher publishes messages with broker confirms and retries.
type Publisher struct {
	conn   *ConnectionManager
	cfg    Config
	logger *slog.Logger

	confirmTimeout time.Duration
	maxRetries     int

	classicMu sync.Mutex
	classicCh *amqp.Channel

	quorumMu sync.Mutex
	quorumCh *amqp.Channel
}

// NewPublisher creates a publisher for the given connection and config.
func NewPublisher(conn *ConnectionManager, cfg Config, logger *slog.Logger, confirmTimeout time.Duration, maxRetries int) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	if confirmTimeout <= 0 {
		confirmTimeout = defaultConfirmTimeout
	}
	if maxRetries < 0 {
		maxRetries = defaultPublishRetries
	}

	return &Publisher{
		conn:           conn,
		cfg:            cfg,
		logger:         logger,
		confirmTimeout: confirmTimeout,
		maxRetries:     maxRetries,
	}
}

// Publish sends a message to the configured exchange and routing key for queueName.
// It waits for broker confirmation before returning.
func (p *Publisher) Publish(ctx context.Context, payload []byte, queueName string, priority int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(payload) == 0 {
		return ErrEmptyPayload
	}

	q := p.cfg.QueueByName(queueName)
	if q == nil {
		return ErrQueueNotFound
	}

	if err := validatePublishPriority(q, priority); err != nil {
		return err
	}

	base := time.Duration(p.cfg.Connection.ReconnectIntervalSeconds) * time.Second
	var lastErr error

	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		lastErr = p.publishOnce(ctx, q, payload, priority)
		if lastErr == nil {
			return nil
		}
		if !isRetryablePublishError(lastErr) || attempt == p.maxRetries {
			return lastErr
		}

		wait := backoff.Duration(attempt, base, 2*base)
		p.logger.Warn("rabbitmq publish retry",
			"queue", queueName,
			"attempt", attempt+1,
			"error", lastErr,
			"wait", wait,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	return lastErr
}

// InvalidateForVHost closes publisher channels for the given vhost after reconnect.
func (p *Publisher) InvalidateForVHost(vhost string) {
	if vhost == p.cfg.Connection.VHost {
		p.invalidateKind(QueueKindClassic)
	}
	if vhost == p.cfg.Connection.QuorumVHost {
		p.invalidateKind(QueueKindQuorum)
	}
}

func (p *Publisher) publishOnce(ctx context.Context, q *QueueConfig, payload []byte, priority int) error {
	ch, unlock, err := p.acquireChannel(q.QueueType)
	if err != nil {
		return err
	}
	defer unlock()

	msg := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         payload,
		Priority:     uint8(priority),
	}

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, q.Exchange, q.RoutingKey, false, false, msg)
	if err != nil {
		p.invalidateKindLocked(q.QueueType)
		return fmt.Errorf("publish to exchange=%s routing_key=%s: %w", q.Exchange, q.RoutingKey, err)
	}
	if dc == nil {
		p.invalidateKindLocked(q.QueueType)
		return ErrPublishNotConfirmed
	}

	waitCtx := ctx
	if p.confirmTimeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, p.confirmTimeout)
		defer cancel()
	}

	acked, err := dc.WaitContext(waitCtx)
	if err != nil {
		p.invalidateKindLocked(q.QueueType)
		return mapConfirmWaitError(ctx, err)
	}
	if !acked {
		p.invalidateKindLocked(q.QueueType)
		return ErrPublishNotConfirmed
	}

	return nil
}

func (p *Publisher) acquireChannel(kind QueueKind) (*amqp.Channel, func(), error) {
	switch kind {
	case QueueKindClassic:
		p.classicMu.Lock()
		ch, err := p.ensureClassicChannel()
		if err != nil {
			p.classicMu.Unlock()
			return nil, func() {}, err
		}
		return ch, func() { p.classicMu.Unlock() }, nil
	case QueueKindQuorum:
		p.quorumMu.Lock()
		ch, err := p.ensureQuorumChannel()
		if err != nil {
			p.quorumMu.Unlock()
			return nil, func() {}, err
		}
		return ch, func() { p.quorumMu.Unlock() }, nil
	default:
		return nil, func() {}, fmt.Errorf("%w: unknown queue kind %q", ErrNotConnected, kind)
	}
}

func (p *Publisher) ensureClassicChannel() (*amqp.Channel, error) {
	if p.classicCh != nil && !p.classicCh.IsClosed() {
		return p.classicCh, nil
	}
	if p.classicCh != nil {
		_ = p.classicCh.Close()
		p.classicCh = nil
	}
	ch, err := p.openConfirmChannel(QueueKindClassic)
	if err != nil {
		return nil, err
	}
	p.classicCh = ch
	return ch, nil
}

func (p *Publisher) ensureQuorumChannel() (*amqp.Channel, error) {
	if p.quorumCh != nil && !p.quorumCh.IsClosed() {
		return p.quorumCh, nil
	}
	if p.quorumCh != nil {
		_ = p.quorumCh.Close()
		p.quorumCh = nil
	}
	ch, err := p.openConfirmChannel(QueueKindQuorum)
	if err != nil {
		return nil, err
	}
	p.quorumCh = ch
	return ch, nil
}

func (p *Publisher) openConfirmChannel(kind QueueKind) (*amqp.Channel, error) {
	ch, err := p.conn.Channel(kind)
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return ch, nil
}

// invalidateKindLocked closes and clears the cached channel for kind.
// Caller must hold the corresponding classicMu or quorumMu.
func (p *Publisher) invalidateKindLocked(kind QueueKind) {
	switch kind {
	case QueueKindClassic:
		if p.classicCh != nil {
			_ = p.classicCh.Close()
			p.classicCh = nil
		}
	case QueueKindQuorum:
		if p.quorumCh != nil {
			_ = p.quorumCh.Close()
			p.quorumCh = nil
		}
	}
}

func (p *Publisher) invalidateKind(kind QueueKind) {
	switch kind {
	case QueueKindClassic:
		p.classicMu.Lock()
		p.invalidateKindLocked(QueueKindClassic)
		p.classicMu.Unlock()
	case QueueKindQuorum:
		p.quorumMu.Lock()
		p.invalidateKindLocked(QueueKindQuorum)
		p.quorumMu.Unlock()
	}
}

func validatePublishPriority(q *QueueConfig, priority int) error {
	if priority < 0 {
		return ErrInvalidPriority
	}

	switch q.QueueType {
	case QueueKindClassic:
		if priority > 0 && !q.Priority {
			return ErrPriorityNotSupported
		}
		if priority > q.MaxPriorityOrDefault() {
			return ErrInvalidPriority
		}
	case QueueKindQuorum:
		if priority > 31 {
			return ErrInvalidPriority
		}
	}

	return nil
}

// mapConfirmWaitError maps deferred-confirm wait failures to retryable publish errors
// when the failure is a confirm timeout (not parent context cancellation).
func mapConfirmWaitError(parent context.Context, err error) error {
	if err == nil {
		return nil
	}
	if parent.Err() != nil {
		return parent.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return ErrPublishNotConfirmed
	}
	return err
}

func isRetryablePublishError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPublishNotConfirmed) || errors.Is(err, ErrNotConnected) {
		return true
	}
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) {
		switch amqpErr.Code {
		case amqp.NotFound, amqp.AccessRefused, amqp.PreconditionFailed:
			return false
		default:
			return true
		}
	}
	return false
}
