package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TopologyManager declares exchanges, queues, and bindings from config.
type TopologyManager struct {
	conn   *ConnectionManager
	cfg    Config
	logger *slog.Logger
}

// NewTopologyManager creates a topology manager for the given connection and config.
func NewTopologyManager(conn *ConnectionManager, cfg Config, logger *slog.Logger) *TopologyManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &TopologyManager{
		conn:   conn,
		cfg:    cfg,
		logger: logger,
	}
}

// DeclareAll declares topology for every configured queue on the correct vhost.
func (tm *TopologyManager) DeclareAll(ctx context.Context) error {
	if tm.conn.needsClassic {
		if err := tm.DeclareForKind(ctx, QueueKindClassic); err != nil {
			return err
		}
	}
	if tm.conn.needsQuorum {
		if err := tm.DeclareForKind(ctx, QueueKindQuorum); err != nil {
			return err
		}
	}
	return nil
}

// DeclareForVHost re-declares topology for queues on the given vhost after reconnect.
func (tm *TopologyManager) DeclareForVHost(ctx context.Context, vhost string) error {
	if vhost == tm.cfg.Connection.VHost {
		return tm.DeclareForKind(ctx, QueueKindClassic)
	}
	if vhost == tm.cfg.Connection.QuorumVHost {
		return tm.DeclareForKind(ctx, QueueKindQuorum)
	}
	return nil
}

// DeclareForKind declares topology for all queues of the given kind on one channel.
func (tm *TopologyManager) DeclareForKind(ctx context.Context, kind QueueKind) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ch, err := tm.conn.Channel(kind)
	if err != nil {
		return topologyError("open channel", kind, "", "", "", err)
	}
	defer ch.Close()

	for _, q := range tm.cfg.Queues {
		if q.QueueType != kind {
			continue
		}
		if err := tm.declareEntry(ctx, ch, q); err != nil {
			return err
		}
	}

	tm.logger.Info("rabbitmq topology declared", "queue_type", kind)
	return nil
}

func (tm *TopologyManager) declareEntry(ctx context.Context, ch *amqp.Channel, q QueueConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	durable := q.DurableOrDefault()

	if err := ch.ExchangeDeclare(
		q.Exchange,
		q.ExchangeType,
		durable,
		false,
		false,
		false,
		nil,
	); err != nil {
		return topologyError("declare exchange", q.QueueType, q.Exchange, q.Name, q.RoutingKey, err)
	}

	args := queueDeclareArgs(q)
	if _, err := ch.QueueDeclare(
		q.Name,
		durable,
		false,
		false,
		false,
		args,
	); err != nil {
		return topologyError("declare queue", q.QueueType, q.Exchange, q.Name, q.RoutingKey, err)
	}

	if err := ch.QueueBind(
		q.Name,
		q.RoutingKey,
		q.Exchange,
		false,
		nil,
	); err != nil {
		return topologyError("bind queue", q.QueueType, q.Exchange, q.Name, q.RoutingKey, err)
	}

	return nil
}

// queueDeclareArgs maps queue config to AMQP queue declare arguments.
func queueDeclareArgs(q QueueConfig) amqp.Table {
	switch q.QueueType {
	case QueueKindQuorum:
		return amqp.Table{"x-queue-type": "quorum"}
	case QueueKindClassic:
		if q.Priority {
			return amqp.Table{"x-max-priority": int32(q.MaxPriorityOrDefault())}
		}
	}
	return nil
}

func topologyError(step string, kind QueueKind, exchange, queue, routingKey string, err error) error {
	return fmt.Errorf(
		"%w: %s (%s exchange=%s queue=%s routing_key=%s): %v",
		ErrTopologyDeclareFailed,
		step,
		kind,
		exchange,
		queue,
		routingKey,
		err,
	)
}
