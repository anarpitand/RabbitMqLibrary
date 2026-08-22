package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Delivery wraps a consumed AMQP message for application handlers.
type Delivery struct {
	QueueName   string
	Body        []byte
	ContentType string
	Priority    uint8
	MessageID   string
	Timestamp   time.Time
	RoutingKey  string
	Exchange    string
}

func newDelivery(queueName string, d amqp.Delivery) Delivery {
	return Delivery{
		QueueName:   queueName,
		Body:        d.Body,
		ContentType: d.ContentType,
		Priority:    d.Priority,
		MessageID:   d.MessageId,
		Timestamp:   d.Timestamp,
		RoutingKey:  d.RoutingKey,
		Exchange:    d.Exchange,
	}
}
