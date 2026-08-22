package rabbitmq

import (
	"fmt"
	"math"

	amqp "github.com/rabbitmq/amqp091-go"
)

const retryCountHeader = "x-rmq-retry-count"

func retryQueueName(source string, level int) string {
	return fmt.Sprintf("%s.retry.%d", source, level)
}

func defaultDLQName(source string) string {
	return source + ".dlq"
}

func parkQueueName(q QueueConfig) string {
	return defaultDLQName(q.Name)
}

// retryDelayMs is the queue TTL for wait level n: min(max, initial * 2^n).
func retryDelayMs(level, initial, max int) int64 {
	if initial <= 0 {
		initial = defaultDeadLetterInitialDelayMs
	}
	if max <= 0 {
		max = defaultDeadLetterMaxDelayMs
	}
	if level < 0 {
		level = 0
	}
	d := int64(initial)
	max64 := int64(max)
	for i := 0; i < level; i++ {
		if d > math.MaxInt64/2 || d*2 > max64 {
			return max64
		}
		d *= 2
	}
	if d > max64 {
		return max64
	}
	return d
}

func retryCountFromHeaders(h amqp.Table) int {
	if h == nil {
		return 0
	}
	v, ok := h[retryCountHeader]
	if !ok || v == nil {
		return 0
	}
	n, ok := amqpInt(v)
	if !ok || n < 0 {
		return 0
	}
	return n
}

func amqpInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int8:
		return int(n), true
	case int16:
		return int(n), true
	case int32:
		return int(n), true
	case int64:
		if n > math.MaxInt || n < math.MinInt {
			return 0, false
		}
		return int(n), true
	case uint:
		if n > uint(math.MaxInt) {
			return 0, false
		}
		return int(n), true
	case uint8:
		return int(n), true
	case uint16:
		return int(n), true
	case uint32:
		if uint64(n) > uint64(math.MaxInt) {
			return 0, false
		}
		return int(n), true
	case uint64:
		if n > uint64(math.MaxInt) {
			return 0, false
		}
		return int(n), true
	default:
		return 0, false
	}
}

func publishingFromDelivery(d amqp.Delivery, retryCount int) amqp.Publishing {
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers[retryCountHeader] = int64(retryCount)
	mode := d.DeliveryMode
	if mode == 0 {
		mode = amqp.Persistent
	}
	return amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    mode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserId:          d.UserId,
		AppId:           d.AppId,
		Body:            d.Body,
	}
}
