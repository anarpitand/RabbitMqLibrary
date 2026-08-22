package rabbitmq

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRetryCountFromHeaders(t *testing.T) {
	if retryCountFromHeaders(nil) != 0 {
		t.Fatal("nil headers")
	}
	if retryCountFromHeaders(amqp.Table{}) != 0 {
		t.Fatal("missing header")
	}
	if retryCountFromHeaders(amqp.Table{retryCountHeader: int32(2)}) != 2 {
		t.Fatal("int32")
	}
	if retryCountFromHeaders(amqp.Table{retryCountHeader: int64(4)}) != 4 {
		t.Fatal("int64")
	}
	if retryCountFromHeaders(amqp.Table{retryCountHeader: "nope"}) != 0 {
		t.Fatal("invalid type")
	}
}

func TestParkName(t *testing.T) {
	q := QueueConfig{Name: "orders", DeadLetter: &DeadLetterConfig{}}
	if parkQueueName(q) != "orders.dlq" {
		t.Fatal("auto dlq")
	}
}

func TestPublishingFromDeliveryCopiesAndSetsCount(t *testing.T) {
	d := amqp.Delivery{
		Headers:     amqp.Table{"keep": "x"},
		ContentType: "text/plain",
		Priority:    3,
		MessageId:   "m1",
		Body:        []byte("hi"),
	}
	p := publishingFromDelivery(d, 2)
	if p.ContentType != "text/plain" || p.Priority != 3 || p.MessageId != "m1" {
		t.Fatalf("copy: %+v", p)
	}
	if p.Headers["keep"] != "x" {
		t.Fatal("expected copied header")
	}
	if p.Headers[retryCountHeader] != int64(2) {
		t.Fatalf("retry header: %v", p.Headers[retryCountHeader])
	}
	if d.Headers[retryCountHeader] != nil {
		t.Fatal("must not mutate original headers")
	}
}
