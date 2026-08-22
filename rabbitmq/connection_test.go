// Package rabbitmq_test provides black-box tests for the public rabbitmq API.
package rabbitmq_test

import (
	"context"
	"testing"

	"RabbitMqLibrary/rabbitmq"
)

func TestConnectionManagerBeforeConnect(t *testing.T) {
	cfg := validConfig()
	cfg.ApplyDefaults()

	mgr := rabbitmq.NewConnectionManager(cfg, nil, nil, nil, nil)
	if _, err := mgr.Channel(rabbitmq.QueueKindClassic); err == nil {
		t.Fatal("expected not connected before Connect")
	}
	if err := mgr.Health(context.Background()); err == nil {
		t.Fatal("expected health error before connect")
	}
}
