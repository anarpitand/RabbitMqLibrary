# RabbitMqLibrary Usage Guide

> This document is updated whenever a new feature is added to the library.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Connection Management](#connection-management)
- [Topology Setup](#topology-setup)
- [Publishing](#publishing)
- [Consuming](#consuming)
  - [Dead-letter retries](#dead-letter-retries)
- [Error Handling](#error-handling)
- [Graceful Shutdown](#graceful-shutdown)
- [Examples](#examples)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

## Installation

Requires **Go 1.22+** and **RabbitMQ 4.3+** (for native quorum queue priority support).

```bash
go get RabbitMqLibrary/rabbitmq
```

Local development uses the module path `RabbitMqLibrary` until a publishable GitHub path is chosen.

## Quick Start

```go
package main

import (
    "context"
    "encoding/json"
    "log"

    "RabbitMqLibrary/rabbitmq"
)

func main() {
    ctx := context.Background()

    client, err := rabbitmq.LoadClientFromFile(ctx, "config.json")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close(ctx)

    err = client.RegisterConsumer("orders.created", func(ctx context.Context, d rabbitmq.Delivery) error {
        log.Printf("received: %s", string(d.Body))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }

    payload, _ := json.Marshal(map[string]string{"id": "1"})
    if err := client.Publish(ctx, payload, "orders.created", 0); err != nil {
        log.Fatal(err)
    }
}
```

Start a local broker for development:

```bash
docker compose up -d
```

## Configuration

Configuration is JSON-first (YAML is also supported). All keys use `snake_case`.

### Top-level shape

```json
{
  "connection": { },
  "queues": [ ]
}
```

The `queues` array defines topology (exchange, queue, binding) and routing metadata. Consumer handlers are registered in Go code via `RegisterConsumer`.

### Connection fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `username` | string | `guest` | Overridable by `RABBITMQ_USERNAME` |
| `password` | string | `guest` | Overridable by `RABBITMQ_PASSWORD` |
| `host` | string | `localhost` | Overridable by `RABBITMQ_HOST` |
| `vhost` | string | `/` | Classic queues |
| `quorum_vhost` | string | — | Required when any queue has `queue_type: quorum` |
| `port` | int | `5672` | Used when `use_ssl` is false |
| `ssl_port` | int | `5671` | Used when `use_ssl` is true |
| `use_ssl` | bool | `false` | |
| `ssl.server_name` | string | same as `host` | TLS ServerName |
| `ssl.min_version` | string | `tls12` | `tls10`, `tls11`, `tls12`, or `tls13` |
| `ssl.insecure_skip_verify` | bool | `false` | Dev only |
| `channel_max` | int | `2047` | Range 1–2047 |
| `heartbeat_seconds` | int | `60` | Must be > 0 |
| `reconnect_interval_seconds` | int | `5` | Must be > 0 |
| `auto_reconnect` | bool | `true` | Re-dial on connection loss |

### Queue fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `name` | string | — | Required. Unique. Used as publish/consume key |
| `role` | string | `subscriber` | `subscriber` or `publishonly` |
| `queue_type` | string | — | Required. `classic` or `quorum` |
| `exchange` | string | same as `name` | Exchange to declare |
| `exchange_type` | string | `direct` | `direct`, `topic`, or `fanout` |
| `routing_key` | string | same as `name` | Binding and publish routing key |
| `durable` | bool | `true` | Queue and exchange durable |
| `priority` | bool | `false` | Classic only. Opt-in priority queue |
| `max_priority` | int | `10` | Classic with `priority: true`, range 1–10 |
| `dead_letter` | object | defaults applied | Optional overrides. Subscriber queues always dead-letter. See [Dead-letter retries](#dead-letter-retries) |

**Routing rule:** declare, publish, and consume for a queue always use that queue's `queue_type` to select the vhost (`vhost` for classic, `quorum_vhost` for quorum).

### Loading from code

```go
cfg := rabbitmq.Config{
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

client, err := rabbitmq.New(ctx, cfg)
```

### Loading from a file

```go
cfg, err := rabbitmq.LoadConfigFromFile("config.json")
client, err := rabbitmq.LoadClientFromFile(ctx, "config.yaml")
```

Extensions `.json`, `.yaml`, and `.yml` are supported.

### Example `config.json`

```json
{
  "connection": {
    "username": "guest",
    "password": "guest",
    "host": "localhost",
    "vhost": "/",
    "quorum_vhost": "/quorum",
    "port": 5672,
    "auto_reconnect": true
  },
  "queues": [
    {
      "name": "orders.created",
      "role": "subscriber",
      "queue_type": "quorum"
    },
    {
      "name": "orders.failed",
      "role": "subscriber",
      "queue_type": "classic",
      "priority": true,
      "max_priority": 10
    },
    {
      "name": "orders.outbound",
      "role": "publishonly",
      "queue_type": "classic"
    }
  ]
}
```

## Connection Management

The library maintains up to two AMQP connections (not a traditional pool):

- **Classic connection** — dialed to `connection.vhost` when any queue has `queue_type: classic`
- **Quorum connection** — dialed to `connection.quorum_vhost` when any queue has `queue_type: quorum`

Each connection:

- Is built from structured config fields (no raw AMQP URL in config)
- Supports TLS when `use_ssl` is true
- Opens channels on demand via the internal connection manager
- Listens for `NotifyClose` and reconnects when `auto_reconnect` is true

### Health checks

```go
if err := client.Health(ctx); err != nil {
    // connection cannot open a channel
}
```

### Hooks

```go
client, err := rabbitmq.New(ctx, cfg,
    rabbitmq.WithDisconnectHook(func(vhost string, err error) {
        log.Printf("disconnected from %s: %v", vhost, err)
    }),
    rabbitmq.WithReconnectHook(func(vhost string) {
        log.Printf("reconnected to %s", vhost)
    }),
)
```

### Graceful close

```go
err := client.Close(ctx)
```

`Close` stops reconnect watchers and closes active connections.

## Topology Setup

On startup (and after reconnect), the library declares topology for **every** queue entry in config — including `publishonly` queues. Declaration order per queue is: exchange → queue → bind.

Queues are declared on the connection selected by `queue_type`:

| `queue_type` | Connection vhost |
| --- | --- |
| `classic` | `connection.vhost` |
| `quorum` | `connection.quorum_vhost` |

No manual `ExchangeDeclare`, `QueueDeclare`, or `QueueBind` calls are required when using config-driven setup.

### Classic queues

Classic queues are durable by default. Priority is opt-in:

```json
{
  "name": "orders.failed",
  "role": "subscriber",
  "queue_type": "classic",
  "priority": true,
  "max_priority": 10
}
```

When `priority` is `true`, the library sets `x-max-priority` on declare (range 1–10, default 10).

### Quorum queues

Quorum queues are declared with `x-queue-type: quorum`. Native message priority (0–31) is supported at publish time on RabbitMQ 4.3+ — do not set `priority` or `max_priority` in config for quorum queues.

```json
{
  "name": "orders.created",
  "role": "subscriber",
  "queue_type": "quorum",
  "exchange": "orders.created",
  "exchange_type": "direct",
  "routing_key": "orders.created"
}
```

### Exchange types

Supported `exchange_type` values in v1: `direct`, `topic`, and `fanout`.

### Roles

| `role` | Topology declared | Consumer allowed |
| --- | --- | --- |
| `subscriber` | Yes | Yes |
| `publishonly` | Yes | No |

### Reconnect behavior

When a connection is restored after a disconnect, topology on that vhost is re-declared automatically before the reconnect hook runs. Classic vhost reconnect re-declares classic queues; quorum vhost reconnect re-declares quorum queues.

### Startup flow

```go
client, err := rabbitmq.New(ctx, cfg)
// Connect → declare all topology → ready for publish/consume
```

If topology declaration fails, `New` / `LoadClientFromFile` returns an error wrapping `ErrTopologyDeclareFailed`.

## Publishing

`Publish` is the single entry point for sending messages. It resolves the target exchange and routing key from config by `queueName`, selects the connection from that queue's `queue_type`, and **waits for broker confirmation** before returning.

```go
err := client.Publish(ctx, payload, "orders.created", 0)
```

### Signature

```go
func (c *Client) Publish(ctx context.Context, payload []byte, queueName string, priority int) error
```

| Parameter | Description |
| --- | --- |
| `ctx` | Cancelled or deadline exceeded during publish/confirm wait returns `ctx.Err()` |
| `payload` | Message body (e.g. JSON bytes). Must be non-empty |
| `queueName` | Must match a queue `name` in config |
| `priority` | Message priority. Classic (with `priority: true`): 0–`max_priority`. Quorum: 0–31. Use `0` for normal priority |

### Routing

Publishing always uses the configured queue entry:

1. Look up `queueName` in config → `ErrQueueNotFound` if missing
2. Publish to that entry's `exchange` + `routing_key`
3. Use the connection for that entry's `queue_type` (classic or quorum vhost)

### Confirm-before-return

Each publish:

1. Opens a confirm-enabled channel (one per vhost connection, mutex-guarded)
2. Publishes with persistent delivery mode and `Content-Type: application/json`
3. Blocks until the broker acks or nacks, or the confirm timeout elapses
4. Returns `nil` only when the broker confirms the message

On nack, confirm timeout, or transient channel errors, the library retries with backoff (default 3 retries). After retries are exhausted, returns `ErrPublishNotConfirmed`.

### Priority

```go
// Classic queue with priority: true, max_priority: 10
err = client.Publish(ctx, payload, "orders.failed", 5)

// Quorum queue — native priority 0–31 (RabbitMQ 4.3+)
err = client.Publish(ctx, payload, "orders.created", 15)
```

| Queue type | Priority rules |
| --- | --- |
| Classic, `priority: false` | Only `0` allowed; higher values return `ErrPriorityNotSupported` |
| Classic, `priority: true` | 0–`max_priority` (1–10) |
| Quorum | 0–31 always supported at publish time |

### Options

```go
client, err := rabbitmq.New(ctx, cfg,
    rabbitmq.WithConfirmTimeout(30 * time.Second),
    rabbitmq.WithPublishMaxRetries(3),
)
```

### Publish errors

| Condition | Error |
| --- | --- |
| Unknown `queueName` | `ErrQueueNotFound` |
| Empty `payload` | `ErrEmptyPayload` |
| Priority > 0 on non-priority classic queue | `ErrPriorityNotSupported` |
| Priority out of range | `ErrInvalidPriority` |
| Not connected | `ErrNotConnected` |
| Confirm timeout / nack after retries | `ErrPublishNotConfirmed` |
| Context cancelled or deadline exceeded | `ctx.Err()` |

### Example

```go
payload, err := json.Marshal(order)
if err != nil {
    return err
}

if err := client.Publish(ctx, payload, "orders.created", 0); err != nil {
    return fmt.Errorf("publish order: %w", err)
}
```

## Consuming

Register a handler for each subscriber queue. The same `name` key is used for `Publish` and `RegisterConsumer`.

```go
err := client.RegisterConsumer("orders.created", func(ctx context.Context, d rabbitmq.Delivery) error {
    log.Printf("queue=%s body=%s priority=%d", d.QueueName, string(d.Body), d.Priority)
    return nil // ack
})
```

### Handler contract

| Handler return | Action |
| --- | --- |
| `nil` | Message is acknowledged |
| non-nil error | Delay-retried, then parked on `{queue}.dlq`. Shutdown nacks with requeue. Park-queue handler errors nack without requeue. |

### Consumer options

Defaults: `prefetch=10`, `concurrency=1`.

```go
err := client.RegisterConsumer("orders.created", handler,
    rabbitmq.WithPrefetch(20),
    rabbitmq.WithConcurrency(4),
)
```

### Delivery fields

| Field | Description |
| --- | --- |
| `QueueName` | Config queue name |
| `Body` | Message payload |
| `ContentType` | AMQP content type |
| `Priority` | Message priority |
| `MessageID` | Optional message ID |
| `Timestamp` | Broker timestamp |
| `RoutingKey` | Routing key from delivery |
| `Exchange` | Exchange from delivery |

### Guards

| Condition | Error |
| --- | --- |
| Unknown `queueName` | `ErrQueueNotFound` |
| `role=publishonly` | `ErrPublishOnlyQueue` |
| Consumer manager stopped | `ErrConsumerStopped` |
| Duplicate registration | `ErrConfigInvalid` |

### Dead-letter retries

Every **subscriber** queue dead-letters on handler error (when the consumer is not shutting down). Omit `dead_letter` to use defaults. Park name is always `{name}.dlq`. There is no delay: failed messages are republished immediately to the **source queue** until `max_retries`, then parked.

```yaml
# optional override; omit the block for defaults
dead_letter:
  max_retries: 5
```

| Field | Default | Notes |
| --- | --- | --- |
| `max_retries` | `3` | Immediate redeliveries after the first failure (`0` parks immediately). Range 0–16 |

JSON config must not include `initial_delay_ms` or `max_delay_ms` (unknown fields are rejected).

To consume poison in-process, add a subscriber queue named `{source}.dlq` with the same `queue_type`. That park queue does not get its own retries. `publishonly` queues do not dead-letter.

Shutdown still requeues in-flight work.

Publish-then-ack is not atomic: a crash after the copy is confirmed and before ack can duplicate. Handlers should stay idempotent. Park queues are unbounded here; cap them with a broker policy if needed.

Do not set the `x-rmq-retry-count` header on publishes; the library owns it.

### Reconnect behavior

When a connection is restored, topology is re-declared and active consumers on that vhost cancel and re-subscribe automatically.

## Error Handling

The library exposes typed sentinel errors for common failure modes:

| Error | Meaning |
| --- | --- |
| `ErrConfigInvalid` | Configuration failed validation or invalid consumer registration |
| `ErrNotConnected` | No active AMQP connection |
| `ErrTopologyDeclareFailed` | Exchange/queue/binding declaration failed |
| `ErrQueueNotFound` | Queue name not in config |
| `ErrEmptyPayload` | Publish with empty body |
| `ErrInvalidPriority` | Priority out of allowed range |
| `ErrPriorityNotSupported` | Priority set on non-priority classic queue |
| `ErrPublishNotConfirmed` | Broker did not confirm publish after retries |
| `ErrPublishOnlyQueue` | Consumer registered on publish-only queue |
| `ErrConsumerStopped` | Consumer manager has been stopped |

Wrap and inspect with `errors.Is`:

```go
if err := client.Publish(ctx, payload, "orders.created", 0); err != nil {
    if errors.Is(err, rabbitmq.ErrQueueNotFound) {
        // handle missing queue config
    }
    return fmt.Errorf("publish failed: %w", err)
}
```

Operational errors (AMQP, network) are wrapped with context including queue, exchange, and routing key where applicable.

**Duplicate delivery:** after reconnect, unacknowledged messages may be redelivered. Dead-letter retry uses publish-then-ack and can also duplicate across a crash. Handlers should be idempotent.

## Graceful Shutdown

Call `Close` to stop consumers gracefully:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := client.Close(ctx); err != nil {
    log.Printf("close: %v", err)
}
```

Shutdown order:

1. Stop accepting new deliveries (cancel AMQP consumers)
2. Wait for in-flight handler goroutines to finish
3. Close AMQP connections

In-flight handlers are allowed to complete and ack/nack before connections close. Use a context deadline on `Close` if you need a bounded wait.

## Examples

Runnable examples are in the repository:

| Example | Description |
| --- | --- |
| [examples/basic](examples/basic) | Code-only config, single classic queue |
| [examples/config-driven](examples/config-driven) | Load `config.json`, publish and consume |

```bash
docker compose up -d
go run ./examples/basic
go run ./examples/config-driven
```

Canonical config references: [config.example.json](config.example.json) and [config.example.yaml](config.example.yaml).

## Development

### Project layout

Tests follow standard Go conventions: `*_test.go` files live in the same directory as the package they test.

```text
rabbitmq/           # public library — *.go + *_test.go + integration_test.go
internal/backoff/   # private helpers — backoff.go + backoff_test.go
examples/           # runnable sample apps
```

| Test type | Location | Package | Build tag |
| --- | --- | --- | --- |
| Unit (black-box) | `rabbitmq/*_test.go` | `rabbitmq_test` | — |
| Unit (white-box) | `rabbitmq/*_test.go` | `rabbitmq` | — |
| Integration | `rabbitmq/integration_test.go` | `rabbitmq_test` | `integration` |
| Internal unit | `internal/backoff/backoff_test.go` | `backoff` | — |

### Running tests

```bash
# Unit tests (no broker)
go test ./...

# Integration tests (requires RabbitMQ)
docker compose up -d
go test -tags=integration ./rabbitmq/...
```

## Troubleshooting

### Connection refused

Ensure RabbitMQ is running (`docker compose up -d`) and `connection.host` / `connection.port` match the broker.

### Missing quorum vhost

If any queue uses `queue_type: quorum`, set `connection.quorum_vhost` and create that vhost on the broker. The provided `docker-compose.yml` creates `/quorum` automatically.

### Config validation errors

Validation runs after defaults and environment overrides. Check duplicate queue names, invalid `queue_type`, and priority rules for classic vs quorum queues.

### Publish not confirmed

Increase `WithConfirmTimeout` or check broker disk/memory alarms. Persistent publishes require the broker to accept the message.

### Messages not consumed

Verify the queue `role` is `subscriber`, the handler is registered before publishing, and prefetch/concurrency settings are appropriate for your workload.

### Duplicate messages

Expected after reconnect if a handler did not ack before disconnect. Design handlers to be idempotent.

