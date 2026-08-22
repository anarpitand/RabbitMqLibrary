# RabbitMqLibrary Features

A config-driven Go library for RabbitMQ that handles connection lifecycle, topology declaration, publishing with broker confirms, and consuming with graceful shutdown.

**Requirements:** Go 1.22+, RabbitMQ 4.3+ (for native quorum queue priority).

---

## Configuration

- **Structured config** — connection settings and queue topology defined in code or files
- **JSON and YAML** — load from `.json`, `.yaml`, or `.yml` via `LoadConfigFromFile` / `LoadClientFromFile`
- **In-code config** — build `rabbitmq.Config` directly and pass to `New`
- **Defaults** — sensible defaults for credentials, ports, heartbeat, reconnect interval, exchange names, routing keys, and durability
- **Validation** — config validated at client creation (`Validate`) with typed `ErrConfigInvalid`
- **Environment overrides** — optional overrides for `RABBITMQ_USERNAME`, `RABBITMQ_PASSWORD`, and `RABBITMQ_HOST`
- **Queue roles** — `subscriber` (publish + consume) and `publishonly` (publish only)
- **Queue types** — `classic` and `quorum`, each routed to the correct vhost automatically

### Connection settings

| Feature | Description |
| --- | --- |
| Host / port | AMQP host and port (default `5672`) |
| TLS | `use_ssl`, `ssl_port`, `server_name`, `min_version` (`tls10`–`tls13`), `insecure_skip_verify` |
| Credentials | Username and password (default `guest` / `guest`) |
| Dual vhosts | Separate `vhost` (classic) and `quorum_vhost` (quorum) |
| Channel limits | Configurable `channel_max` (1–2047) |
| Heartbeat | Configurable `heartbeat_seconds` |
| Auto-reconnect | Toggle via `auto_reconnect` (default `true`) |
| Reconnect interval | Backoff base via `reconnect_interval_seconds` |

### Queue / topology settings

| Feature | Description |
| --- | --- |
| Exchange | Per-queue `exchange`, `exchange_type` (`direct`, `topic`, `fanout`) |
| Routing key | Per-queue `routing_key` for binding and publish |
| Durability | Per-queue `durable` flag (default `true`) |
| Classic priority | Opt-in `priority` with `max_priority` (1–10, default 10) |
| Quorum queues | Declared with `x-queue-type: quorum` |
| Dead letter | Default for subscribers: retries then `{name}.dlq` |

---

## Connection Management

- **Dual connections** — separate AMQP connections for classic and quorum vhosts when needed (not a channel pool)
- **On-demand channels** — channels opened through the internal connection manager
- **Automatic reconnection** — watches `NotifyClose` and re-dials when `auto_reconnect` is enabled
- **Exponential backoff with jitter** — reconnect and publish retry timing via internal backoff helper
- **Health checks** — `client.Health(ctx)` verifies each active connection can open a channel
- **Lifecycle hooks** — `WithDisconnectHook` and `WithReconnectHook` for disconnect and reconnect events (per vhost)
- **Injectable dial** — `WithDialFunc` for tests or custom connection logic
- **Structured logging** — `WithLogger` for `log/slog` integration across connection, topology, publish, and consume

---

## Topology

- **Automatic declaration** — exchanges, queues, and bindings declared from config on startup
- **No manual AMQP setup** — no need to call `ExchangeDeclare`, `QueueDeclare`, or `QueueBind` in application code
- **Per-queue declaration** — exchange → queue → bind for every configured queue (including `publishonly`)
- **Vhost-aware** — classic queues on `connection.vhost`, quorum queues on `connection.quorum_vhost`
- **Classic priority queues** — sets `x-max-priority` when `priority: true`
- **Quorum queues** — sets `x-queue-type: quorum` on declare
- **Dead-letter wait queues** — `{name}.retry.{n}` with per-level queue TTL for every subscriber
- **Dead-letter park queue** — `{name}.dlq` (list it in `queues` to consume poison)
- **Reconnect re-declaration** — topology on the affected vhost is re-declared after reconnect before publish/consume resume
- **Startup failure handling** — topology errors during `New` / `LoadClientFromFile` wrap `ErrTopologyDeclareFailed`

---

## Publishing

- **Single publish API** — `client.Publish(ctx, payload, queueName, priority)`
- **Config-driven routing** — resolves `exchange` and `routing_key` from the queue `name` in config
- **Broker confirms** — publish waits for broker acknowledgment before returning
- **Persistent messages** — `DeliveryMode: persistent` by default
- **JSON content type** — `Content-Type: application/json` set on published messages
- **Confirm timeout** — configurable via `WithConfirmTimeout` (default 30s)
- **Automatic retries** — transient failures retried with backoff (default 3 retries via `WithPublishMaxRetries`)
- **Context-aware** — respects cancellation and deadlines on publish and confirm wait
- **Priority support** — classic (0–`max_priority` when enabled) and quorum (0–31 on RabbitMQ 4.3+)
- **Validation** — empty payload, unknown queue, invalid priority, and priority-not-supported checks before publish
- **Per-vhost confirm channels** — one confirm-enabled channel per connection kind, mutex-guarded
- **Channel invalidation on failure** — broken publisher channels are closed and reopened on retry

### Publish error types

Typed sentinel errors: `ErrQueueNotFound`, `ErrEmptyPayload`, `ErrInvalidPriority`, `ErrPriorityNotSupported`, `ErrNotConnected`, `ErrPublishNotConfirmed`, plus `ctx.Err()` for cancellation.

---

## Consuming

- **Handler-based API** — `RegisterConsumer(queueName, handler, opts...)`
- **Manual ack** — return `nil` to ack; return an error to delay-retry then park on `{queue}.dlq`
- **Prefetch (QoS)** — `WithPrefetch(count)` (default 10)
- **Concurrency** — `WithConcurrency(count)` worker goroutines per queue (default 1)
- **Dead-letter retries** — default for subscribers; override with `dead_letter` (see Usage)
- **Shutdown requeue** — messages not yet handled or interrupted during shutdown are requeued (not discarded)
- **Handler context** — handlers receive a context that is cancelled when the consumer manager stops
- **Delivery wrapper** — `Delivery` struct with `QueueName`, `Body`, `ContentType`, `Priority`, `MessageID`, `Timestamp`, `RoutingKey`, `Exchange`
- **Registration guards** — rejects nil handler, unknown queue, publish-only queue, duplicate registration, and registration after stop
- **Automatic resubscribe** — consumers on a reconnecting vhost cancel and re-subscribe automatically
- **Consumer retry loop** — failed consume attempts retry with backoff until stopped

---

## Client Lifecycle

- **Unified client** — `Client` coordinates connection, topology, publishing, and consuming
- **Startup flow** — connect → declare topology → ready for publish/consume
- **Graceful shutdown** — `Close(ctx)` stops consumers, waits for in-flight handlers, then closes connections
- **Shutdown order** — cancel AMQP consumers → wait for handler goroutines → close connections
- **Config access** — `client.Config()` returns a copy of the active configuration

---

## Error Handling

- **Typed sentinel errors** — inspectable with `errors.Is` for all common failure modes
- **Wrapped operational errors** — AMQP and network errors include queue, exchange, and routing key context where applicable
- **Idempotency guidance** — unacknowledged messages may be redelivered after reconnect; handlers should be idempotent

| Error | Meaning |
| --- | --- |
| `ErrConfigInvalid` | Invalid configuration or consumer registration |
| `ErrNotConnected` | No active AMQP connection |
| `ErrTopologyDeclareFailed` | Topology declaration failed |
| `ErrQueueNotFound` | Queue name not in config |
| `ErrEmptyPayload` | Publish with empty body |
| `ErrInvalidPriority` | Priority out of allowed range |
| `ErrPriorityNotSupported` | Priority on non-priority classic queue |
| `ErrPublishNotConfirmed` | Broker did not confirm publish after retries |
| `ErrPublishOnlyQueue` | Consumer on publish-only queue |
| `ErrConsumerStopped` | Consumer manager has been stopped |

---

## Client Options

| Option | Purpose |
| --- | --- |
| `WithLogger` | Structured logger (`slog`) |
| `WithDisconnectHook` | Callback on unexpected disconnect (per vhost) |
| `WithReconnectHook` | Callback after reconnect (per vhost) |
| `WithDialFunc` | Custom AMQP dial function (tests) |
| `WithConfirmTimeout` | Max wait for broker confirm |
| `WithPublishMaxRetries` | Publish retry count for transient failures |

## Consumer Options

| Option | Purpose |
| --- | --- |
| `WithPrefetch` | Consumer prefetch count (`basic.qos`) |
| `WithConcurrency` | Concurrent handler goroutines |

---

## Development & Testing

- **Runnable examples** — `examples/basic` (code config) and `examples/config-driven` (file config)
- **Canonical config samples** — `config.example.json` and `config.example.yaml`
- **Docker Compose** — local RabbitMQ for development and integration tests
- **Unit tests** — `*_test.go` beside source in `rabbitmq/`
- **Integration tests** — broker-backed tests with `//go:build integration` tag
- **Test injectability** — `WithDialFunc` for mocking connections in tests

---

## Related Documentation

- [USAGE.md](USAGE.md) — full usage guide with examples and troubleshooting
- [README.md](../README.md) — quick start and project layout
