# RabbitMqLibrary

A config-driven Go library for RabbitMQ that handles connection lifecycle, topology declaration, publishing with confirms, and consuming with graceful shutdown.

Requires **Go 1.22+** and **RabbitMQ 4.3+** (for native quorum queue priority).

## Install

```bash
go get RabbitMqLibrary/rabbitmq
```

## Quick start

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

    client, err := rabbitmq.New(ctx, rabbitmq.Config{
        Connection: rabbitmq.ConnectionConfig{
            Host:        "localhost",
            VHost:       "/",
            QuorumVHost: "/quorum",
        },
        Queues: []rabbitmq.QueueConfig{
            {Name: "orders.created", QueueType: rabbitmq.QueueKindQuorum},
        },
    })
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

Start a local broker:

```bash
docker compose up -d
```

## Documentation

Full usage guide: [docs/USAGE.md](docs/USAGE.md)

## Examples

- [examples/basic](examples/basic) — minimal publish and consume
- [examples/config-driven](examples/config-driven) — load `config.json` and run

## Project layout

Standard Go conventions: library source and tests live together per package.

```text
RabbitMqLibrary/
├── rabbitmq/                 # public library package
│   ├── *.go                  # source
│   ├── *_test.go             # unit tests (same directory as source)
│   └── integration_test.go   # integration tests (//go:build integration)
├── internal/backoff/         # private helpers + backoff_test.go
├── examples/                 # runnable sample apps
├── docs/USAGE.md             # full usage guide
├── config.example.json       # canonical config reference
├── config.example.yaml
└── docker-compose.yml        # local RabbitMQ for dev/integration tests
```

**Testing conventions**

| File pattern | Package | Purpose |
| --- | --- | --- |
| `*_test.go` | `rabbitmq` or `rabbitmq_test` | Unit tests beside source in `rabbitmq/` |
| `integration_test.go` | `rabbitmq_test` + `//go:build integration` | Broker-backed tests |

Unit tests use `package rabbitmq` when testing unexported helpers; `package rabbitmq_test` for black-box API tests. Both patterns are standard in Go.

## Development

```bash
go test ./...
go test -tags=integration ./rabbitmq/...
```
