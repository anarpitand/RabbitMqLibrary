package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Option configures a Client.
type Option func(*clientOptions)

type clientOptions struct {
	logger            *slog.Logger
	onDisconnect      DisconnectHook
	onReconnect       ReconnectHook
	dialFn            DialFunc
	confirmTimeout    time.Duration
	publishMaxRetries int
}

// WithLogger sets the structured logger used by the client.
func WithLogger(logger *slog.Logger) Option {
	return func(o *clientOptions) {
		o.logger = logger
	}
}

// WithDisconnectHook registers a callback when a connection closes unexpectedly.
func WithDisconnectHook(hook DisconnectHook) Option {
	return func(o *clientOptions) {
		o.onDisconnect = hook
	}
}

// WithReconnectHook registers a callback after a connection is re-established.
func WithReconnectHook(hook ReconnectHook) Option {
	return func(o *clientOptions) {
		o.onReconnect = hook
	}
}

// WithDialFunc overrides the default AMQP dial function (primarily for tests).
func WithDialFunc(fn DialFunc) Option {
	return func(o *clientOptions) {
		o.dialFn = fn
	}
}

// WithConfirmTimeout sets how long Publish waits for broker confirmation.
func WithConfirmTimeout(timeout time.Duration) Option {
	return func(o *clientOptions) {
		o.confirmTimeout = timeout
	}
}

// WithPublishMaxRetries sets how many times Publish retries transient failures.
func WithPublishMaxRetries(retries int) Option {
	return func(o *clientOptions) {
		o.publishMaxRetries = retries
	}
}

// Client coordinates RabbitMQ connections, topology, publishing, and consuming.
type Client struct {
	cfg    Config
	logger *slog.Logger
	conn   *ConnectionManager
	topo   *TopologyManager
	pub    *Publisher
	cons   *ConsumerManager
}

// New creates a Client from an in-memory configuration.
func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	cfg.ApplyDefaults()
	cfg.ApplyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := defaultClientOptions()
	for _, opt := range opts {
		opt(&o)
	}

	var topo *TopologyManager
	var pub *Publisher
	var cons *ConsumerManager
	onReconnect := func(vhost string) {
		if o.onReconnect != nil {
			o.onReconnect(vhost)
		}
		if topo != nil {
			if err := topo.DeclareForVHost(context.Background(), vhost); err != nil {
				o.logger.Error("topology redeclare failed", "vhost", vhost, "error", err)
			}
		}
		if pub != nil {
			pub.InvalidateForVHost(vhost)
		}
		if cons != nil {
			cons.ResubscribeForVHost(vhost)
		}
	}

	conn := NewConnectionManager(cfg, o.logger, o.dialFn, o.onDisconnect, onReconnect)
	topo = NewTopologyManager(conn, cfg, o.logger)
	pub = NewPublisher(conn, cfg, o.logger, o.confirmTimeout, o.publishMaxRetries)
	cons = NewConsumerManager(conn, cfg, o.logger, pub)

	client := &Client{
		cfg:    cfg,
		logger: o.logger,
		conn:   conn,
		topo:   topo,
		pub:    pub,
		cons:   cons,
	}

	if err := client.conn.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	if err := client.topo.DeclareAll(ctx); err != nil {
		_ = client.conn.Close()
		return nil, fmt.Errorf("declare topology: %w", err)
	}

	return client, nil
}

// LoadClientFromFile loads configuration from a JSON or YAML file and creates a Client.
func LoadClientFromFile(ctx context.Context, path string, opts ...Option) (*Client, error) {
	cfg, err := LoadConfigFromFile(path)
	if err != nil {
		return nil, err
	}
	return New(ctx, cfg, opts...)
}

// Config returns a copy of the client configuration.
func (c *Client) Config() Config {
	return c.cfg
}

// Health checks that active AMQP connections can open channels.
func (c *Client) Health(ctx context.Context) error {
	return c.conn.Health(ctx)
}

// Publish sends a message to the queue identified by queueName and waits for broker confirmation.
func (c *Client) Publish(ctx context.Context, payload []byte, queueName string, priority int) error {
	return c.pub.Publish(ctx, payload, queueName, priority)
}

// RegisterConsumer registers a handler for the named subscriber queue.
func (c *Client) RegisterConsumer(queueName string, handler Handler, opts ...ConsumerOption) error {
	return c.cons.RegisterConsumer(queueName, handler, opts...)
}

// Close gracefully stops consumers and closes AMQP connections.
func (c *Client) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.cons != nil {
		c.cons.Stop()
	}
	return c.conn.Close()
}

func defaultClientOptions() clientOptions {
	return clientOptions{
		logger:            slog.Default(),
		publishMaxRetries: defaultPublishRetries,
	}
}
