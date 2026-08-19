package rabbitmq

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"RabbitMqLibrary/internal/backoff"
)

// DialFunc dials RabbitMQ and is injectable for tests.
type DialFunc func(cfg ConnectionConfig, vhost string) (*amqp.Connection, error)

// DisconnectHook is called when a connection closes unexpectedly.
type DisconnectHook func(vhost string, err error)

// ReconnectHook is called after a connection is re-established.
type ReconnectHook func(vhost string)

// ConnectionManager maintains classic and quorum AMQP connections.
type ConnectionManager struct {
	cfg    Config
	logger *slog.Logger

	needsClassic bool
	needsQuorum  bool

	classic *managedConnection
	quorum  *managedConnection

	onDisconnect DisconnectHook
	onReconnect  ReconnectHook

	dialFn DialFunc

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

type managedConnection struct {
	label string
	vhost string
	cfg   ConnectionConfig

	mu   sync.RWMutex
	conn *amqp.Connection

	logger *slog.Logger

	onDisconnect DisconnectHook
	onReconnect  ReconnectHook
	dialFn       DialFunc

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewConnectionManager creates a connection manager for the given config.
func NewConnectionManager(cfg Config, logger *slog.Logger, dialFn DialFunc, onDisconnect DisconnectHook, onReconnect ReconnectHook) *ConnectionManager {
	if logger == nil {
		logger = slog.Default()
	}
	if dialFn == nil {
		dialFn = defaultDial
	}

	mgr := &ConnectionManager{
		cfg:          cfg,
		logger:       logger,
		needsClassic: cfg.needsClassicConnection(),
		needsQuorum:  cfg.needsQuorumConnection(),
		onDisconnect: onDisconnect,
		onReconnect:  onReconnect,
		dialFn:       dialFn,
		stopCh:       make(chan struct{}),
	}

	if mgr.needsClassic {
		mgr.classic = &managedConnection{
			label:        "classic",
			vhost:        cfg.Connection.VHost,
			cfg:          cfg.Connection,
			logger:       logger,
			onDisconnect: onDisconnect,
			onReconnect:  onReconnect,
			dialFn:       dialFn,
			stopCh:       mgr.stopCh,
		}
	}
	if mgr.needsQuorum {
		mgr.quorum = &managedConnection{
			label:        "quorum",
			vhost:        cfg.Connection.QuorumVHost,
			cfg:          cfg.Connection,
			logger:       logger,
			onDisconnect: onDisconnect,
			onReconnect:  onReconnect,
			dialFn:       dialFn,
			stopCh:       mgr.stopCh,
		}
	}

	return mgr
}

// Connect dials all required connections and starts reconnect watchers.
func (m *ConnectionManager) Connect(ctx context.Context) error {
	if m.needsClassic {
		if err := m.classic.connect(ctx); err != nil {
			return fmt.Errorf("connect classic vhost %q: %w", m.classic.vhost, err)
		}
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.classic.watch()
		}()
	}
	if m.needsQuorum {
		if err := m.quorum.connect(ctx); err != nil {
			return fmt.Errorf("connect quorum vhost %q: %w", m.quorum.vhost, err)
		}
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			m.quorum.watch()
		}()
	}
	return nil
}

// Channel opens a new AMQP channel on the connection for the given queue kind.
func (m *ConnectionManager) Channel(kind QueueKind) (*amqp.Channel, error) {
	mc, err := m.managedForKind(kind)
	if err != nil {
		return nil, err
	}
	return mc.channel()
}

// Health verifies each active connection can open a channel.
func (m *ConnectionManager) Health(ctx context.Context) error {
	if m.needsClassic {
		if err := m.classic.health(ctx); err != nil {
			return fmt.Errorf("classic connection unhealthy: %w", err)
		}
	}
	if m.needsQuorum {
		if err := m.quorum.health(ctx); err != nil {
			return fmt.Errorf("quorum connection unhealthy: %w", err)
		}
	}
	return nil
}

// Close stops watchers and closes all connections.
func (m *ConnectionManager) Close() error {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
	m.wg.Wait()

	var firstErr error
	if m.classic != nil {
		if err := m.classic.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if m.quorum != nil {
		if err := m.quorum.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *ConnectionManager) managedForKind(kind QueueKind) (*managedConnection, error) {
	switch kind {
	case QueueKindClassic:
		if m.classic == nil {
			return nil, fmt.Errorf("%w: classic connection not configured", ErrNotConnected)
		}
		return m.classic, nil
	case QueueKindQuorum:
		if m.quorum == nil {
			return nil, fmt.Errorf("%w: quorum connection not configured", ErrNotConnected)
		}
		return m.quorum, nil
	default:
		return nil, fmt.Errorf("%w: unknown queue kind %q", ErrNotConnected, kind)
	}
}

func (mc *managedConnection) connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	conn, err := mc.dialFn(mc.cfg, mc.vhost)
	if err != nil {
		return err
	}

	mc.mu.Lock()
	mc.conn = conn
	mc.mu.Unlock()

	mc.logger.Info("rabbitmq connected", "connection", mc.label, "vhost", mc.vhost)
	return nil
}

func (mc *managedConnection) watch() {
	attempt := 0
	for {
		mc.mu.RLock()
		conn := mc.conn
		mc.mu.RUnlock()
		if conn == nil {
			return
		}

		notify := conn.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-mc.stopCh:
			return
		case err := <-notify:
			if err == nil {
				return
			}
			mc.logger.Warn("rabbitmq connection closed", "connection", mc.label, "vhost", mc.vhost, "error", err)

			if mc.onDisconnect != nil {
				mc.onDisconnect(mc.vhost, err)
			}

			if !mc.cfg.AutoReconnectOrDefault() {
				return
			}

			base := time.Duration(mc.cfg.ReconnectIntervalSeconds) * time.Second
			wait := backoff.Duration(attempt, base, 2*base)
			attempt++

			select {
			case <-mc.stopCh:
				return
			case <-time.After(wait):
			}

			if err := mc.reconnect(); err != nil {
				mc.logger.Error("rabbitmq reconnect failed", "connection", mc.label, "vhost", mc.vhost, "error", err)
				continue
			}

			attempt = 0
			if mc.onReconnect != nil {
				mc.onReconnect(mc.vhost)
			}
		}
	}
}

func (mc *managedConnection) reconnect() error {
	mc.mu.Lock()
	if mc.conn != nil {
		_ = mc.conn.Close()
		mc.conn = nil
	}
	mc.mu.Unlock()

	conn, err := mc.dialFn(mc.cfg, mc.vhost)
	if err != nil {
		return err
	}

	mc.mu.Lock()
	mc.conn = conn
	mc.mu.Unlock()

	mc.logger.Info("rabbitmq reconnected", "connection", mc.label, "vhost", mc.vhost)
	return nil
}

func (mc *managedConnection) channel() (*amqp.Channel, error) {
	mc.mu.RLock()
	conn := mc.conn
	mc.mu.RUnlock()
	if conn == nil || conn.IsClosed() {
		return nil, ErrNotConnected
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	return ch, nil
}

func (mc *managedConnection) health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ch, err := mc.channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return nil
}

func (mc *managedConnection) close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.conn == nil {
		return nil
	}
	err := mc.conn.Close()
	mc.conn = nil
	return err
}

func defaultDial(cfg ConnectionConfig, vhost string) (*amqp.Connection, error) {
	addr := buildAMQPAddr(cfg, vhost)
	config := amqp.Config{
		Vhost:      vhost,
		ChannelMax: uint16(cfg.ChannelMax),
		Heartbeat:  time.Duration(cfg.HeartbeatSeconds) * time.Second,
		Locale:     "en_US",
	}

	if cfg.UseSSL {
		tlsCfg, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		config.TLSClientConfig = tlsCfg
	}

	return amqp.DialConfig(addr, config)
}

func buildAMQPAddr(cfg ConnectionConfig, vhost string) string {
	scheme := "amqp"
	port := cfg.Port
	if cfg.UseSSL {
		scheme = "amqps"
		port = cfg.SSLPort
	}

	user := url.QueryEscape(cfg.Username)
	pass := url.QueryEscape(cfg.Password)
	host := cfg.Host
	vh := url.PathEscape(vhost)

	return fmt.Sprintf("%s://%s:%s@%s:%d/%s", scheme, user, pass, host, port, vh)
}

func buildTLSConfig(cfg ConnectionConfig) (*tls.Config, error) {
	minVersion, err := tlsVersion(cfg.SSL.MinVersion)
	if err != nil {
		return nil, err
	}

	serverName := cfg.SSL.ServerName
	if serverName == "" {
		serverName = cfg.Host
	}

	return &tls.Config{
		MinVersion:         minVersion,
		ServerName:         serverName,
		InsecureSkipVerify: cfg.SSL.InsecureSkipVerify,
	}, nil
}

func tlsVersion(version string) (uint16, error) {
	switch version {
	case "tls10":
		return tls.VersionTLS10, nil
	case "tls11":
		return tls.VersionTLS11, nil
	case "tls12":
		return tls.VersionTLS12, nil
	case "tls13":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unknown tls version %q", version)
	}
}
