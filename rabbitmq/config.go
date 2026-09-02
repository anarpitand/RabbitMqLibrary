package rabbitmq

import (
	"fmt"
	"os"
	"strings"
)

// QueueRole describes how an application may use a configured queue.
type QueueRole string

const (
	QueueRoleSubscriber  QueueRole = "subscriber"
	QueueRolePublishOnly QueueRole = "publishonly"
)

// QueueKind describes the broker queue type.
type QueueKind string

const (
	QueueKindClassic QueueKind = "classic"
	QueueKindQuorum  QueueKind = "quorum"
)

// Config is the top-level library configuration loaded from code or a file.
type Config struct {
	Connection ConnectionConfig `json:"connection" yaml:"connection"`
	Queues     []QueueConfig    `json:"queues" yaml:"queues"`
}

// ConnectionConfig holds AMQP connection parameters.
type ConnectionConfig struct {
	Username                 string    `json:"username" yaml:"username"`
	Password                 string    `json:"password" yaml:"password"`
	Host                     string    `json:"host" yaml:"host"`
	VHost                    string    `json:"vhost" yaml:"vhost"`
	QuorumVHost              string    `json:"quorum_vhost" yaml:"quorum_vhost"`
	Port                     int       `json:"port" yaml:"port"`
	SSLPort                  int       `json:"ssl_port" yaml:"ssl_port"`
	UseSSL                   bool      `json:"use_ssl" yaml:"use_ssl"`
	SSL                      SSLConfig `json:"ssl" yaml:"ssl"`
	ChannelMax               int       `json:"channel_max" yaml:"channel_max"`
	HeartbeatSeconds         int       `json:"heartbeat_seconds" yaml:"heartbeat_seconds"`
	ReconnectIntervalSeconds int       `json:"reconnect_interval_seconds" yaml:"reconnect_interval_seconds"`
	AutoReconnect            *bool     `json:"auto_reconnect" yaml:"auto_reconnect"`
}

// AutoReconnectOrDefault returns whether automatic reconnection is enabled (default true).
func (c ConnectionConfig) AutoReconnectOrDefault() bool {
	if c.AutoReconnect == nil {
		return true
	}
	return *c.AutoReconnect
}

// SSLConfig holds TLS settings used when use_ssl is true.
type SSLConfig struct {
	ServerName         string `json:"server_name" yaml:"server_name"`
	MinVersion         string `json:"min_version" yaml:"min_version"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify" yaml:"insecure_skip_verify"`
}

// QueueConfig describes one exchange, queue, and binding triple.
type QueueConfig struct {
	Name         string            `json:"name" yaml:"name"`
	Role         QueueRole         `json:"role" yaml:"role"`
	QueueType    QueueKind         `json:"queue_type" yaml:"queue_type"`
	Exchange     string            `json:"exchange" yaml:"exchange"`
	ExchangeType string            `json:"exchange_type" yaml:"exchange_type"`
	RoutingKey   string            `json:"routing_key" yaml:"routing_key"`
	Durable      *bool             `json:"durable" yaml:"durable"`
	Priority     bool              `json:"priority" yaml:"priority"`
	MaxPriority  *int              `json:"max_priority" yaml:"max_priority"`
	DeadLetter   *DeadLetterConfig `json:"dead_letter,omitempty" yaml:"dead_letter,omitempty"`
}

const (
	defaultDeadLetterMaxRetries = 3
	maxDeadLetterRetries        = 16
)

// DeadLetterConfig overrides default nack retry and parking for a subscriber queue.
// Omitting it still enables dead-lettering with these defaults.
type DeadLetterConfig struct {
	MaxRetries *int `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
}

// MaxRetriesOrDefault returns immediate redeliveries before parking (default 3).
func (d DeadLetterConfig) MaxRetriesOrDefault() int {
	if d.MaxRetries != nil {
		return *d.MaxRetries
	}
	return defaultDeadLetterMaxRetries
}

// DurableOrDefault returns the effective durable flag (default true).
func (q QueueConfig) DurableOrDefault() bool {
	if q.Durable == nil {
		return true
	}
	return *q.Durable
}

// MaxPriorityOrDefault returns the effective max priority for classic priority queues.
func (q QueueConfig) MaxPriorityOrDefault() int {
	if q.MaxPriority != nil {
		return *q.MaxPriority
	}
	return 10
}

// ApplyDefaults fills unset connection and queue fields with documented defaults.
func (c *Config) ApplyDefaults() {
	if c.Connection.Username == "" {
		c.Connection.Username = "guest"
	}
	if c.Connection.Password == "" {
		c.Connection.Password = "guest"
	}
	if c.Connection.Host == "" {
		c.Connection.Host = "localhost"
	}
	if c.Connection.VHost == "" {
		c.Connection.VHost = "/"
	}
	if c.Connection.Port == 0 {
		c.Connection.Port = 5672
	}
	if c.Connection.SSLPort == 0 {
		c.Connection.SSLPort = 5671
	}
	if c.Connection.ChannelMax == 0 {
		c.Connection.ChannelMax = 2047
	}
	if c.Connection.HeartbeatSeconds == 0 {
		c.Connection.HeartbeatSeconds = 60
	}
	if c.Connection.ReconnectIntervalSeconds == 0 {
		c.Connection.ReconnectIntervalSeconds = 5
	}
	if c.Connection.SSL.ServerName == "" {
		c.Connection.SSL.ServerName = c.Connection.Host
	}
	if c.Connection.SSL.MinVersion == "" {
		c.Connection.SSL.MinVersion = "tls12"
	}

	for i := range c.Queues {
		q := &c.Queues[i]
		if q.Role == "" {
			q.Role = QueueRoleSubscriber
		}
		if q.Exchange == "" {
			q.Exchange = q.Name
		}
		if q.RoutingKey == "" {
			q.RoutingKey = q.Name
		}
		if q.ExchangeType == "" {
			q.ExchangeType = "direct"
		}
	}

	for i := range c.Queues {
		q := &c.Queues[i]
		if q.Role == QueueRolePublishOnly || c.isDeadLetterParkQueue(q.Name) {
			continue
		}
		if q.DeadLetter == nil {
			q.DeadLetter = &DeadLetterConfig{}
		}
		applyDeadLetterDefaults(q.DeadLetter)
	}
}

func applyDeadLetterDefaults(dl *DeadLetterConfig) {
	if dl.MaxRetries == nil {
		v := defaultDeadLetterMaxRetries
		dl.MaxRetries = &v
	}
}

// isDeadLetterParkQueue reports whether name is `{other}.dlq` for another configured queue.
func (c *Config) isDeadLetterParkQueue(name string) bool {
	for _, q := range c.Queues {
		if q.Role == QueueRolePublishOnly {
			continue
		}
		if q.Name != name && defaultDLQName(q.Name) == name {
			return true
		}
	}
	return false
}

// ApplyEnvOverrides applies optional environment variable overrides.
func (c *Config) ApplyEnvOverrides() {
	if v := os.Getenv("RABBITMQ_PASSWORD"); v != "" {
		c.Connection.Password = v
	}
	if v := os.Getenv("RABBITMQ_HOST"); v != "" {
		c.Connection.Host = v
	}
	if v := os.Getenv("RABBITMQ_USERNAME"); v != "" {
		c.Connection.Username = v
	}
}

// Validate checks the configuration after defaults and env overrides are applied.
func (c *Config) Validate() error {
	if err := c.validateConnection(); err != nil {
		return err
	}
	if err := c.validateQueues(); err != nil {
		return err
	}
	if err := c.validateDeadLetter(); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateConnection() error {
	if strings.TrimSpace(c.Connection.Host) == "" {
		return configError("connection.host must be non-empty")
	}
	if c.Connection.Port < 1 || c.Connection.Port > 65535 {
		return configError("connection.port must be between 1 and 65535")
	}
	if c.Connection.SSLPort < 1 || c.Connection.SSLPort > 65535 {
		return configError("connection.ssl_port must be between 1 and 65535")
	}
	if c.Connection.ChannelMax < 1 || c.Connection.ChannelMax > 2047 {
		return configError("connection.channel_max must be between 1 and 2047")
	}
	if c.Connection.HeartbeatSeconds <= 0 {
		return configError("connection.heartbeat_seconds must be greater than 0")
	}
	if c.Connection.ReconnectIntervalSeconds <= 0 {
		return configError("connection.reconnect_interval_seconds must be greater than 0")
	}
	if err := validateTLSVersion(c.Connection.SSL.MinVersion); err != nil {
		return err
	}
	if c.needsQuorumConnection() && strings.TrimSpace(c.Connection.QuorumVHost) == "" {
		return configError("connection.quorum_vhost is required when any queue has queue_type quorum")
	}
	return nil
}

func (c *Config) validateQueues() error {
	if len(c.Queues) == 0 {
		return configError("queues must not be empty")
	}

	seen := make(map[string]struct{}, len(c.Queues))
	for i, q := range c.Queues {
		prefix := fmt.Sprintf("queues[%d]", i)
		if strings.TrimSpace(q.Name) == "" {
			return configError(prefix + ".name must be non-empty")
		}
		if _, ok := seen[q.Name]; ok {
			return configError("duplicate queue name: " + q.Name)
		}
		seen[q.Name] = struct{}{}

		switch q.Role {
		case QueueRoleSubscriber, QueueRolePublishOnly:
		default:
			return configError(prefix + ".role must be subscriber or publishonly")
		}

		switch q.QueueType {
		case QueueKindClassic, QueueKindQuorum:
		default:
			return configError(prefix + ".queue_type must be classic or quorum")
		}

		switch q.ExchangeType {
		case "direct", "topic", "fanout":
		default:
			return configError(prefix + ".exchange_type must be direct, topic, or fanout")
		}

		if q.QueueType == QueueKindQuorum {
			if q.Priority {
				return configError(prefix + ": priority is not supported on quorum queues")
			}
			if q.MaxPriority != nil {
				return configError(prefix + ": max_priority is not supported on quorum queues")
			}
		}

		if q.QueueType == QueueKindClassic {
			if q.Priority {
				maxP := q.MaxPriorityOrDefault()
				if maxP < 1 || maxP > 10 {
					return configError(prefix + ".max_priority must be between 1 and 10 when priority is true")
				}
			} else if q.MaxPriority != nil {
				return configError(prefix + ": max_priority cannot be set when priority is false")
			}
		}
	}

	return nil
}

func (c *Config) validateDeadLetter() error {
	for i, q := range c.Queues {
		prefix := fmt.Sprintf("queues[%d].dead_letter", i)
		if q.Role == QueueRolePublishOnly && q.DeadLetter != nil {
			return configError(prefix + " is not allowed on publishonly queues")
		}
		if c.isDeadLetterParkQueue(q.Name) && q.DeadLetter != nil {
			return configError(prefix + " is not allowed on a dead-letter park queue")
		}
		if q.DeadLetter == nil {
			continue
		}

		maxRetries := q.DeadLetter.MaxRetriesOrDefault()
		if maxRetries < 0 || maxRetries > maxDeadLetterRetries {
			return configError(prefix + ".max_retries must be between 0 and 16")
		}

		dlq := defaultDLQName(q.Name)
		if target := c.QueueByName(dlq); target != nil && target.QueueType != q.QueueType {
			return configError(prefix + ": park queue " + dlq + " must have the same queue_type as the source")
		}
	}
	return nil
}

func validateTLSVersion(version string) error {
	switch strings.ToLower(version) {
	case "tls10", "tls11", "tls12", "tls13":
		return nil
	default:
		return configError("connection.ssl.min_version must be tls10, tls11, tls12, or tls13")
	}
}

func (c *Config) needsClassicConnection() bool {
	for _, q := range c.Queues {
		if q.QueueType == QueueKindClassic {
			return true
		}
	}
	return false
}

func (c *Config) needsQuorumConnection() bool {
	for _, q := range c.Queues {
		if q.QueueType == QueueKindQuorum {
			return true
		}
	}
	return false
}

// QueueByName returns the queue config for name, or nil if not found.
func (c *Config) QueueByName(name string) *QueueConfig {
	for i := range c.Queues {
		if c.Queues[i].Name == name {
			return &c.Queues[i]
		}
	}
	return nil
}

func configError(msg string) error {
	return fmt.Errorf("%w: %s", ErrConfigInvalid, msg)
}
