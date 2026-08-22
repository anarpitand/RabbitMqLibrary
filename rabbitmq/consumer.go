package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"RabbitMqLibrary/internal/backoff"
)

const (
	defaultPrefetch    = 10
	defaultConcurrency = 1
)

// Handler processes a consumed message. Return nil to ack; return an error to
// delay-retry or park on the dead-letter queue (or nack on shutdown).
type Handler func(ctx context.Context, d Delivery) error

// ConsumerOption configures a registered consumer.
type ConsumerOption func(*consumerSettings)

type consumerSettings struct {
	prefetch    int
	concurrency int
}

// WithPrefetch sets the consumer prefetch count (basic.qos).
func WithPrefetch(count int) ConsumerOption {
	return func(s *consumerSettings) {
		if count > 0 {
			s.prefetch = count
		}
	}
}

// WithConcurrency sets the number of concurrent handler goroutines.
func WithConcurrency(count int) ConsumerOption {
	return func(s *consumerSettings) {
		if count > 0 {
			s.concurrency = count
		}
	}
}

func defaultConsumerSettings() consumerSettings {
	return consumerSettings{
		prefetch:    defaultPrefetch,
		concurrency: defaultConcurrency,
	}
}

// ConsumerManager registers consumers and manages their lifecycle.
type ConsumerManager struct {
	conn   *ConnectionManager
	pub    *Publisher
	cfg    Config
	logger *slog.Logger

	mu      sync.Mutex
	runners map[string]*consumerRunner
	stopped bool

	stopOnce sync.Once
	stopCh   chan struct{}

	handlerWG sync.WaitGroup
	runnerWG  sync.WaitGroup
}

// NewConsumerManager creates a consumer manager.
func NewConsumerManager(conn *ConnectionManager, cfg Config, logger *slog.Logger, pub *Publisher) *ConsumerManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &ConsumerManager{
		conn:    conn,
		pub:     pub,
		cfg:     cfg,
		logger:  logger,
		runners: make(map[string]*consumerRunner),
		stopCh:  make(chan struct{}),
	}
}

// RegisterConsumer starts a consumer for the named queue.
func (m *ConsumerManager) RegisterConsumer(queueName string, handler Handler, opts ...ConsumerOption) error {
	if handler == nil {
		return fmt.Errorf("%w: handler is nil", ErrConfigInvalid)
	}

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return ErrConsumerStopped
	}

	q := m.cfg.QueueByName(queueName)
	if q == nil {
		m.mu.Unlock()
		return ErrQueueNotFound
	}
	if q.Role == QueueRolePublishOnly {
		m.mu.Unlock()
		return ErrPublishOnlyQueue
	}
	if _, exists := m.runners[queueName]; exists {
		m.mu.Unlock()
		return fmt.Errorf("%w: consumer already registered for queue %s", ErrConfigInvalid, queueName)
	}

	settings := defaultConsumerSettings()
	for _, opt := range opts {
		opt(&settings)
	}

	runner := &consumerRunner{
		mgr:       m,
		queueName: queueName,
		qcfg:      *q,
		handler:   handler,
		settings:  settings,
	}
	m.runners[queueName] = runner
	m.runnerWG.Add(1)
	m.mu.Unlock()

	go func() {
		defer m.runnerWG.Done()
		runner.run()
	}()

	m.logger.Info("rabbitmq consumer registered", "queue", queueName, "prefetch", settings.prefetch, "concurrency", settings.concurrency)
	return nil
}

// ResubscribeForVHost cancels active consumers on the vhost so they re-subscribe after reconnect.
func (m *ConsumerManager) ResubscribeForVHost(vhost string) {
	m.mu.Lock()
	runners := make([]*consumerRunner, 0, len(m.runners))
	for _, r := range m.runners {
		if m.vhostForQueue(r.qcfg) == vhost {
			runners = append(runners, r)
		}
	}
	m.mu.Unlock()

	for _, r := range runners {
		r.cancelConsume()
	}
}

// Stop gracefully stops all consumers and waits for in-flight handlers.
func (m *ConsumerManager) Stop() {
	m.stopOnce.Do(func() {
		m.mu.Lock()
		m.stopped = true
		runners := make([]*consumerRunner, 0, len(m.runners))
		for _, r := range m.runners {
			runners = append(runners, r)
		}
		m.mu.Unlock()

		close(m.stopCh)

		for _, r := range runners {
			r.cancelConsume()
		}

		m.runnerWG.Wait()
		m.handlerWG.Wait()
	})
}

func (m *ConsumerManager) vhostForQueue(q QueueConfig) string {
	if q.QueueType == QueueKindQuorum {
		return m.cfg.Connection.QuorumVHost
	}
	return m.cfg.Connection.VHost
}

type consumerRunner struct {
	mgr       *ConsumerManager
	queueName string
	qcfg      QueueConfig
	handler   Handler
	settings  consumerSettings

	mu          sync.Mutex
	ch          *amqp.Channel
	consumerTag string
}

func (r *consumerRunner) run() {
	base := time.Duration(r.mgr.cfg.Connection.ReconnectIntervalSeconds) * time.Second
	attempt := 0

	for {
		select {
		case <-r.mgr.stopCh:
			return
		default:
		}

		if err := r.consumeOnce(); err != nil {
			select {
			case <-r.mgr.stopCh:
				return
			default:
			}

			wait := backoff.Duration(attempt, base, 2*base)
			attempt++
			r.mgr.logger.Warn("rabbitmq consumer retry",
				"queue", r.queueName,
				"error", err,
				"wait", wait,
			)

			select {
			case <-r.mgr.stopCh:
				return
			case <-time.After(wait):
			}
			continue
		}

		attempt = 0
	}
}

func (r *consumerRunner) consumeOnce() error {
	ch, err := r.mgr.conn.Channel(r.qcfg.QueueType)
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	tag := fmt.Sprintf("%s-%d", r.queueName, time.Now().UnixNano())

	if err := ch.Qos(r.settings.prefetch, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("qos: %w", err)
	}

	deliveries, err := ch.Consume(
		r.queueName,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return fmt.Errorf("consume: %w", err)
	}

	r.mu.Lock()
	r.ch = ch
	r.consumerTag = tag
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.ch = nil
		r.consumerTag = ""
		r.mu.Unlock()
		ch.Close()
	}()

	jobs := make(chan amqp.Delivery, r.settings.concurrency)
	var workerWG sync.WaitGroup

	for i := 0; i < r.settings.concurrency; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for d := range jobs {
				r.handleDelivery(d)
			}
		}()
	}

	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		for {
			select {
			case <-r.mgr.stopCh:
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				select {
				case jobs <- d:
				case <-r.mgr.stopCh:
					// Delivery taken from broker but not handed to a worker: requeue.
					if nackErr := d.Nack(false, true); nackErr != nil {
						r.mgr.logger.Error("rabbitmq nack failed", "queue", r.queueName, "error", nackErr)
					}
					return
				}
			}
		}
	}()

	select {
	case <-dispatchDone:
	case <-r.mgr.stopCh:
		r.cancelConsume()
		<-dispatchDone
	}

	close(jobs)
	workerWG.Wait()

	select {
	case <-r.mgr.stopCh:
		return nil
	default:
		return fmt.Errorf("delivery channel closed")
	}
}

func (r *consumerRunner) handleDelivery(d amqp.Delivery) {
	r.mgr.handlerWG.Add(1)
	defer r.mgr.handlerWG.Done()

	select {
	case <-r.mgr.stopCh:
		// Never handled: requeue so shutdown does not drop messages.
		if nackErr := d.Nack(false, true); nackErr != nil {
			r.mgr.logger.Error("rabbitmq nack failed", "queue", r.queueName, "error", nackErr)
		}
		return
	default:
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-r.mgr.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	delivery := newDelivery(r.queueName, d)
	err := r.handler(ctx, delivery)
	if err != nil {
		stopping := false
		select {
		case <-r.mgr.stopCh:
			stopping = true
		default:
		}
		if !stopping && r.qcfg.DeadLetter != nil {
			if routeErr := r.routeFailedDelivery(d, err); routeErr != nil {
				r.mgr.logger.Error("rabbitmq dead letter route failed",
					"queue", r.queueName,
					"error", routeErr,
				)
				if nackErr := d.Nack(false, true); nackErr != nil {
					r.mgr.logger.Error("rabbitmq nack failed", "queue", r.queueName, "error", nackErr)
				}
			}
			return
		}
		requeue := stopping
		r.mgr.logger.Warn("rabbitmq handler error",
			"queue", r.queueName,
			"error", err,
			"requeue", requeue,
		)
		if nackErr := d.Nack(false, requeue); nackErr != nil {
			r.mgr.logger.Error("rabbitmq nack failed", "queue", r.queueName, "error", nackErr)
		}
		return
	}

	if ackErr := d.Ack(false); ackErr != nil {
		r.mgr.logger.Error("rabbitmq ack failed", "queue", r.queueName, "error", ackErr)
	}
}

// routeFailedDelivery parks or delay-retries a nacked message, then acks the original.
// ponytail: publish-then-ack can duplicate across crash; handlers must stay idempotent.
// Upgrade: broker-atomic nack+DLX if a later design can still do per-level delay.
func (r *consumerRunner) routeFailedDelivery(d amqp.Delivery, handlerErr error) error {
	if r.mgr.pub == nil {
		return fmt.Errorf("publisher not configured")
	}

	count := retryCountFromHeaders(d.Headers)
	target, nextCount := deadLetterTarget(r.qcfg, count)

	r.mgr.logger.Warn("rabbitmq handler error",
		"queue", r.queueName,
		"error", handlerErr,
		"retry_count", count,
		"target", target,
	)

	ctx := context.Background()
	msg := publishingFromDelivery(d, nextCount)
	if err := r.mgr.pub.publishToQueue(ctx, r.qcfg.QueueType, target, msg); err != nil {
		return err
	}
	if ackErr := d.Ack(false); ackErr != nil {
		return fmt.Errorf("ack after dead-letter publish: %w", ackErr)
	}
	return nil
}

func deadLetterTarget(q QueueConfig, count int) (string, int) {
	maxRetries := q.DeadLetter.MaxRetriesOrDefault()
	if count < maxRetries {
		return retryQueueName(q.Name, count), count + 1
	}
	return parkQueueName(q), count
}

func (r *consumerRunner) cancelConsume() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ch == nil || r.consumerTag == "" {
		return
	}
	if err := r.ch.Cancel(r.consumerTag, false); err != nil {
		r.mgr.logger.Warn("rabbitmq cancel consumer failed", "queue", r.queueName, "error", err)
	}
}
