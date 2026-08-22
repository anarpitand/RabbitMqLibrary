package rabbitmq

import "errors"

var (
	// ErrConfigInvalid indicates configuration failed validation.
	ErrConfigInvalid = errors.New("rabbitmq: invalid configuration")

	// ErrNotConnected indicates no active AMQP connection is available.
	ErrNotConnected = errors.New("rabbitmq: not connected")

	// ErrTopologyDeclareFailed indicates topology declaration failed.
	ErrTopologyDeclareFailed = errors.New("rabbitmq: topology declare failed")

	// ErrQueueNotFound indicates the queue name is not present in config.
	ErrQueueNotFound = errors.New("rabbitmq: queue not found")

	// ErrEmptyPayload indicates a publish was attempted with an empty body.
	ErrEmptyPayload = errors.New("rabbitmq: empty payload")

	// ErrInvalidPriority indicates the publish priority is out of range.
	ErrInvalidPriority = errors.New("rabbitmq: invalid priority")

	// ErrPriorityNotSupported indicates priority was set on a non-priority queue.
	ErrPriorityNotSupported = errors.New("rabbitmq: priority not supported for queue")

	// ErrPublishNotConfirmed indicates the broker did not confirm the publish.
	ErrPublishNotConfirmed = errors.New("rabbitmq: publish not confirmed")

	// ErrPublishOnlyQueue indicates a consumer was registered on a publish-only queue.
	ErrPublishOnlyQueue = errors.New("rabbitmq: publish-only queue")

	// ErrConsumerStopped indicates the consumer manager has been stopped.
	ErrConsumerStopped = errors.New("rabbitmq: consumer stopped")
)
