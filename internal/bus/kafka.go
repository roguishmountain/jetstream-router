// Package bus holds the Kafka plumbing shared by the router and the workers.
package bus

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Brokers parses a comma-separated broker list such as "localhost:9092".
func Brokers(s string) []string {
	return strings.Split(s, ",")
}

// NewWriter returns a producer that can write to any topic
// each message names its own topic.
//
// onDropped is called when a batch could not be delivered after any
// retries, so those events are lost. It runs on the writer's own
// goroutines, so it must be safe to call concurrently and must not panic.
func NewWriter(brokers []string, onDropped func(topic string, count int, err error)) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(brokers...),

		// Messages with the same key always land on the same partition, so
		// they stay in order relative to each other.
		Balancer: &kafka.Hash{},

		// Async means WriteMessages returns immediately and messages are
		// sent in batches in the background. Without it, each call waits for
		// its batch to be acknowledged, which is far too slow for a firehose.
		// The cost: write errors are not returned to the caller.
		Async:        true,
		BatchTimeout: 50 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,

		// Called once per batch with the result. Every message in a batch
		// is for the same topic and partition.
		Completion: func(messages []kafka.Message, err error) {
			if err != nil && len(messages) > 0 {
				onDropped(messages[0].Topic, len(messages), err)
			}
		},
	}
}

// NewDLQWriter returns a synchronous producer for dead-letter messages.
// Unlike NewWriter, WriteMessages waits until Kafka has stored the message
// (or the write failed), so the caller knows whether it is safe to move on.
// Each message names its own topic.
func NewDLQWriter(brokers []string) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(brokers...),

		// Send each message straight away. With a larger batch size a
		// synchronous write would wait up to BatchTimeout for the batch to
		// fill, and dead letters are rare.
		BatchSize:    1,
		RequiredAcks: kafka.RequireOne,
	}
}

// NewReader returns a consumer for one topic. Every replica that uses the
// same groupID shares the topic's partitions, so each message is handled by
// one replica.
func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,

		// Only matters for a brand-new group: start at the newest message
		// rather than the oldest, so a new worker doesn't process (and alert
		// on) old history. An existing group always resumes from its
		// committed offset, so restarts don't skip anything.
		StartOffset: kafka.LastOffset,
	})
}

// EnsureTopics creates any topics that don't exist yet. Creating them up
// front, rather than relying on auto-creation, means we choose the partition
// count instead of getting the broker's default.
func EnsureTopics(ctx context.Context, broker string, topics []string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Topics must be created through the cluster's controller broker.
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	addr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	cconn, err := kafka.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer cconn.Close()

	configs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		configs[i] = kafka.TopicConfig{Topic: t, NumPartitions: partitions, ReplicationFactor: 1}
	}
	err = cconn.CreateTopics(configs...)
	if errors.Is(err, kafka.TopicAlreadyExists) {
		return nil
	}
	return err
}
