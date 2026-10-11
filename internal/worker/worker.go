// Package worker is the harness every worker binary shares: read from one
// Kafka topic, decode each message, and pass it to that worker's Handler.
// Messages that can't be decoded or handled go to a dead-letter topic.
// The harness is the same everywhere; only the Handler differs.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/segmentio/kafka-go"

	"jetstream-router/internal/bus"
	"jetstream-router/internal/ingest"
)

// Handler is the one thing each worker implements.
type Handler interface {
	Handle(ctx context.Context, ev ingest.Event) error
}

// HandlerFunc lets a plain function be used as a Handler.
type HandlerFunc func(ctx context.Context, ev ingest.Event) error

func (f HandlerFunc) Handle(ctx context.Context, ev ingest.Event) error { return f(ctx, ev) }

// Run consumes topic until the process receives Ctrl+C or SIGTERM. name is
// used as the Kafka consumer group and in logs. Messages that fail go to
// "<topic>.dlq".
func Run(name, topic string, h Handler) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("worker", name)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	brokers := bus.Brokers(envOr("KAFKA_BROKERS", "localhost:9092"))
	dlqTopic := topic + ".dlq"

	// Each worker owns its DLQ topic, so it creates it. One partition is
	// plenty: dead letters should be rare.
	if err := bus.EnsureTopics(ctx, brokers[0], []string{dlqTopic}, 1); err != nil {
		logger.Error("could not create DLQ topic", "err", err, "topic", dlqTopic)
		os.Exit(1)
	}

	reader := bus.NewReader(brokers, topic, name)
	defer reader.Close()
	dlq := bus.NewDLQWriter(brokers)
	defer dlq.Close()

	logger.Info("started", "topic", topic, "dlq", dlqTopic, "brokers", brokers)

	for {
		// FetchMessage doesn't mark the message as done. That happens in
		// CommitMessages below, once it has been handled or dead-lettered.
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				logger.Info("shut down cleanly")
				return
			}
			logger.Error("read failed", "err", err)
			continue
		}

		if err := process(ctx, h, msg); err != nil {
			if ctx.Err() != nil {
				// Shutting down mid-message. It isn't committed, so it
				// will be delivered again on the next start.
				logger.Info("shut down cleanly")
				return
			}
			if dlqErr := sendToDLQ(ctx, dlq, dlqTopic, name, msg, err); dlqErr != nil {
				logger.Error("DLQ write failed, message dropped",
					"err", err, "dlq_err", dlqErr, "offset", msg.Offset, "partition", msg.Partition)
			} else {
				logger.Warn("message sent to DLQ",
					"err", err, "dlq", dlqTopic, "offset", msg.Offset, "partition", msg.Partition)
			}
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				logger.Info("shut down cleanly")
				return
			}
			logger.Error("commit failed", "err", err, "offset", msg.Offset)
		}
	}
}

// process decodes msg and passes it to the handler. Any error means the
// message could not be handled and belongs in the DLQ.
func process(ctx context.Context, h Handler, msg kafka.Message) error {
	var ev ingest.Event
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	// Every handler reads ev.Commit, so catch a missing one here rather
	// than letting a handler crash on it.
	if ev.Commit == nil {
		return errors.New("event has no commit")
	}
	return h.Handle(ctx, ev)
}

// sendToDLQ publishes the original message unchanged to the dead-letter
// topic, with headers saying where it came from and why it failed. It
// returns once Kafka has stored it.
func sendToDLQ(ctx context.Context, w *kafka.Writer, dlqTopic, worker string, msg kafka.Message, cause error) error {
	return w.WriteMessages(ctx, kafka.Message{
		Topic: dlqTopic,
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "error", Value: []byte(cause.Error())},
			{Key: "worker", Value: []byte(worker)},
			{Key: "source_topic", Value: []byte(msg.Topic)},
			{Key: "source_partition", Value: []byte(strconv.Itoa(msg.Partition))},
			{Key: "source_offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		},
	})
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
