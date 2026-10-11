package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"jetstream-router/internal/bus"
	"jetstream-router/internal/ingest"
	"jetstream-router/internal/route"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	jetstreamURL := envOr("JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe")
	collections := strings.Split(envOr("WANTED_COLLECTIONS",
		"app.bsky.feed.post,app.bsky.feed.like,app.bsky.feed.repost,app.bsky.graph.follow"), ",")
	brokers := bus.Brokers(envOr("KAFKA_BROKERS", "localhost:9092"))

	// ctx is cancelled on Ctrl+C or when Kubernetes sends SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := bus.EnsureTopics(ctx, brokers[0], route.AllTopics, 2); err != nil {
		logger.Error("could not create Kafka topics", "err", err, "brokers", brokers)
		os.Exit(1)
	}

	// Dropped events are counted and reported in the 5-second summary rather
	// than logged one batch at a time, which floods the log while Kafka is down.
	var dropped drops
	writer := bus.NewWriter(brokers, dropped.add)
	defer writer.Close() // flushes anything still batched

	events := make(chan ingest.Event, 1000)
	client := ingest.NewClient(jetstreamURL, collections, logger)

	go func() {
		client.Run(ctx, events)
		close(events) // tells the loop below there is nothing more coming
	}()

	// Route each event to its topic, and print a summary every 5 seconds.
	routed := map[string]int{}
	unrouted := 0
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				logger.Info("shut down cleanly")
				return
			}
			topic := route.Topic(ev)
			if topic == "" {
				unrouted++
				continue
			}
			value, err := json.Marshal(ev)
			if err != nil {
				logger.Error("encode failed", "err", err)
				continue
			}
			// The writer is async, so this returns immediately. The key
			// decides the partition; see route.Key.
			err = writer.WriteMessages(ctx, kafka.Message{
				Topic: topic,
				Key:   []byte(route.Key(ev)),
				Value: value,
			})
			if err != nil {
				dropped.add(topic, 1, err)
				continue
			}
			routed[topic]++

		case <-ticker.C:
			args := []any{
				"lag", lag(client.Cursor()),
				"unrouted", unrouted,
				"decode_errors", client.DecodeErrors(),
				"reconnects", client.Reconnects(),
			}
			for topic, n := range routed {
				args = append(args, topic, n)
			}
			logger.Info("last 5s", args...)
			routed = map[string]int{}
			unrouted = 0

			if byTopic, lastErr := dropped.take(); len(byTopic) > 0 {
				args := []any{"last_err", lastErr}
				for topic, n := range byTopic {
					args = append(args, topic, n)
				}
				logger.Error("kafka publish failed, events dropped", args...)
			}

			// Stats covers the time since the last call, so the last 5s.
			ws := writer.Stats()
			if ws.Errors > 0 || ws.Retries > 0 || ws.WriteTime.Max > slowWrite {
				logger.Warn("kafka is slow or failing",
					"failed_attempts", ws.Errors,
					"retries", ws.Retries,
					"slowest_write", ws.WriteTime.Max.Round(time.Millisecond))
			}
		}
	}
}

// drops counts events Kafka didn't accept, per topic, between summaries.
// The writer reports failed batches from its own goroutines, so it's locked.
type drops struct {
	mu      sync.Mutex
	byTopic map[string]int
	lastErr error
}

func (d *drops) add(topic string, count int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.byTopic == nil {
		d.byTopic = map[string]int{}
	}
	d.byTopic[topic] += count
	d.lastErr = err
}

// take returns the counts since the last call, and the latest error, and
// starts counting again from zero.
func (d *drops) take() (map[string]int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	byTopic, lastErr := d.byTopic, d.lastErr
	d.byTopic, d.lastErr = nil, nil
	return byTopic, lastErr
}

// slowWrite is how long a single write to Kafka can take before we warn.
// Locally writes take a few milliseconds.
const slowWrite = 500 * time.Millisecond

// lag is how far behind real time the last event is.
func lag(cursorUS int64) time.Duration {
	if cursorUS == 0 {
		return 0
	}
	return time.Since(time.UnixMicro(cursorUS)).Round(time.Millisecond)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
