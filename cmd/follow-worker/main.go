// follow-worker handles new follows: it logs an alert when an account gains
// an unusual number of distinct followers within a sliding time window.
//
// Configuration:
//
//	BURST_THRESHOLD  distinct new followers that count as a burst (default 2)
//	BURST_WINDOW     how far back to count, as a Go duration (default "10m")
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"time"

	"jetstream-router/internal/ingest"
	"jetstream-router/internal/route"
	"jetstream-router/internal/window"
	"jetstream-router/internal/worker"
)

// sweepEvery is how much event time passes between sweeps that free memory
// for accounts with no recent follows.
const sweepEvery = 30 * time.Second

// follow holds the fields of app.bsky.graph.follow we use.
type follow struct {
	Subject string `json:"subject"` // DID of the account being followed
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("worker", "follow-worker")

	threshold, err := strconv.Atoi(envOr("BURST_THRESHOLD", "2"))
	if err != nil || threshold < 1 {
		logger.Error("BURST_THRESHOLD must be a whole number of at least 1", "err", err)
		os.Exit(1)
	}
	burstWindow, err := time.ParseDuration(envOr("BURST_WINDOW", "10m"))
	if err != nil || burstWindow <= 0 {
		logger.Error("BURST_WINDOW must be a positive duration such as 5m", "err", err)
		os.Exit(1)
	}
	logger.Info("burst detection", "threshold", threshold, "window", burstWindow)

	detector := window.New(burstWindow, sweepEvery, threshold)

	handle := func(ctx context.Context, ev ingest.Event) error {
		var f follow
		if err := json.Unmarshal(ev.Commit.Record, &f); err != nil {
			return err
		}
		if f.Subject == "" {
			return errors.New("follow has no subject")
		}

		count, fire := detector.Add(f.Subject, ev.DID, ev.TimeUS)
		if fire {
			logger.Info("follow burst", "account", f.Subject, "followers", count, "window", burstWindow)
		}
		return nil
	}

	worker.Run("follow-worker", route.TopicFollows, worker.HandlerFunc(handle))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
