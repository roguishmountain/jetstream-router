// engagement-worker handles new likes and reposts.
// For now, it prints each one
// rolling counts and threshold alerts will go in handle.
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"jetstream-router/internal/ingest"
	"jetstream-router/internal/route"
	"jetstream-router/internal/worker"
)

// engagement holds the fields shared by app.bsky.feed.like and
// app.bsky.feed.repost: a reference to the post being liked or reposted.
type engagement struct {
	Subject struct {
		URI string `json:"uri"`
	} `json:"subject"`
}

func handle(ctx context.Context, ev ingest.Event) error {
	var e engagement
	if err := json.Unmarshal(ev.Commit.Record, &e); err != nil {
		return err
	}
	fmt.Printf("ENGAGE  %s by=%s post=%s\n", ev.Commit.Collection, ev.DID, e.Subject.URI)
	return nil
}

func main() {
	worker.Run("engagement-worker", route.TopicEngagement, worker.HandlerFunc(handle))
}
