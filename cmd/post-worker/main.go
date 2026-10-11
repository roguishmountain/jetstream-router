// post-worker handles new posts: it logs a notification for posts in the
// configured languages that mention a configured keyword.
//
// Configuration (comma-separated):
//
//	KEYWORDS  keywords to look for (default "hockey")
//	LANGS     language tags to accept, matched exactly (default "en,en-US,en-CA")
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"

	"jetstream-router/internal/ingest"
	"jetstream-router/internal/match"
	"jetstream-router/internal/route"
	"jetstream-router/internal/worker"
)

// post holds the fields of app.bsky.feed.post we use.
type post struct {
	Text  string   `json:"text"`
	Langs []string `json:"langs"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("worker", "post-worker")

	matcher := match.New(
		strings.Split(envOr("KEYWORDS", "hockey"), ","),
		strings.Split(envOr("LANGS", "en,en-US,en-CA"), ","),
	)

	handle := func(ctx context.Context, ev ingest.Event) error {
		var p post
		if err := json.Unmarshal(ev.Commit.Record, &p); err != nil {
			return err
		}

		keyword, kind := matcher.Match(p.Text, p.Langs)
		if kind == match.None {
			return nil
		}

		logger.Info("keyword match",
			"keyword", keyword,
			"match", kind,
			"langs", p.Langs,
			// A link to the post on the Bluesky web app; profiles accept a DID
			// in place of a handle.
			"url", "https://bsky.app/profile/"+ev.DID+"/post/"+ev.Commit.RKey,
		)
		return nil
	}

	worker.Run("post-worker", route.TopicPosts, worker.HandlerFunc(handle))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
