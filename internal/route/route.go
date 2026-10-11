// Package route decides which Kafka topic each event goes to. It is a pure
// function of the event so it is easy to test and reason about.
package route

import (
	"encoding/json"

	"jetstream-router/internal/ingest"
)

// One topic per downstream path. Workers subscribe to exactly one of these.
const (
	TopicPosts      = "bsky.posts"      // new posts
	TopicEngagement = "bsky.engagement" // new likes and reposts
	TopicFollows    = "bsky.follows"    // new follows
	TopicDeletes    = "bsky.deletes"    // any delete
)

// AllTopics lists every topic so the router can create them at startup.
var AllTopics = []string{TopicPosts, TopicEngagement, TopicFollows, TopicDeletes}

// Topic returns the topic ev should be published to, or "" if no path
// handles it.
//
// Deletes are checked first: a deleted like goes to the delete path, not the
// engagement path, whatever its collection.
func Topic(ev ingest.Event) string {
	if ev.Kind != ingest.KindCommit || ev.Commit == nil {
		return "" // identity and account events have no route yet
	}
	c := ev.Commit

	if c.Operation == "delete" {
		return TopicDeletes
	}
	if c.Operation != "create" {
		return "" // updates have no route yet
	}

	switch c.Collection {
	case "app.bsky.feed.post":
		return TopicPosts
	case "app.bsky.feed.like", "app.bsky.feed.repost":
		return TopicEngagement
	case "app.bsky.graph.follow":
		return TopicFollows
	}
	return ""
}

// Key returns the Kafka message key for ev. Messages with the same key go to
// the same partition, so the same worker replica sees all of them, in order.
//
// New follows are keyed by the account being followed, because the follow
// worker counts followers per account and needs every follow of an account
// in one place. This is the only reason the router looks inside a record.
// Everything else, including a follow whose record can't be read, is keyed by
// the actor.
func Key(ev ingest.Event) string {
	c := ev.Commit
	if ev.Kind == ingest.KindCommit && c != nil &&
		c.Operation == "create" && c.Collection == "app.bsky.graph.follow" {
		var f struct {
			Subject string `json:"subject"`
		}
		if err := json.Unmarshal(c.Record, &f); err == nil && f.Subject != "" {
			return f.Subject
		}
	}
	return ev.DID
}
