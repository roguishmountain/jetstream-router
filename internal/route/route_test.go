package route

import (
	"testing"

	"jetstream-router/internal/ingest"
)

func commit(collection, operation string) ingest.Event {
	return ingest.Event{
		Kind:   ingest.KindCommit,
		Commit: &ingest.Commit{Collection: collection, Operation: operation},
	}
}

func TestTopic(t *testing.T) {
	tests := []struct {
		name string
		ev   ingest.Event
		want string
	}{
		{"new post", commit("app.bsky.feed.post", "create"), TopicPosts},
		{"new like", commit("app.bsky.feed.like", "create"), TopicEngagement},
		{"new repost", commit("app.bsky.feed.repost", "create"), TopicEngagement},
		{"new follow", commit("app.bsky.graph.follow", "create"), TopicFollows},

		// Deletes go to the delete path whatever the collection.
		{"deleted post", commit("app.bsky.feed.post", "delete"), TopicDeletes},
		{"deleted like", commit("app.bsky.feed.like", "delete"), TopicDeletes},
		{"deleted block", commit("app.bsky.graph.block", "delete"), TopicDeletes},

		// Things with no route.
		{"profile update", commit("app.bsky.actor.profile", "update"), ""},
		{"new block", commit("app.bsky.graph.block", "create"), ""},
		{"identity event", ingest.Event{Kind: ingest.KindIdentity}, ""},
		{"account event", ingest.Event{Kind: ingest.KindAccount}, ""},
		{"commit kind with no commit body", ingest.Event{Kind: ingest.KindCommit}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Topic(tt.ev); got != tt.want {
				t.Errorf("Topic() = %q, want %q", got, tt.want)
			}
		})
	}
}

// withRecord returns a commit event by actor with the given raw record.
func withRecord(actor, collection, operation, record string) ingest.Event {
	ev := commit(collection, operation)
	ev.DID = actor
	if record != "" {
		ev.Commit.Record = []byte(record)
	}
	return ev
}

func TestKey(t *testing.T) {
	const actor = "did:plc:actor"
	tests := []struct {
		name string
		ev   ingest.Event
		want string
	}{
		// Follows are keyed by the account being followed.
		{"new follow", withRecord(actor, "app.bsky.graph.follow", "create",
			`{"subject":"did:plc:target"}`), "did:plc:target"},

		// Everything else is keyed by the actor.
		{"new like", withRecord(actor, "app.bsky.feed.like", "create",
			`{"subject":{"uri":"at://did:plc:target/app.bsky.feed.post/1"}}`), actor},
		{"new post", withRecord(actor, "app.bsky.feed.post", "create", `{"text":"hi"}`), actor},
		{"deleted follow", withRecord(actor, "app.bsky.graph.follow", "delete", ""), actor},
		{"identity event", ingest.Event{Kind: ingest.KindIdentity, DID: actor}, actor},

		// A follow whose subject can't be read falls back to the actor.
		{"follow with bad record", withRecord(actor, "app.bsky.graph.follow", "create", `not json`), actor},
		{"follow with no subject", withRecord(actor, "app.bsky.graph.follow", "create", `{}`), actor},
		{"follow with no record", withRecord(actor, "app.bsky.graph.follow", "create", ""), actor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Key(tt.ev); got != tt.want {
				t.Errorf("Key() = %q, want %q", got, tt.want)
			}
		})
	}
}
