// delete-worker handles deletes of any record type.
// Deletes carry no record body, only which record was removed,
// so there is nothing to decode.
package main

import (
	"context"
	"fmt"

	"jetstream-router/internal/ingest"
	"jetstream-router/internal/route"
	"jetstream-router/internal/worker"
)

func handle(ctx context.Context, ev ingest.Event) error {
	fmt.Printf("DELETE  %s by=%s rkey=%s\n", ev.Commit.Collection, ev.DID, ev.Commit.RKey)
	return nil
}

func main() {
	worker.Run("delete-worker", route.TopicDeletes, worker.HandlerFunc(handle))
}
