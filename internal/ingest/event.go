package ingest

import "encoding/json"

// Event kinds Jetstream sends.
const (
	KindCommit   = "commit"   // a record was created, updated or deleted
	KindIdentity = "identity" // handle / DID document change
	KindAccount  = "account"  // account activated, deactivated, etc.
)

// Event is the envelope of one Jetstream message. Only the fields needed
// to route an event are decoded here; the record body is left raw.
type Event struct {
	DID    string  `json:"did"`
	TimeUS int64   `json:"time_us"`
	Kind   string  `json:"kind"`
	Commit *Commit `json:"commit,omitempty"` // nil unless Kind == KindCommit
}

type Commit struct {
	Rev        string `json:"rev"`
	Operation  string `json:"operation"`  // create | update | delete
	Collection string `json:"collection"` // e.g. app.bsky.feed.post
	RKey       string `json:"rkey"`       // record key; DID+collection+rkey identifies a record

	// Record is kept as raw JSON so only the handlers that need the body
	// pay to decode it. Empty for deletes.
	Record json.RawMessage `json:"record,omitempty"`
}
