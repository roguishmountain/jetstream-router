// Package ingest keeps a connection to Bluesky Jetstream open and turns its
// messages into Events. It knows nothing about routing.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second
	readLimit      = 1 << 20 // 1 MB; the library default (32 KB) is too small for some records

)

// Client is a reconnecting Jetstream subscriber.
type Client struct {
	baseURL     string
	collections []string
	logger      *slog.Logger

	cursor       atomic.Int64 // time_us of the last event handed off
	decodeErrors atomic.Int64
	reconnects   atomic.Int64
}

// NewClient creates a client for baseURL (e.g.
// "wss://jetstream2.us-east.bsky.network/subscribe"). collections are sent as
// wantedCollections so Jetstream filters on its side; empty means everything.
func NewClient(baseURL string, collections []string, logger *slog.Logger) *Client {
	return &Client{
		baseURL:     baseURL,
		collections: collections,
		logger:      logger,
	}
}

// Cursor returns the time_us of the last event handed off.
func (c *Client) Cursor() int64 { return c.cursor.Load() }

// DecodeErrors returns how many messages could not be parsed.
func (c *Client) DecodeErrors() int64 { return c.decodeErrors.Load() }

// Reconnects returns how many reconnect attempts have been made, including
// ones that failed to connect.
func (c *Client) Reconnects() int64 { return c.reconnects.Load() }

// Run streams events into out until ctx is canceled, reconnecting with
// exponential backoff whenever the connection drops. It blocks when out is
// full, which applies backpressure to the stream.
func (c *Client) Run(ctx context.Context, out chan<- Event) {
	backoff := initialBackoff
	for {
		connected, err := c.runOnce(ctx, out)
		if ctx.Err() != nil {
			return // shutting down
		}
		if connected {
			backoff = initialBackoff // had a healthy session; start over
		}
		c.reconnects.Add(1)
		wait := jitter(backoff)
		c.logger.Warn("stream disconnected", "err", err, "retry_in", wait)

		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// runOnce handles one connection from dial to disconnect. connected reports
// whether the dial succeeded, so Run knows whether to reset its backoff.
func (c *Client) runOnce(ctx context.Context, out chan<- Event) (connected bool, err error) {
	u, err := c.subscribeURL()
	if err != nil {
		return false, err
	}
	c.logger.Info("connecting", "url", u)

	conn, _, err := websocket.Dial(ctx, u, nil)
	if err != nil {
		return false, err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(readLimit)
	c.logger.Info("connected")

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return true, err
		}

		var ev Event
		if err := json.Unmarshal(data, &ev); err != nil {
			// One bad message shouldn't take the stream down.
			c.decodeErrors.Add(1)
			continue
		}

		select {
		case out <- ev:
		case <-ctx.Done():
			return true, ctx.Err()
		}
		c.cursor.Store(ev.TimeUS)
	}
}

// subscribeURL builds the connect URL with the collection filter and, after
// the first connection, a resume cursor.
func (c *Client) subscribeURL() (string, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for _, col := range c.collections {
		q.Add("wantedCollections", col)
	}
	if cur := c.cursor.Load(); cur > 0 {
		q.Set("cursor", strconv.FormatInt(cur, 10))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// jitter returns a random duration in [d/2, d) so many clients don't all
// reconnect at the same instant.
func jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d/2)
}
