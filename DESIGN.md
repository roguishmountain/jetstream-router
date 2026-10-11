# Design

## Goal

Consume one high-volume stream of many event types, send each type to its own downstream work, and keep those paths independent: a slow, failing or busy path must not affect the others.

## Architecture

```
Jetstream ──▶ router ──▶ Kafka: one topic per route ──▶ one consumer group per worker
```

**Router** (one replica). An ingest goroutine holds the WebSocket and decodes each event's envelope; the main loop picks a topic and partition key and publishes. A 1,000-event channel between them absorbs short hiccups. It asks Jetstream for only the collections it routes (`wantedCollections`); Jetstream can't filter by operation, so create-versus-delete is decided here.

**Workers** share one harness and differ only in a handler. Each runs 2 replicas, one per partition.
- **post-worker:** keyword match on English posts (`en`, `en-US`, `en-CA`), whole-word and substring.
- **follow-worker:** alerts when an account gains N distinct followers within a sliding window.
- **engagement-worker:** logs each new like or repost and the post it targets. Planned, not built: per-post counts of distinct likers using the same window detector, alerting on unusual traction.
- **delete-worker:** logs each delete (collection, actor, record key), including unlikes and unfollows. Planned, not built: retraction, such as withdrawing a notification for a deleted post.

## Backpressure and slow paths

- **Kafka → workers:** workers pull at their own pace, so a slow worker builds lag on its own topic and slows nothing else which matters for isolation.
- **Router → Kafka:** deliberately none. The writer is async, so a slow or down Kafka causes counted drops rather than slowing ingest; for the prototype I preferred keeping up with the live stream over completeness. Production item 2 adds real backpressure.
- **Jetstream → router:** the 1,000-event channel absorbs short hiccups. If routing fell further behind, the reader would stop reading and Jetstream might disconnect it; the reconnect resumes from the cursor, and the `lag` figure in the router's summary shows it.

## Follow-burst detection

For each followed account: how many **distinct** accounts followed it in the last 5 minutes, and did that just reach the threshold?

- **Per account:** a map of follower → latest follow time (uniqueness and count) and a queue in arrival order (cheap expiry from the front).
- **Event time, not the clock.** "Now" is the newest `time_us` seen, so catching up on a backlog replays real timing instead of raising false bursts, and tests are deterministic.
- **Fires once,** when a new follower brings the count to the threshold exactly. Redelivered follows don't change the count.
- **Memory:** expiry on each follow, plus a sweep of all accounts every 30s of event time, run inside `Add` so there's no timer goroutine or lock.
- **Partitioning** by followed account sends every follow of an account to one replica, so in-memory counts stay correct with two replicas.

## Failures and delivery guarantees

| Situation | What happens |
|---|---|
| WebSocket drops | Reconnect with backoff and jitter (1s → 30s), resuming from the exact `time_us` cursor on the same instance. |
| Router restarts | The cursor is in memory, so it restarts live: **the gap is lost**. |
| Kafka down | The async writer doesn't slow the router. Each batch fails on its first attempt and is dropped; memory stays flat. Drops are counted per topic in the 5-second summary. |
| Kafka slow | Temporary errors are retried up to 10 times, so batches queue in memory; the router's memory limit caps this. |
| Worker slow or stopped | Its lag grows; nothing else is affected. It resumes from its committed offset. |
| Bad message | Sent unchanged to `<topic>.dlq` with error and source headers, then committed. |
| Worker crashes mid-message | Commit happens after handling, so the message is redelivered. |
| New consumer group | Starts at the newest offset, so it doesn't alert on old history. |

Overall: at-least-once from Kafka to the workers, best-effort before that. Duplicates come from cursor resumes, retries, crashes and rebalances.

## Kubernetes

- One distroless, non-root image with all five binaries; each Deployment picks one with `command`.
- The router runs one replica since a rolling update would briefly run two and publish every event twice.
- An init container waits up to 5 minutes for Kafka, so pods show `Init:0/1` instead of crash-looping, then fail visibly.
- Kafka is a single-broker StatefulSet and a headless Service.

## Trade-offs

- **Async Kafka writes:** high throughput and the router never stalls, but events are lost while Kafka is down (counted and logged, not recoverable).
- **Follows keyed by followed account:** correct counts across replicas, but a very popular account makes its partition a hot spot.
- **In-memory detector state:** fast and simple, but a restart or rebalance loses up to one window, so a burst in progress can be missed.
- **Fire once at the threshold:** one alert per burst rather than a flood, but a burst that keeps growing isn't reported again.
- **New consumer groups start at the latest offset:** no alerts on old history, but events published before a worker first starts are skipped.

## Alternatives considered

- **One process with Go channels:** simplest, but one crash or memory blow-up takes down every path, and paths can't scale separately.
- **HTTP push to workers:** the router would have to buffer and retry for slow workers and would result in rebuilding a queue.
- **Other brokers** (RabbitMQ, SQS, NATS JetStream, Kinesis): I chose Kafka for independent consumer offsets (each worker reads at its own pace), keyed partitions (every event for a key reaches one consumer, which keeps the detector's in-memory counts correct) and replay if needed in the future. The others lack native per-key routing to one consumer or don't run on minikube. The cost is that Kafka is the heaviest part to run.

## Path to production

1. **Land raw events first.** A minimal ingester writes every event unchanged to `bsky.raw`, and a separate routing service fans out from it. The router can then be redeployed or fixed without a gap in ingest, routing bugs can be corrected by replaying `bsky.raw`, and the cursor becomes the last raw message Kafka accepted.
2. **No silent loss at ingest.** Synchronous, batched writes, so a slow or failed Kafka pushes back on the WebSocket reader instead of dropping events. Persist the cursor after each acknowledgement, so a restart resumes where it stopped instead of losing the gap.
3. **Durable worker state.** Keep each account's recent followers in a Redis sorted set scored by follow time. Counts survive restarts and rebalances, and partitions can be added without losing them.
4. **Observability.** Metrics for events per topic, drops, reconnects, ingest lag, consumer lag and DLQ volume, with alerts on drops, growing lag and DLQ traffic. These replace the per-event logs.
5. **Health endpoints.** `/healthz` shows only that the process is working; if it checked Jetstream or Kafka, an outage would become a restart loop. `/readyz` reflects connection state and lag, mainly to stop a bad rollout.
6. **Kafka.** At least three brokers, replication factor 3, `acks=all` and `min.insync.replicas=2`. Three is the smallest cluster that survives losing one broker, including routine restarts, with no write outage and no loss of acknowledged writes: with two, you must choose between stopping writes or keeping a single copy. Three is also the smallest controller quorum that tolerates a failure.
7. **Retries before the DLQ.** Retry temporary failures with backoff, then via a retry topic so they don't block the partition; dead-letter only permanent failures.
8. **Tuning.** The follow-burst threshold (50 distinct followers), window (5 minutes) and sweep interval (30 seconds) are starting points to tune from real traffic.
