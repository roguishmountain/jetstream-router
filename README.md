# jetstream-router

A Go service that reads the live Bluesky firehose ([Jetstream](https://github.com/bluesky-social/jetstream)), works out what each event is, and routes it through Kafka to independent workers.

```
                                         Kafka (one topic per route)
Jetstream ──WebSocket──▶ router ──┬──▶ bsky.posts       ──▶ post-worker ×2        keyword + language match → notification
  (filtered to the                ├──▶ bsky.engagement  ──▶ engagement-worker ×2  likes / reposts (logged)
   collections we route)          ├──▶ bsky.follows     ──▶ follow-worker ×2      follow-burst detection → alert
                                  └──▶ bsky.deletes     ──▶ delete-worker ×2      retraction path (logged)
                                                               │
                                                               └──▶ <topic>.dlq   messages a worker couldn't handle
```

Each worker is its own Kubernetes Deployment with its own Kafka consumer group, so the paths run, fail and scale independently. See [DESIGN.md](DESIGN.md) for the reasoning.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/), installed **and running**
- [minikube](https://minikube.sigs.k8s.io/docs/start/) and `kubectl`
- Go 1.23+ (only needed to run the tests)

## Run it on minikube

All commands are run from the repository root.

**Shortcut on Windows:** with Docker Desktop running, `scripts/setup.ps1` does steps 1–6 below: it starts minikube if needed, builds and loads the image, deploys, and waits until every pod is ready. Run it again after a code change to rebuild and restart the pods onto the new image.
```powershell
powershell -ExecutionPolicy Bypass -File scripts\setup.ps1
```

1. Start minikube. This creates the cluster if it doesn't exist yet and points `kubectl` at it.
   ```bash
   minikube start --driver=docker
   ```

2. Build the image (one image containing all five programs):
   ```bash
   docker build -t jetstream-router:0.1 .
   ```

3. Load it into minikube, which can't see Docker Desktop's images:
   ```bash
   minikube image load jetstream-router:0.1
   ```

4. Optional, recommended on a new cluster: download Kafka and the small helper image ahead of time. Kafka's image is several hundred MB, and fetching it first makes the first start faster and smoother.
   ```bash
   minikube image pull apache/kafka:4.3.1
   ```
   ```bash
   minikube image pull busybox:1.36
   ```

5. Deploy everything:
   ```bash
   kubectl apply -f k8s/
   ```

6. Watch the pods start (Ctrl+C to stop watching):
   ```bash
   kubectl get pods -n jetstream-router -w
   ```
   Expect 10 pods: `kafka-0`, `router`, and two of each worker. `kafka-0` starts first and takes the longest. Meanwhile the router and workers show `Init:0/1`: an init container is waiting for Kafka, then they switch to `Running`. A first start can take a few minutes.

   If Kafka takes longer than 5 minutes to come up (usually because its image is still downloading), the waiting pods show `Init:Error` or `Init:CrashLoopBackOff`. That resolves on its own: Kubernetes retries them, and they start once Kafka is up.

## See it working

Router: a summary every 5 seconds of events routed per topic, lag behind real time, and reconnects. If Kafka didn't accept some events, a `kafka publish failed, events dropped` line follows it with the count per topic.
```bash
kubectl logs -n jetstream-router deploy/router -f
```

Post-worker: a `keyword match` line for English posts mentioning "hockey". The post text is never logged; the `url` links to the post on bsky.app.
```bash
kubectl logs -n jetstream-router -l app=post-worker -f --prefix
```

Follow-worker: a `follow burst` line when an account gains 50 distinct followers within 5 minutes. These are rare; see demo mode below.
```bash
kubectl logs -n jetstream-router -l app=follow-worker -f --prefix
```

Engagement and delete workers log every event they receive.
```bash
kubectl logs -n jetstream-router -l app=delete-worker -f --prefix
```

Each worker's two replicas split their topic's two partitions. To see which pod owns which partition, and how far behind each is (`LAG`):
```bash
kubectl exec -n jetstream-router kafka-0 -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group follow-worker
```

### Demo mode: frequent burst alerts

To lower the threshold for the follow-worker (this restarts its pods):
```bash
kubectl set env -n jetstream-router deploy/follow-worker BURST_THRESHOLD=2 BURST_WINDOW=10m
```

### Clean up

Remove the app, including Kafka and its data, and keep the cluster:
```bash
kubectl delete namespace jetstream-router
```

Or delete the whole minikube cluster, which also frees the memory and disk it uses:
```bash
minikube delete
```

## Configuration

Set through the `jetstream-router` ConfigMap ([k8s/10-config.yaml](k8s/10-config.yaml)) in the cluster, or as environment variables when running locally.

| Variable | Used by | Default (local) | In the cluster |
|---|---|---|---|
| `JETSTREAM_URL` | router | `wss://jetstream2.us-east.bsky.network/subscribe` | default |
| `WANTED_COLLECTIONS` | router | `app.bsky.feed.post,app.bsky.feed.like,app.bsky.feed.repost,app.bsky.graph.follow` | default |
| `KAFKA_BROKERS` | all | `localhost:9092` | `kafka:9092` |
| `KEYWORDS` | post-worker | `hockey` | same |
| `LANGS` | post-worker | `en,en-US,en-CA` (exact tags) | same |
| `BURST_THRESHOLD` | follow-worker | `2` | `50` |
| `BURST_WINDOW` | follow-worker | `10m` | `5m` |

The local defaults for the burst detector are deliberately low so alerts appear quickly during development.

## Tests

```bash
go test ./...
```

The tests cover the core logic, which is kept free of I/O so it can be tested directly:
- `internal/route`: which topic each event type goes to, and its partition key
- `internal/match`: keyword and language matching
- `internal/window`: the sliding-window burst detector (uniqueness, expiry, firing once, memory sweep), driven by fake timestamps

## Project layout

```
cmd/
  router/             Jetstream → Kafka
  post-worker/        keyword + language matching
  engagement-worker/  likes and reposts
  follow-worker/      follow-burst detection
  delete-worker/      deletes of any record type
internal/
  ingest/   reconnecting Jetstream client
  route/    event → topic and partition key
  bus/      Kafka writers, readers and topic setup
  worker/   shared consume loop with dead-letter handling
  match/    keyword / language matcher
  window/   sliding-window distinct counter
k8s/        Kubernetes manifests, applied in file-name order
```

## Assumptions

- **Missed or duplicated events are acceptable** for this prototype. Where each can happen, and how production would close the gaps, is in [DESIGN.md](DESIGN.md).
- **Only English posts are matched,** identified by their `langs` tags: `en`, `en-US` or `en-CA` exactly. Untagged posts are skipped.
- **Unfollows are not seen by the follow-worker:** every delete goes to the delete path.
- **The router must run as a single replica.** A second one would open its own Jetstream connection and publish every event twice.
- **Kafka is a single broker** with replication factor 1. Fine locally; not for production.
- **Post text is never logged,** since the firehose is unmoderated.
- **Times are Jetstream's timestamps** (`time_us`), not the local clock. The router's `lag` figure compares them with the local clock, so it is only as accurate as that clock.
