# One image containing all five programs: /router, /post-worker,
# /engagement-worker, /follow-worker and /delete-worker. It runs the router by
# default; choose another with `docker run --entrypoint` or, in Kubernetes,
# the container's `command`.

# ---- Build stage: compile everything with the full Go toolchain ----
FROM golang:1.27 AS build
WORKDIR /src

# Download dependencies in their own layer, so Docker reuses it from cache
# and only downloads again when go.mod or go.sum change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 makes self-contained binaries that need no C library, so they
# run on the minimal image below. -trimpath leaves local file paths out of
# the binaries. Building ./cmd/... into a directory builds every program.
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/...

# ---- Final stage: only the binaries ----
# Distroless has no shell or package manager, just CA certificates (needed
# for wss:// to Jetstream) and a non-root user to run as.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /
ENTRYPOINT ["/router"]
