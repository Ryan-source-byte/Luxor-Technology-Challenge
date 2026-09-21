# Concurrent TCP Processing Pipeline

A Go implementation of a stateful TCP message-processing service with two persistence modes: direct PostgreSQL writes or asynchronous RabbitMQ delivery.

The system focuses on the details that make stream services reliable: explicit framing, isolated connection state, replay protection, rate limiting, testable boundaries and deployable local infrastructure.

## Architecture

Direct mode:

~~~text
TCP client -> session-aware server -> PostgreSQL
~~~

Asynchronous mode:

~~~text
TCP client -> session-aware server -> RabbitMQ -> processor -> PostgreSQL
~~~

NDJSON provides message framing over TCP. Each connection owns its authorisation state, active job, server nonce, used client nonces, rate-limit timing and bounded job history. Statistics are aggregated by username and UTC minute.

## Reliability controls

- SHA-256 proof validation over server and client nonces
- per-session nonce tracking to reject replayed submissions
- job ID and expiry validation
- server-side submission rate limiting
- injectable Recorder abstraction for in-memory or PostgreSQL persistence
- RabbitMQ publisher/consumer path for asynchronous processing
- thin cmd entry points with domain logic isolated in internal packages
- unit, TCP integration and optional PostgreSQL integration tests

## Repository map

~~~text
cmd/server          TCP server
cmd/client          long-running reference client
cmd/processor       RabbitMQ consumer

internal/protocol   NDJSON request and event contracts
internal/server     auth, sessions, jobs and validation
internal/client     client state and timing behaviour
internal/stats      recorders and minute-level aggregation
internal/queue      RabbitMQ publisher and consumer
internal/proof      SHA-256 proof calculation
internal/nonce      nonce generation
~~~

## Requirements

- Go 1.23+
- Docker with Compose
- PostgreSQL and RabbitMQ, provided by docker-compose.yaml

## Run: direct PostgreSQL mode

~~~bash
docker compose up -d postgres rabbitmq
export DATABASE_URL='postgres://luxor:luxor@localhost:5432/luxor?sslmode=disable'
go run ./cmd/server
~~~

In a second terminal:

~~~bash
go run ./cmd/client --username admin
~~~

## Run: asynchronous mode

Start the processor:

~~~bash
export DATABASE_URL='postgres://luxor:luxor@localhost:5432/luxor?sslmode=disable'
export AMQP_URL='amqp://guest:guest@localhost:5672/'
go run ./cmd/processor
~~~

Start the server in queue mode:

~~~bash
export AMQP_URL='amqp://guest:guest@localhost:5672/'
go run ./cmd/server --stats-mode queue
~~~

Then start a client as above.

## Test

~~~bash
go test ./...
~~~

The default suite requires no external service. To include the PostgreSQL integration test:

~~~bash
docker compose up -d postgres
TEST_DATABASE_URL='postgres://luxor:luxor@localhost:5432/luxor?sslmode=disable' go test ./...
~~~

## Known delivery boundary

RabbitMQ provides at-least-once delivery. The current consumer writes the aggregate before acknowledging the message, so a crash between those steps can double-count a redelivery. A production version should assign every submission an event ID and enforce idempotency with a unique database constraint or inbox table.

That limitation is documented deliberately: delivery success and exactly-once business effects are different guarantees.
