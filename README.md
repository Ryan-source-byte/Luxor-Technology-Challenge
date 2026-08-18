# About Ryan

Hello everyone, This is Ryan, and I’m thrilled to take on this challenge in Go development. In the past, I’ve primarily used Python for full-stack development, as most of my projects involve AI application development and building AI infra. However, I did experiment with Go during my student days. Before preparing for this challenge, I spent a significant amount of time self-studying advanced Go syntax. My impression is that Go shares more syntactic similarities with C language but offers superior capabilities for handling high concurrency. This is precisely why it’s favored by blockchain technology companies like Luxor. All in all, this challenge has been incredibly exciting for me, and it has been immensely helpful for the growth of my Go skills.

Another thing this project taught me is that no matter how carefully the logic is designed, the system can still turn a tiny timing detail into a full detective story @ `internal/client`  😂

# Luxor TCP Challenge

This is my Go solution for the Luxor TCP message processing challenge.

I tried to keep the implementation small and readable instead of hiding the
logic behind too many abstractions. The main flow is:

```text
client -> TCP server -> PostgreSQL
```

For the bonus path, the flow becomes:

```text
client -> TCP server -> RabbitMQ -> processor -> PostgreSQL
```

## A Few Design Choices

The challenge describes the JSON messages, but TCP itself does not know where
one message ends and the next one starts. I used NDJSON for framing: one JSON
object per line. It is simple, easy to debug with logs or netcat.

Example messages:

```json
{"id":1,"method":"authorize","params":{"username":"admin"}}
{"id":null,"method":"job","params":{"job_id":1,"server_nonce":"abc"}}
{"id":2,"method":"submit","params":{"job_id":1,"client_nonce":"def","result":"..."}}
```

The server creates one session for each TCP connection. Even if several clients
all log in as `admin`, their session state is separate:

- current `job_id`
- latest `server_nonce`
- used client nonces
- rate limit timing
- small job history

The database statistics are still grouped by username and minute, so multiple
`admin` clients will add into the same `admin` row for that minute. I think that
matches the requirement better than treating username as a unique connection id.

For authentication, I defaulted to allowing `admin`, because that is the example
username in the prompt. It can be changed with `--allowed-users`.

## Project Layout

```text
cmd/server      TCP server entry point
cmd/client      TCP client entry point
cmd/processor   RabbitMQ bonus processor

internal/protocol   shared JSON message encoding/decoding
internal/server     TCP sessions, auth, jobs, submit validation
internal/client     long-running client behavior
internal/stats      in-memory and PostgreSQL statistics recorders
internal/queue      RabbitMQ publisher/consumer
internal/proof      SHA256(server_nonce + client_nonce)
internal/nonce      random nonce generation
```

I kept `cmd/*/main.go` fairly thin on purpose. Most real logic lives in
`internal/*`, so it is easier to test and reason about.

## Requirements

- Go 1.23+
- Docker / Docker Compose
- PostgreSQL and RabbitMQ can be started through the included compose file

## Run: Direct PostgreSQL Mode

This is the basic version:

```text
client -> server -> PostgreSQL
```

Start dependencies:

```bash
docker compose up -d postgres rabbitmq
```

Start the server:

```bash
export DATABASE_URL='postgres://luxor:luxor@localhost:5432/luxor?sslmode=disable'
go run ./cmd/server
```

In another terminal, start a client:

```bash
go run ./cmd/client --username admin
```

The client should print logs like:

```text
authorized as admin
received job_id=1 server_nonce=...
submission accepted
```

Check the database:

```bash
docker exec luxorchallenge-postgres-1 psql -U luxor -d luxor -c "select * from submissions order by timestamp desc;"
```

## Run: RabbitMQ Bonus Mode

This is the async version:

```text
client -> server -> RabbitMQ -> processor -> PostgreSQL
```

Start dependencies first:

```bash
docker compose up -d postgres rabbitmq
```

Terminal 1, start the processor:

```bash
export DATABASE_URL='postgres://luxor:luxor@localhost:5432/luxor?sslmode=disable'
export AMQP_URL='amqp://guest:guest@localhost:5672/'
go run ./cmd/processor
```

Terminal 2, start the server in queue mode:

```bash
export AMQP_URL='amqp://guest:guest@localhost:5672/'
go run ./cmd/server --stats-mode queue
```

Terminal 3, start the client:

```bash
go run ./cmd/client --username admin
```

In this mode, the server does not talk to PostgreSQL directly. It only validates
the submit request and publishes an event to RabbitMQ. The processor consumes
that event and writes the minute-level stats to PostgreSQL.

## Client Submit Timing

The prompt says the client should submit at most once per second and at least
once per minute. I originally tried to submit exactly every second, but that can
hit the server rate limit because the server checks when the request arrives,
not when the client intended to send it.

So the client uses a small safety margin around the 1-second boundary. It still
submits far more often than once per minute, but avoids accidental
`Submission too frequent` errors caused by scheduler or socket timing jitter.

## Tests

In the past, when developing Python programs, I used to put all test files in a dedicated `tests/` directory. However, the Go ecosystem seems to favor placing each test file right next to the main program, so I’ve decided to follow local conventions😂.

Run the normal test suite:

```bash
go test ./...
```

These tests cover the parts I cared about most while building this:

- the SHA256 example from the prompt
- NDJSON request/event encoding
- default `admin` auth behavior
- valid submit flow
- invalid result
- missing job id
- expired job id
- duplicate nonce
- submit rate limiting
- a real local TCP server/client happy path

There is also a PostgreSQL integration test for the minute-bucket upsert. It is
skipped by default so `go test ./...` does not require Docker to be running. To
include it:

```bash
TEST_DATABASE_URL='postgres://luxor:luxor@localhost:5432/luxor?sslmode=disable' go test ./...
```

For that one, start Postgres first:

```bash
docker compose up -d postgres
```

## Useful Commands

Clear old submission rows:

```bash
docker exec luxorchallenge-postgres-1 psql -U luxor -d luxor -c "TRUNCATE TABLE submissions;"
```

Stop containers:

```bash
docker compose down
```

Stop containers and delete volumes:

```bash
docker compose down -v
```

## Database Table

The app creates this table automatically if it does not exist:

```sql
CREATE TABLE IF NOT EXISTS submissions (
    username VARCHAR(255) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    submission_count INT NOT NULL,
    PRIMARY KEY (username, timestamp)
);
```

Timestamps are stored in UTC and truncated to the minute.

## To-do List

One thing I had to skip due to time limits is adding a unique event_id to each submission, which I'd definitely implement next! Right now, the RabbitMQ processor writes to the DB and then sends an ACK. However, since RabbitMQ is 'at least once' delivery, if the app crashes right after the DB write but before the ACK, the same message gets processed again, which would mess up the final stats count. Storing an event_id in Postgres would let us easily ignore duplicate messages, ensuring our final data is 100% correct and not just successfully delivered.