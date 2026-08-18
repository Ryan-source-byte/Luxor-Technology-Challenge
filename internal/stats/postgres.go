package stats

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS submissions (
	username VARCHAR(255) NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL,
	submission_count INT NOT NULL,
	PRIMARY KEY (username, timestamp)
);
`

// PostgresRecorder is the real stats writer. I keep the pool and schema setup
// here so the rest of the app can just call Record.
type PostgresRecorder struct {
	pool *pgxpool.Pool
}

// NewPostgresRecorder opens the DB and creates the table up front. Failing fast
// at startup is easier to understand than accepting client work and failing
// later.
func NewPostgresRecorder(ctx context.Context, databaseURL string) (*PostgresRecorder, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	recorder := &PostgresRecorder{pool: pool}
	if err := recorder.Init(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return recorder, nil
}

// Init creates the minute aggregation table. The primary key is exactly the
// bucket I want to upsert into: username plus minute timestamp.
func (p *PostgresRecorder) Init(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, schema)
	return err
}

// Record stores one event in the UTC minute bucket. Multiple successful submits
// in the same minute just increment the existing row.
func (p *PostgresRecorder) Record(ctx context.Context, event Event) error {
	if event.Count <= 0 {
		event.Count = 1
	}
	bucket := event.Timestamp.UTC().Truncate(time.Minute)
	_, err := p.pool.Exec(ctx, `
INSERT INTO submissions (username, timestamp, submission_count)
VALUES ($1, $2, $3)
ON CONFLICT (username, timestamp)
DO UPDATE SET submission_count = submissions.submission_count + EXCLUDED.submission_count;
`, event.Username, bucket, event.Count)
	return err
}

// Close releases the pool. The context is only here to match Recorder.
func (p *PostgresRecorder) Close(context.Context) error {
	p.pool.Close()
	return nil
}
