package stats

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPostgresRecorderUpsertsMinuteBuckets(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recorder, err := NewPostgresRecorder(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer recorder.Close(context.Background())

	username := "test-postgres-recorder"
	bucket := time.Date(2026, 5, 7, 12, 45, 0, 0, time.UTC)
	if _, err := recorder.pool.Exec(ctx, `DELETE FROM submissions WHERE username = $1`, username); err != nil {
		t.Fatal(err)
	}

	if err := recorder.Record(ctx, Event{Username: username, Timestamp: bucket.Add(10 * time.Second), Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Record(ctx, Event{Username: username, Timestamp: bucket.Add(30 * time.Second), Count: 1}); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := recorder.pool.QueryRow(ctx, `
SELECT submission_count FROM submissions WHERE username = $1 AND timestamp = $2
`, username, bucket).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("submission_count = %d, want 2", count)
	}
}
