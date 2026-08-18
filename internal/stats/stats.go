package stats

import (
	"context"
	"sync"
	"time"
)

type Event struct {
	Username  string    `json:"username"`
	Timestamp time.Time `json:"timestamp"`
	Count     int       `json:"submission_count"`
}

// Recorder is the small boundary the TCP server depends on. Submit validation
// should not care whether stats go straight to Postgres, through RabbitMQ, or
// nowhere during a protocol-only run.
type Recorder interface {
	Record(ctx context.Context, event Event) error
	Close(ctx context.Context) error
}

// NewSubmission builds the event only after a submit has passed validation. I
// normalize to UTC early so direct mode and queue mode bucket minutes the same
// way.
func NewSubmission(username string, at time.Time) Event {
	return Event{Username: username, Timestamp: at.UTC(), Count: 1}
}

// NoopRecorder is handy when I only want to exercise the TCP protocol without
// starting Postgres or RabbitMQ.
type NoopRecorder struct{}

func (NoopRecorder) Record(context.Context, Event) error {
	return nil
}

func (NoopRecorder) Close(context.Context) error {
	return nil
}

type MemoryRecorder struct {
	mu     sync.Mutex
	counts map[string]int
}

// NewMemoryRecorder gives a tiny in-process stats sink for local checks.
func NewMemoryRecorder() *MemoryRecorder {
	return &MemoryRecorder{counts: make(map[string]int)}
}

// Record mirrors the Postgres behavior: group by username and UTC minute, then
// add the count.
func (m *MemoryRecorder) Record(_ context.Context, event Event) error {
	if event.Count <= 0 {
		event.Count = 1
	}
	bucket := event.Timestamp.UTC().Truncate(time.Minute)
	key := event.Username + "|" + bucket.Format(time.RFC3339)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[key] += event.Count
	return nil
}

func (m *MemoryRecorder) Close(context.Context) error {
	return nil
}

// Count reads one bucket back out for quick checks.
func (m *MemoryRecorder) Count(username string, bucket time.Time) int {
	key := username + "|" + bucket.UTC().Truncate(time.Minute).Format(time.RFC3339)

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[key]
}
