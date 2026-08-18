package server

import (
	"context"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"luxor-challenge/internal/proof"
	"luxor-challenge/internal/protocol"
	"luxor-challenge/internal/stats"
)

func TestServerAuthorizeJobAndSubmit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recorder := stats.NewMemoryRecorder()
	cfg := DefaultConfig()
	cfg.Logger = log.New(io.Discard, "", 0)
	cfg.JobInterval = time.Hour
	cfg.Now = func() time.Time {
		return time.Date(2026, 5, 7, 12, 34, 15, 0, time.UTC)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, recorder)
	go func() {
		_ = srv.Serve(ctx, ln)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	auth, err := protocol.NewRequest(1, "authorize", protocol.AuthorizeParams{Username: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(auth); err != nil {
		t.Fatal(err)
	}

	authResp, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if !protocol.BoolValue(authResp.Result) {
		t.Fatalf("authorization failed: %s", authResp.Error)
	}

	jobMsg, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	job, err := protocol.DecodeParams[protocol.JobParams](jobMsg)
	if err != nil {
		t.Fatal(err)
	}
	if job.JobID != 1 || job.ServerNonce == "" {
		t.Fatalf("unexpected job: %+v", job)
	}

	submit, err := protocol.NewRequest(2, "submit", protocol.SubmitParams{
		JobID:       job.JobID,
		ClientNonce: "client",
		Result:      proof.Result(job.ServerNonce, "client"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(submit); err != nil {
		t.Fatal(err)
	}

	submitResp, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if !protocol.BoolValue(submitResp.Result) {
		t.Fatalf("submit failed: %s", submitResp.Error)
	}

	bucket := time.Date(2026, 5, 7, 12, 34, 0, 0, time.UTC)
	if got := recorder.Count("admin", bucket); got != 1 {
		t.Fatalf("recorded submissions = %d, want 1", got)
	}
}
