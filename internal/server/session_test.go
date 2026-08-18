package server

import (
	"io"
	"log"
	"net"
	"testing"
	"time"

	"luxor-challenge/internal/proof"
	"luxor-challenge/internal/protocol"
	"luxor-challenge/internal/stats"
)

func TestValidateSubmitCoversExpectedErrors(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	cfg := DefaultConfig()
	cfg.Logger = log.New(io.Discard, "", 0)
	cfg.Now = func() time.Time { return now }
	cfg.SubmitMinInterval = time.Second
	srv := New(cfg, stats.NewMemoryRecorder())
	session := newSession(srv, 1, noopConn{})
	session.username = "admin"
	session.authenticated = true
	session.rememberJob(1, "123")

	valid := protocol.SubmitParams{
		JobID:       1,
		ClientNonce: "456",
		Result:      proof.Result("123", "456"),
	}
	if _, _, errMessage := session.validateSubmit(valid); errMessage != "" {
		t.Fatalf("valid submit error = %q", errMessage)
	}

	duplicate := valid
	if _, _, errMessage := session.validateSubmit(duplicate); errMessage != errDuplicate {
		t.Fatalf("duplicate error = %q, want %q", errMessage, errDuplicate)
	}

	now = now.Add(time.Second)
	badResult := protocol.SubmitParams{JobID: 1, ClientNonce: "789", Result: "bad"}
	if _, _, errMessage := session.validateSubmit(badResult); errMessage != errInvalidResult {
		t.Fatalf("bad result error = %q, want %q", errMessage, errInvalidResult)
	}

	now = now.Add(time.Second)
	missing := protocol.SubmitParams{JobID: 99, ClientNonce: "999", Result: "bad"}
	if _, _, errMessage := session.validateSubmit(missing); errMessage != errTaskMissing {
		t.Fatalf("missing job error = %q, want %q", errMessage, errTaskMissing)
	}
}

func TestAuthorizeRejectsUnknownUser(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Logger = log.New(io.Discard, "", 0)
	srv := New(cfg, stats.NewMemoryRecorder())

	if !srv.isAllowed("admin") {
		t.Fatal("admin should be allowed by default")
	}
	if srv.isAllowed("not-admin") {
		t.Fatal("unexpected user should be rejected")
	}
}

func TestValidateSubmitRateLimit(t *testing.T) {
	now := time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
	cfg := DefaultConfig()
	cfg.Logger = log.New(io.Discard, "", 0)
	cfg.Now = func() time.Time { return now }
	cfg.SubmitMinInterval = time.Second
	srv := New(cfg, stats.NewMemoryRecorder())
	session := newSession(srv, 1, noopConn{})
	session.username = "admin"
	session.authenticated = true
	session.rememberJob(1, "123")

	first := protocol.SubmitParams{JobID: 1, ClientNonce: "a", Result: proof.Result("123", "a")}
	if _, _, errMessage := session.validateSubmit(first); errMessage != "" {
		t.Fatalf("first submit error = %q", errMessage)
	}

	second := protocol.SubmitParams{JobID: 1, ClientNonce: "b", Result: proof.Result("123", "b")}
	if _, _, errMessage := session.validateSubmit(second); errMessage != errRateLimit {
		t.Fatalf("rate limit error = %q, want %q", errMessage, errRateLimit)
	}
}

func TestValidateSubmitExpiredJob(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Logger = log.New(io.Discard, "", 0)
	cfg.JobHistoryLimit = 1
	srv := New(cfg, stats.NewMemoryRecorder())
	session := newSession(srv, 1, noopConn{})
	session.username = "admin"
	session.authenticated = true
	session.rememberJob(1, "old")
	session.rememberJob(2, "new")

	params := protocol.SubmitParams{JobID: 1, ClientNonce: "nonce", Result: proof.Result("old", "nonce")}
	if _, _, errMessage := session.validateSubmit(params); errMessage != errTaskExpired {
		t.Fatalf("expired job error = %q, want %q", errMessage, errTaskExpired)
	}
}

type noopConn struct{}

func (noopConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (noopConn) Write(p []byte) (int, error)      { return len(p), nil }
func (noopConn) Close() error                     { return nil }
func (noopConn) LocalAddr() net.Addr              { return netAddr("local") }
func (noopConn) RemoteAddr() net.Addr             { return netAddr("remote") }
func (noopConn) SetDeadline(time.Time) error      { return nil }
func (noopConn) SetReadDeadline(time.Time) error  { return nil }
func (noopConn) SetWriteDeadline(time.Time) error { return nil }

type netAddr string

func (n netAddr) Network() string { return string(n) }
func (n netAddr) String() string  { return string(n) }
