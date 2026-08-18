package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"luxor-challenge/internal/stats"
)

type Server struct {
	cfg      Config
	recorder stats.Recorder

	mu       sync.Mutex
	sessions map[*Session]struct{}
	nextID   atomic.Int64
}

// New wires the TCP server to whatever stats path we want to use. Keeping that
// behind Recorder lets the same session code work for direct DB writes, queue
// mode, or a no-op local run.
func New(cfg Config, recorder stats.Recorder) *Server {
	cfg = cfg.withDefaults()
	if recorder == nil {
		recorder = stats.NoopRecorder{}
	}

	return &Server{
		cfg:      cfg,
		recorder: recorder,
		sessions: make(map[*Session]struct{}),
	}
}

// ListenAndServe opens the real TCP listener for the CLI server.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve accepts long-lived TCP connections until the context is cancelled. I
// split it out so the listener is easy to swap in small checks or tests.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
		s.CloseSessions()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		session := s.newSession(conn)
		go session.run(ctx)
	}
}

// CloseSessions shuts down active clients when the server is stopping.
func (s *Server) CloseSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for session := range s.sessions {
		_ = session.close()
	}
}

// newSession creates one independent state machine per TCP connection. This is
// why two clients can both be "admin" without sharing job or nonce state.
func (s *Server) newSession(conn net.Conn) *Session {
	id := s.nextID.Add(1)
	session := newSession(s, id, conn)

	s.mu.Lock()
	s.sessions[session] = struct{}{}
	s.mu.Unlock()

	return session
}

// removeSession drops a finished connection from the active set.
func (s *Server) removeSession(session *Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, session)
}

// isAllowed is intentionally simple for the challenge, but hiding it behind a
// method leaves room for a real auth source later.
func (s *Server) isAllowed(username string) bool {
	_, ok := s.cfg.AllowedUsers[username]
	return ok
}

// isClosed keeps normal client disconnects out of the scary error logs.
func isClosed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed)
}
