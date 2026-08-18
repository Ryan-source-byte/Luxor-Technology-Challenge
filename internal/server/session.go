package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"luxor-challenge/internal/nonce"
	"luxor-challenge/internal/proof"
	"luxor-challenge/internal/protocol"
	"luxor-challenge/internal/stats"
)

const (
	errTaskMissing       = "Task does not exist"
	errTaskExpired       = "Task expired"
	errInvalidResult     = "Invalid result"
	errRateLimit         = "Submission too frequent"
	errDuplicate         = "Duplicate submission"
	errUnauthorized      = "Unauthorized"
	errInvalidRequest    = "Invalid request"
	errAlreadyAuthorized = "Already authorized"
	errStatsUnavailable  = "Statistics unavailable"
)

// Session is where I keep the moving parts for one TCP client. The parent
// Server accepts sockets; this type owns username, jobs, nonce history, and the
// per-client rate limit.
type Session struct {
	server *Server
	id     int64
	conn   net.Conn
	enc    *protocol.Encoder

	ctx    context.Context
	cancel context.CancelFunc

	mu             sync.Mutex
	username       string
	authenticated  bool
	currentJobID   int64
	currentNonce   string
	jobHistory     map[int64]string
	jobOrder       []int64
	expiredJobs    map[int64]struct{}
	expiredOrder   []int64
	usedNonces     map[string]struct{}
	usedNonceOrder []string
	lastSubmission time.Time
	jobLoopStarted bool
}

// newSession sets up the bounded maps I need for validation. I split active
// jobs, expired jobs, and used nonces so errors can be specific without letting
// a long-lived connection grow memory forever.
func newSession(server *Server, id int64, conn net.Conn) *Session {
	return &Session{
		server:      server,
		id:          id,
		conn:        conn,
		enc:         protocol.NewEncoder(conn),
		jobHistory:  make(map[int64]string),
		expiredJobs: make(map[int64]struct{}),
		usedNonces:  make(map[string]struct{}),
	}
}

// run is the session read loop. It keeps dispatch simple: decode one frame,
// look at the method, and let the handler decide whether an id is required.
func (s *Session) run(parent context.Context) {
	s.ctx, s.cancel = context.WithCancel(parent)
	defer func() {
		_ = s.close()
		s.server.removeSession(s)
	}()

	dec := protocol.NewDecoder(s.conn)
	for {
		msg, err := dec.Decode()
		if err != nil {
			if !isClosed(err) {
				s.server.cfg.Logger.Printf("session %d decode error: %v", s.id, err)
			}
			return
		}

		switch msg.Method {
		case "authorize":
			s.handleAuthorize(msg)
		case "submit":
			s.handleSubmit(msg)
		default:
			s.respondMessage(msg, false, fmt.Sprintf("unknown method %q", msg.Method))
		}
	}
}

// close stops this session's job loop and closes the socket.
func (s *Session) close() error {
	if s.cancel != nil {
		s.cancel()
	}
	return s.conn.Close()
}

// handleAuthorize accepts the simple username-based auth request and only then
// starts job delivery. That keeps the client flow close to the prompt.
func (s *Session) handleAuthorize(msg protocol.Message) {
	if msg.ID == nil {
		return
	}

	params, err := protocol.DecodeParams[protocol.AuthorizeParams](msg)
	if err != nil || params.Username == "" {
		s.respond(*msg.ID, false, errInvalidRequest)
		return
	}
	if !s.server.isAllowed(params.Username) {
		s.respond(*msg.ID, false, errUnauthorized)
		return
	}

	shouldStartJobs := false
	s.mu.Lock()
	if s.authenticated {
		s.mu.Unlock()
		s.respond(*msg.ID, false, errAlreadyAuthorized)
		return
	}
	s.username = params.Username
	s.authenticated = true
	if !s.jobLoopStarted {
		s.jobLoopStarted = true
		shouldStartJobs = true
	}
	s.mu.Unlock()

	s.respond(*msg.ID, true, "")
	if shouldStartJobs {
		s.startJobLoop()
	}
}

// handleSubmit keeps the shape checks separate from the real business checks.
// I only record stats after everything passes, so rejected work never counts.
func (s *Session) handleSubmit(msg protocol.Message) {
	if msg.ID == nil {
		return
	}

	params, err := protocol.DecodeParams[protocol.SubmitParams](msg)
	if err != nil || params.JobID <= 0 || params.ClientNonce == "" || params.Result == "" {
		s.respond(*msg.ID, false, errInvalidRequest)
		return
	}

	username, serverNonce, submitErr := s.validateSubmit(params)
	if submitErr != "" {
		s.respond(*msg.ID, false, submitErr)
		return
	}

	event := stats.NewSubmission(username, s.server.cfg.Now())
	if err := s.server.recorder.Record(s.ctx, event); err != nil {
		s.server.cfg.Logger.Printf("session %d stats error: %v", s.id, err)
		s.respond(*msg.ID, false, errStatsUnavailable)
		return
	}

	_ = serverNonce
	s.respond(*msg.ID, true, "")
}

// This function handles all the validation for a single TCP session.
//
// I intentionally ordered the checks here. It's tempting to just check the hash
// right away, but it's better to know exactly *why* a submit failed (e.g.,
// duplicate nonce, submitting too fast, or an old job).
//
// The strict order is: duplicate nonce -> rate limit -> job history.
// Keeping job history helps us tell if a job expired or if the client just made it up.
//
// I also added a lock here to keep things thread-safe. All these checks rely on
// session state (nonces, timestamps, etc.). Without the lock, concurrent requests
// from the exact same connection could race and mess up the state.
func (s *Session) validateSubmit(params protocol.SubmitParams) (string, string, string) {
	now := s.server.cfg.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.authenticated {
		return "", "", errUnauthorized
	}

	if _, exists := s.usedNonces[params.ClientNonce]; exists {
		return "", "", errDuplicate
	}
	s.rememberClientNonce(params.ClientNonce)

	if !s.lastSubmission.IsZero() && now.Sub(s.lastSubmission) < s.server.cfg.SubmitMinInterval {
		return "", "", errRateLimit
	}
	s.lastSubmission = now

	serverNonce, exists := s.jobHistory[params.JobID]
	if !exists {
		if _, expired := s.expiredJobs[params.JobID]; expired {
			return "", "", errTaskExpired
		}
		return "", "", errTaskMissing
	}

	if proof.Result(serverNonce, params.ClientNonce) != params.Result {
		return "", "", errInvalidResult
	}

	return s.username, serverNonce, ""
}

// startJobLoop sends the first job right away, then refreshes on the configured
// interval. Waiting 30 seconds before the first job would be annoying to demo.
func (s *Session) startJobLoop() {
	go func() {
		s.issueJob()

		ticker := time.NewTicker(s.server.cfg.JobInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				s.issueJob()
			}
		}
	}()
}

// issueJob creates the next nonce/job pair and sends it as a fire-and-forget
// event. The mapping stays here so submit can prove it used the right nonce.
func (s *Session) issueJob() {
	serverNonce, err := nonce.Hex(s.server.cfg.NonceBytes)
	if err != nil {
		s.server.cfg.Logger.Printf("session %d nonce error: %v", s.id, err)
		return
	}

	s.mu.Lock()
	s.currentJobID++
	jobID := s.currentJobID
	s.currentNonce = serverNonce
	s.rememberJob(jobID, serverNonce)
	s.mu.Unlock()

	event, err := protocol.NewEvent("job", protocol.JobParams{
		JobID:       jobID,
		ServerNonce: serverNonce,
	})
	if err != nil {
		s.server.cfg.Logger.Printf("session %d job encode error: %v", s.id, err)
		return
	}
	if err := s.enc.Encode(event); err != nil {
		s.server.cfg.Logger.Printf("session %d job send error: %v", s.id, err)
		_ = s.close()
	}
}

// rememberJob keeps a small job history. The latest job matters most, but a
// short history lets me return "Task expired" instead of only "Task does not exist".
func (s *Session) rememberJob(jobID int64, serverNonce string) {
	s.jobHistory[jobID] = serverNonce
	s.jobOrder = append(s.jobOrder, jobID)

	for len(s.jobOrder) > s.server.cfg.JobHistoryLimit {
		expiredID := s.jobOrder[0]
		s.jobOrder = s.jobOrder[1:]
		delete(s.jobHistory, expiredID)
		s.rememberExpiredJob(expiredID)
	}
}

// rememberExpiredJob keeps just enough old ids to explain recent stale submits.
func (s *Session) rememberExpiredJob(jobID int64) {
	s.expiredJobs[jobID] = struct{}{}
	s.expiredOrder = append(s.expiredOrder, jobID)

	for len(s.expiredOrder) > s.server.cfg.JobHistoryLimit {
		oldest := s.expiredOrder[0]
		s.expiredOrder = s.expiredOrder[1:]
		delete(s.expiredJobs, oldest)
	}
}

// rememberClientNonce rejects replayed submissions while keeping the memory
// footprint bounded.
func (s *Session) rememberClientNonce(clientNonce string) {
	s.usedNonces[clientNonce] = struct{}{}
	s.usedNonceOrder = append(s.usedNonceOrder, clientNonce)

	for len(s.usedNonceOrder) > s.server.cfg.NonceHistoryLimit {
		oldest := s.usedNonceOrder[0]
		s.usedNonceOrder = s.usedNonceOrder[1:]
		delete(s.usedNonces, oldest)
	}
}

// respondMessage only replies when there is an id to correlate with.
func (s *Session) respondMessage(msg protocol.Message, result bool, errMessage string) {
	if msg.ID == nil {
		return
	}
	s.respond(*msg.ID, result, errMessage)
}

// respond writes the common result/error shape back to the client.
func (s *Session) respond(id int64, result bool, errMessage string) {
	if err := s.enc.Encode(protocol.NewResponse(id, result, errMessage)); err != nil {
		s.server.cfg.Logger.Printf("session %d response error: %v", s.id, err)
		_ = s.close()
	}
}
