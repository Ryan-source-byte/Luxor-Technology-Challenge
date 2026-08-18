package client

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"luxor-challenge/internal/nonce"
	"luxor-challenge/internal/proof"
	"luxor-challenge/internal/protocol"
)

type Config struct {
	Addr           string
	Username       string
	SubmitInterval time.Duration
	Logger         *log.Logger
}

type Client struct {
	cfg Config
}

// New only fills in defaults. I leave the network work to Run so constructing a
// client stays boring and side-effect free.
func New(cfg Config) *Client {
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:4000"
	}
	if cfg.Username == "" {
		cfg.Username = "admin"
	}
	if cfg.SubmitInterval <= 0 {
		cfg.SubmitInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stdout, "client ", log.LstdFlags|log.Lmicroseconds)
	}
	return &Client{cfg: cfg}
}

// Run owns the client lifecycle. The challenge asks for one server connection
// per client instance, so I reconnect serially instead of spinning up extra
// connections in the background.
func (c *Client) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := c.runOnce(ctx); err != nil && ctx.Err() == nil {
			c.cfg.Logger.Printf("connection ended: %v", err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	auth, err := protocol.NewRequest(1, "authorize", protocol.AuthorizeParams{Username: c.cfg.Username})
	if err != nil {
		return err
	}
	if err := enc.Encode(auth); err != nil {
		return err
	}

	authResp, err := dec.Decode()
	if err != nil {
		return err
	}
	if authResp.ID == nil || *authResp.ID != 1 || !protocol.BoolValue(authResp.Result) {
		if authResp.Error != "" {
			return fmt.Errorf("authorization failed: %s", authResp.Error)
		}
		return fmt.Errorf("authorization failed")
	}
	c.cfg.Logger.Printf("authorized as %s", c.cfg.Username)

	// After auth, reading and submitting are separate loops. I did that so a
	// slow submit cadence does not make the client late to notice a new job.
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	state := &jobState{}
	wake := make(chan struct{}, 1)
	readErr := make(chan error, 1)

	go c.readLoop(sessionCtx, dec, state, wake, readErr)
	return c.submitLoop(sessionCtx, enc, state, wake, readErr)
}

func (c *Client) readLoop(ctx context.Context, dec *protocol.Decoder, state *jobState, wake chan<- struct{}, readErr chan<- error) {
	for {
		msg, err := dec.Decode()
		if err != nil {
			select {
			case readErr <- err:
			default:
			}
			return
		}

		if msg.Method == "job" {
			params, err := protocol.DecodeParams[protocol.JobParams](msg)
			if err != nil {
				c.cfg.Logger.Printf("invalid job payload: %v", err)
				continue
			}
			state.set(params)
			c.cfg.Logger.Printf("received job_id=%d server_nonce=%s", params.JobID, params.ServerNonce)
			// A fresh job wakes the submit loop right away. Waiting for the next
			// timer tick would make the client feel sluggish for no reason.
			select {
			case wake <- struct{}{}:
			default:
			}
			continue
		}

		if msg.Result != nil {
			if protocol.BoolValue(msg.Result) {
				c.cfg.Logger.Printf("submission accepted")
			} else {
				c.cfg.Logger.Printf("submission rejected: %s", msg.Error)
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (c *Client) submitLoop(ctx context.Context, enc *protocol.Encoder, state *jobState, wake <-chan struct{}, readErr <-chan error) error {
	var requestID int64 = 2
	var lastSubmit time.Time

	// At first I expected the default 1s client interval to produce close to 60
	// successful submissions per minute. But while testing the processor path,
	// the database aggregation was closer to 30 per minute, which felt wrong.
	//
	// I ruled out a few things first. The processor only consumes RabbitMQ
	// events and writes them to Postgres, so it should not cut the rate in half.
	// Postgres is just doing a minute-bucket upsert, so it should not silently
	// drop half the rows either. That pushed me back toward the client timing or
	// the server rate limit.
	//
	// The client logs had occasionally shown "Submission too frequent". That
	// made the issue click: the client may intend to send every 1.000s, but the
	// server judges the receive-time gap. If scheduling or socket timing makes
	// one request arrive at 999ms, the server is right to reject it.
	//
	// I first added a small client-side margin, using about 1.1s instead of
	// exactly 1.0s. The requirement says "1/second maximum", not "exactly once
	// every second", so this still stays well above the 1/minute minimum while
	// avoiding accidental rate-limit hits.
	//
	// Then I found a more subtle bug in my first fix. I used a fixed ticker that
	// fired every 1.1s, and submit also checked time.Since(lastSubmit). If the
	// ticker fired at 1.099s, the guard skipped that attempt, and the next tick
	// would not arrive until roughly 2.2s after the previous submit. That matched
	// the 30-ish submissions per minute I was seeing.
	//
	// The current version schedules the next timer after a submit actually
	// succeeds, using the real lastSubmit time as the baseline. A new job can
	// still wake the loop immediately, but if the interval has not passed yet it
	// waits only the remaining time instead of skipping a whole extra tick.
	//
	// This ended up being a useful little reminder: the symptom looked like a
	// RabbitMQ/Postgres counting problem, but the real bug was the boundary
	// between client timing and server-side rate limiting.
	effectiveInterval := c.effectiveSubmitInterval()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var timerC <-chan time.Time

	scheduleNext := func(delay time.Duration) {
		if delay < 0 {
			delay = 0
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(delay)
		timerC = timer.C
	}

	// submit always reads the latest job. Once the server rotates the nonce, I
	// would rather move forward than keep producing noisy expired-job attempts.
	submit := func() error {
		job, ok := state.get()
		if !ok {
			return nil
		}

		clientNonce, err := nonce.Hex(16)
		if err != nil {
			c.cfg.Logger.Printf("client nonce error: %v", err)
			return nil
		}
		params := protocol.SubmitParams{
			JobID:       job.JobID,
			ClientNonce: clientNonce,
			Result:      proof.Result(job.ServerNonce, clientNonce),
		}
		msg, err := protocol.NewRequest(requestID, "submit", params)
		if err != nil {
			c.cfg.Logger.Printf("submit encode error: %v", err)
			return nil
		}
		requestID++

		if err := enc.Encode(msg); err != nil {
			return err
		}
		lastSubmit = time.Now()
		scheduleNext(effectiveInterval)
		return nil
	}

	trySubmit := func() error {
		if _, ok := state.get(); !ok {
			return nil
		}
		if !lastSubmit.IsZero() {
			remaining := effectiveInterval - time.Since(lastSubmit)
			if remaining > 0 {
				scheduleNext(remaining)
				return nil
			}
		}
		if err := submit(); err != nil {
			return err
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErr:
			return err
		case <-wake:
			if err := trySubmit(); err != nil {
				return err
			}
		case <-timerC:
			timerC = nil
			if err := trySubmit(); err != nil {
				return err
			}
		}
	}
}

func (c *Client) effectiveSubmitInterval() time.Duration {
	if c.cfg.SubmitInterval < 2*time.Second {
		// The server checks receive time, not the client's intention. A tiny
		// margin keeps the default "1s" setting from accidentally arriving as
		// 999ms after scheduler or socket jitter.
		return c.cfg.SubmitInterval + 100*time.Millisecond
	}
	return c.cfg.SubmitInterval
}

type jobState struct {
	mu  sync.RWMutex
	job protocol.JobParams
	ok  bool
}

// jobState is the only shared state between the read loop and submit loop. A
// tiny lock here felt clearer than folding everything into one large select.
func (s *jobState) set(job protocol.JobParams) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.job = job
	s.ok = true
}

func (s *jobState) get() (protocol.JobParams, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.job, s.ok
}
