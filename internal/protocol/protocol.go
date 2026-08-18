package protocol

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Message is the one envelope I use for everything that crosses the TCP wire.
//
// The challenge really has three message shapes:
//   - requests that need a response, such as "authorize" and "submit"
//   - server-side events that do not need a response, such as "job"
//   - responses that must be correlated back to the request id
//
// I kept them in one struct because they are all still just one NDJSON frame on
// the same TCP connection. The pointer ID is the practical switch: nil means
// "fire and forget", non-nil means the sender expects a correlated response.
type Message struct {
	ID     *int64          `json:"id"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result *bool           `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// AuthorizeParams stays small because auth policy belongs in the server, not in
// the wire-format package.
type AuthorizeParams struct {
	Username string `json:"username"`
}

// JobParams keeps job_id and server_nonce together because the submit check is
// all about validating that exact pair later.
type JobParams struct {
	JobID       int64  `json:"job_id"`
	ServerNonce string `json:"server_nonce"`
}

// SubmitParams is just the payload shape. The SHA256 decision happens in the
// server layer where the current session state is available.
type SubmitParams struct {
	JobID       int64  `json:"job_id"`
	ClientNonce string `json:"client_nonce"`
	Result      string `json:"result"`
}

type Encoder struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewEncoder writes newline-delimited JSON frames. Wrapping json.Encoder keeps
// the NDJSON choice in one place instead of repeating it around the codebase.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{enc: json.NewEncoder(w)}
}

// Encode has a small lock because a session can send responses and job events
// from different goroutines. I would rather serialize here than debug mixed TCP
// writes later.
func (e *Encoder) Encode(msg Message) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.enc.Encode(msg)
}

type Decoder struct {
	reader *bufio.Reader
}

// NewDecoder reads the same NDJSON framing. Newlines are easy to see in logs,
// which made this feel like the least surprising framing choice.
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{reader: bufio.NewReader(r)}
}

// Decode reads one frame. I still parse a final line without a newline so a
// clean-ish TCP close does not throw away a complete JSON object.
func (d *Decoder) Decode() (Message, error) {
	line, err := d.reader.ReadBytes('\n')
	if err != nil {
		if len(line) == 0 {
			return Message{}, err
		}
		if !errors.Is(err, io.EOF) {
			return Message{}, err
		}
	}

	var msg Message
	if unmarshalErr := json.Unmarshal(line, &msg); unmarshalErr != nil {
		return Message{}, unmarshalErr
	}
	return msg, nil
}

// NewRequest builds a message that expects a response. The explicit id is the
// simple correlation handle for authorize and submit.
func NewRequest(id int64, method string, params any) (Message, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return Message{}, err
	}
	return Message{ID: &id, Method: method, Params: raw}, nil
}

// NewEvent builds a fire-and-forget message. Jobs use this path because the
// client is supposed to start work, not send a job acknowledgement.
func NewEvent(method string, params any) (Message, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return Message{}, err
	}
	return Message{ID: nil, Method: method, Params: raw}, nil
}

// NewResponse keeps the success/error envelope consistent.
func NewResponse(id int64, result bool, errMessage string) Message {
	return Message{ID: &id, Result: &result, Error: errMessage}
}

// DecodeParams is a small helper so handlers can focus on behavior instead of
// repeating json.Unmarshal every time.
func DecodeParams[T any](msg Message) (T, error) {
	var params T
	if len(msg.Params) == 0 {
		return params, fmt.Errorf("missing params")
	}
	if err := json.Unmarshal(msg.Params, &params); err != nil {
		return params, err
	}
	return params, nil
}

// BoolValue is a convenience for response handling; nil still stays meaningful
// on the Message type itself.
func BoolValue(v *bool) bool {
	return v != nil && *v
}
