package protocol

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRequestAndEventIDs(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	req, err := NewRequest(7, "authorize", AuthorizeParams{Username: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewEvent("job", JobParams{JobID: 1, ServerNonce: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(req); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(event); err != nil {
		t.Fatal(err)
	}

	dec := NewDecoder(&buf)
	decodedReq, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if decodedReq.ID == nil || *decodedReq.ID != 7 {
		t.Fatalf("decoded request id = %v, want 7", decodedReq.ID)
	}

	decodedEvent, err := dec.Decode()
	if err != nil {
		t.Fatal(err)
	}
	if decodedEvent.ID != nil {
		t.Fatalf("decoded event id = %v, want nil", decodedEvent.ID)
	}
}
