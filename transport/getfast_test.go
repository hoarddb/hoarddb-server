package transport

import (
	"bytes"
	"testing"
)

// Round-trip a typical request: the encoding must decode to the same three
// fields, or a read silently targets the wrong document.
func TestGetFastRequestRoundTrip(t *testing.T) {
	want := GetFastRequest{Database: "app", Bucket: "users", Key: "user_000123"}

	payload, err := MarshalGetFast(want)
	if err != nil {
		t.Fatalf("MarshalGetFast: %v", err)
	}
	got, err := UnmarshalGetFast(payload)
	if err != nil {
		t.Fatalf("UnmarshalGetFast: %v", err)
	}
	if got != want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}

	// The compact payload must be smaller than an equivalent BSON encoding,
	// which is the entire point of the optimization.
	if len(payload) != 4+len(want.Database)+len(want.Bucket)+len(want.Key) {
		t.Fatalf("payload is %d bytes, want %d", len(payload), 4+len(want.Database)+len(want.Bucket)+len(want.Key))
	}
}

// Empty field values are legal (the server reports the real error); they must
// still round-trip without confusing the parser.
func TestGetFastRequestRoundTripEmptyFields(t *testing.T) {
	for _, want := range []GetFastRequest{
		{Database: "", Bucket: "", Key: ""},
		{Database: "app", Bucket: "", Key: "k"},
		{Database: "", Bucket: "b", Key: ""},
	} {
		payload, err := MarshalGetFast(want)
		if err != nil {
			t.Fatalf("MarshalGetFast(%+v): %v", want, err)
		}
		got, err := UnmarshalGetFast(payload)
		if err != nil {
			t.Fatalf("UnmarshalGetFast(%+v): %v", want, err)
		}
		if got != want {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

// A field over its encodable maximum must error, never truncate.
func TestMarshalGetFastRejectsOverlongFields(t *testing.T) {
	longName := string(bytes.Repeat([]byte("a"), MaxGetFastNameLen+1))
	if _, err := MarshalGetFast(GetFastRequest{Database: longName}); err == nil {
		t.Fatal("MarshalGetFast accepted a database name over the maximum")
	}
	if _, err := MarshalGetFast(GetFastRequest{Bucket: longName}); err == nil {
		t.Fatal("MarshalGetFast accepted a bucket name over the maximum")
	}
	longKey := string(bytes.Repeat([]byte("k"), MaxGetFastKeyLen+1))
	if _, err := MarshalGetFast(GetFastRequest{Key: longKey}); err == nil {
		t.Fatal("MarshalGetFast accepted a key over the maximum")
	}
}

// A truncated payload must return an error, not panic or read past the end.
func TestUnmarshalGetFastRejectsTruncatedPayloads(t *testing.T) {
	full, err := MarshalGetFast(GetFastRequest{Database: "app", Bucket: "users", Key: "user_000123"})
	if err != nil {
		t.Fatalf("MarshalGetFast: %v", err)
	}
	for i := 0; i < len(full); i++ {
		if _, err := UnmarshalGetFast(full[:i]); err == nil {
			t.Fatalf("UnmarshalGetFast accepted a %d-byte prefix of a %d-byte payload", i, len(full))
		}
	}
}

func TestGetFastResponseRoundTrip(t *testing.T) {
	document := []byte{0x05, 0x00, 0x00, 0x00, 0x00}

	payload := MarshalGetFastResponse(GetFastFound, document)
	status, got, err := UnmarshalGetFastResponse(payload)
	if err != nil {
		t.Fatalf("UnmarshalGetFastResponse: %v", err)
	}
	if status != GetFastFound {
		t.Fatalf("status = %#x, want %#x", status, GetFastFound)
	}
	if !bytes.Equal(got, document) {
		t.Fatalf("document = %v, want %v", got, document)
	}

	// Not-found carries no document.
	payload = MarshalGetFastResponse(GetFastNotFound, nil)
	status, got, err = UnmarshalGetFastResponse(payload)
	if err != nil {
		t.Fatalf("UnmarshalGetFastResponse: %v", err)
	}
	if status != GetFastNotFound {
		t.Fatalf("status = %#x, want %#x", status, GetFastNotFound)
	}
	if len(got) != 0 {
		t.Fatalf("document = %v, want empty", got)
	}

	if _, _, err := UnmarshalGetFastResponse(nil); err == nil {
		t.Fatal("UnmarshalGetFastResponse accepted an empty payload")
	}
}
