package main

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// The shape of clients/testdata/driver-vectors/v1.json. The normative
// description is clients/handbook/testing.md section 1.1; these types are its
// one encoding, so a field added here and not there is a handbook bug.

// formatName is the file's format word. A breaking change to the shape is a
// new word and a new file, never an edit of this one.
const formatName = "hoarddb-driver-vectors/1"

// vectorFile is the whole file.
type vectorFile struct {
	Format        string `json:"format"`
	License       string `json:"license"`
	ServerCommit  string `json:"server_commit"`
	ServerVersion string `json:"server_version"`
	// ServerEnv is the environment every replaying server starts with, on top
	// of the per-test directories and ports (handbook testing.md section 3.1).
	ServerEnv      map[string]string `json:"server_env"`
	Normalisations []string          `json:"normalisations"`
	Vectors        []vector          `json:"vectors"`
}

// vector is one entry. Kind decides which of the optional members are set.
type vector struct {
	Name   string `json:"name"`
	Group  string `json:"group"`
	Kind   string `json:"kind"`
	Status string `json:"status"`
	Notes  string `json:"notes,omitempty"`

	// exchange
	StoreType  string            `json:"store_type,omitempty"`
	Topology   string            `json:"topology,omitempty"`
	Replay     *bool             `json:"replay,omitempty"`
	Connection string            `json:"connection,omitempty"`
	ServerEnv  map[string]string `json:"server_env,omitempty"`
	Setup      []setupStep       `json:"setup,omitempty"`
	Request    *vectorRequest    `json:"request,omitempty"`
	Response   *vectorResponse   `json:"response,omitempty"`
	Error      *vectorError      `json:"error,omitempty"`
	AuthFail   *vectorAuthFail   `json:"auth_fail,omitempty"`
	// Refuse is a payload that a strict decoder must refuse (kind "refuse").
	Refuse *vectorRefusal `json:"refuse,omitempty"`

	// frame and decode
	FrameHex         string       `json:"frame_hex,omitempty"`
	RequestHeaderHex string       `json:"request_header_hex,omitempty"`
	FrameExpect      *frameExpect `json:"expect,omitempty"`

	// trust
	PublicKeyHex   string            `json:"public_key_hex,omitempty"`
	CertificatePEM string            `json:"certificate_pem,omitempty"`
	Fingerprint    string            `json:"fingerprint,omitempty"`
	KnownServers   *knownServersCase `json:"known_servers,omitempty"`
}

// setupStep is a request the harness sends, and expects to succeed, before
// the vector's own request.
type setupStep struct {
	Opcode      uint8           `json:"opcode"`
	OpcodeName  string          `json:"opcode_name"`
	PayloadJSON json.RawMessage `json:"payload_json"`
}

// vectorRequest is the request as the server's own encoder produced it.
type vectorRequest struct {
	Opcode        uint8           `json:"opcode"`
	OpcodeName    string          `json:"opcode_name"`
	RequestIDHex  string          `json:"request_id_hex"`
	PayloadJSON   json.RawMessage `json:"payload_json,omitempty"`
	PayloadHex    string          `json:"payload_hex"`
	AllowedFields []string        `json:"allowed_fields"`
}

// vectorResponse is the server's answer as captured, after normalisation.
type vectorResponse struct {
	Opcode       uint8           `json:"opcode"`
	OpcodeName   string          `json:"opcode_name"`
	RequestIDHex string          `json:"request_id_hex"`
	PayloadHex   string          `json:"payload_hex"`
	PayloadJSON  json.RawMessage `json:"payload_json,omitempty"`
	// Fields is every wire name the answer's type can carry, nested ones as
	// paths ("buckets[].name"), whether or not this answer carries it.
	Fields         []string `json:"fields"`
	Vary           []string `json:"vary"`
	AdditiveFields []string `json:"additive_fields"`
	AbsentFields   []string `json:"absent_fields"`
}

// vectorError is the error an ERROR answer must become in a driver.
type vectorError struct {
	Code            int      `json:"code"`
	Name            string   `json:"name"`
	WriteOutcome    string   `json:"write_outcome,omitempty"`
	MessageContains []string `json:"message_contains"`
}

// vectorRefusal says who must refuse a malformed payload and how. Direction request:
// the server answers the request in Request with an OpError of this code. Direction
// reply or error: a driver must not accept the Response payload, and reports this code.
type vectorRefusal struct {
	Direction string `json:"direction"`
	Code      int    `json:"code"`
	Name      string `json:"name"`
}

// vectorAuthFail is the refusal an AUTH_FAIL answer must become in a driver.
type vectorAuthFail struct {
	Reason          int      `json:"reason"`
	Name            string   `json:"name"`
	MessageContains []string `json:"message_contains"`
}

// frameExpect is what a frame or decode vector's frame must do in a driver.
type frameExpect struct {
	Result        string `json:"result"`
	Version       *uint8 `json:"version,omitempty"`
	Length        uint32 `json:"length,omitempty"`
	Opcode        *uint8 `json:"opcode,omitempty"`
	RequestIDHex  string `json:"request_id_hex,omitempty"`
	PayloadHex    string `json:"payload_hex,omitempty"`
	RefuseBecause string `json:"refuse_because,omitempty"`

	// decode: the error the driver reports.
	Code            *int     `json:"code,omitempty"`
	Name            string   `json:"name,omitempty"`
	AuthFailReason  *int     `json:"auth_fail_reason,omitempty"`
	RequestIDZero   *bool    `json:"request_id_zero,omitempty"`
	ConnectionFatal *bool    `json:"connection_fatal,omitempty"`
	MessageContains []string `json:"message_contains,omitempty"`

	// answer: a success frame a driver decodes into its typed response,
	// whose values must equal PayloadJSON.
	PayloadJSON json.RawMessage `json:"payload_json,omitempty"`
}

// knownServersCase is one known_servers file text and what the server's own
// loader made of it.
type knownServersCase struct {
	FileText string `json:"file_text"`
	// Entries maps host:port to the raw public key's hex, when the file loads.
	Entries       map[string]string `json:"entries,omitempty"`
	ErrorContains string            `json:"error_contains,omitempty"`
}

// encodeVectorFile renders the file deterministically: two-space indent,
// no HTML escaping, a trailing newline. encoding/json sorts map keys, and
// every struct member has a fixed order, so equal values give equal bytes.
func encodeVectorFile(file vectorFile) ([]byte, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(file); err != nil {
		return nil, fmt.Errorf("encoding the vector file: %w", err)
	}
	return encoded.Bytes(), nil
}

// decodeVectorFile reads a file written by encodeVectorFile.
func decodeVectorFile(encoded []byte) (vectorFile, error) {
	var file vectorFile
	if err := json.Unmarshal(encoded, &file); err != nil {
		return vectorFile{}, fmt.Errorf("decoding the vector file: %w", err)
	}
	return file, nil
}
