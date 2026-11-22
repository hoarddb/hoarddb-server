package transport

import "fmt"

// ErrorCode is a uint8 error code sent in OpError responses.
type ErrorCode uint8

const (
	ErrOK                 ErrorCode = 0x00 // No error
	ErrNotFound           ErrorCode = 0x01 // Key not found
	ErrAlreadyExists      ErrorCode = 0x02 // Database or bucket already exists
	ErrPermissionDenied   ErrorCode = 0x03 // Auth failure
	ErrInvalidArgument    ErrorCode = 0x04 // Malformed request
	ErrInternal           ErrorCode = 0x05 // Server error
	ErrUnavailable        ErrorCode = 0x06 // Server shutting down
	ErrTimeout            ErrorCode = 0x07 // Operation timed out
	ErrConflict           ErrorCode = 0x08 // Concurrent modification conflict
	ErrQuotaExceeded      ErrorCode = 0x09 // Free tier limit reached
	ErrUnsupportedVersion ErrorCode = 0x0A // Unknown protocol version; also a write to a bucket whose typed-field declaration this version cannot enforce
	ErrEarlyDataRejected  ErrorCode = 0x0B // Mutating op rejected from 0-RTT
	ErrNotAuthenticated   ErrorCode = 0x0C // Stream rejected: connection not yet authenticated
	// ErrOverloaded is sent when the server is at its connection limit and
	// refuses a new connection (tcp-protocol-spec.md 5.6).
	ErrOverloaded ErrorCode = 0x0D // Connection refused: server at its connection limit
	// ErrResultTooLarge is sent when a response would not fit in one frame:
	// a query result over the node's ceiling (the error carries
	// max_result_bytes and retained_bytes), or, as a server bug, any response
	// over MaxFrameLength (query-pipelining spec W-2, §3.3). The connection
	// and its other requests are unaffected.
	ErrResultTooLarge ErrorCode = 0x0E // Response too large for one frame
	// ErrDuplicateKey is sent when a write would give a unique index a second
	// document with the same key (index-parity spec §4.6). Nothing of the
	// write was stored. It has its own code because it is the refusal an
	// application most often branches on — insert, and on a duplicate update
	// instead — and ALREADY_EXISTS would make "the value is taken" and "the
	// bucket exists" one answer, while CONFLICT invites a retry that can never
	// succeed.
	ErrDuplicateKey ErrorCode = 0x0F // Unique index already holds the value
	// ErrFieldType is sent when a document is refused for one of its fields:
	// a top-level field name no path can reach (it contains ".", "[" or "]"),
	// and, on a bucket that declares field types, a value the declaration
	// refuses (typed-fields spec §6.1). Nothing of the write was stored. It has
	// its own code for the reason DUPLICATE_KEY does: an application branches
	// on it, and INVALID_ARGUMENT would make "your request is malformed" and
	// "this document's field is refused" one answer.
	ErrFieldType ErrorCode = 0x10 // A field of the document is refused by the bucket's rules
	// ErrBatchTooLarge is sent when a batch put is over the largest batch the
	// store commits atomically, by bytes or by entry count (BUG-0467 G11).
	// Nothing of the batch was stored (write_outcome not_applied). It has its
	// own code so a driver halves the batch on the code, not on a phrase of the
	// message, and because INVALID_ARGUMENT would make "split this batch" and
	// "this request is malformed" one answer.
	ErrBatchTooLarge ErrorCode = 0x11 // A batch exceeds the store's atomic bound; nothing was stored
	// ErrDocumentExists is sent when an insert names an _id_ that is already
	// taken: today, one _id_ repeated inside a single BATCH_PUT, which is
	// refused whole before anything is logged. Its own code lets a driver
	// branch to an upsert without parsing the message.
	ErrDocumentExists ErrorCode = 0x12 // An insert names an _id_ that is already taken; nothing was stored
)

// String returns a human-readable name for the error code.
func (c ErrorCode) String() string {
	switch c {
	case ErrOK:
		return "OK"
	case ErrNotFound:
		return "NOT_FOUND"
	case ErrAlreadyExists:
		return "ALREADY_EXISTS"
	case ErrPermissionDenied:
		return "PERMISSION_DENIED"
	case ErrInvalidArgument:
		return "INVALID_ARGUMENT"
	case ErrInternal:
		return "INTERNAL"
	case ErrUnavailable:
		return "UNAVAILABLE"
	case ErrTimeout:
		return "TIMEOUT"
	case ErrConflict:
		return "CONFLICT"
	case ErrQuotaExceeded:
		return "QUOTA_EXCEEDED"
	case ErrUnsupportedVersion:
		return "UNSUPPORTED_VERSION"
	case ErrEarlyDataRejected:
		return "EARLY_DATA_REJECTED"
	case ErrNotAuthenticated:
		return "NOT_AUTHENTICATED"
	case ErrOverloaded:
		return "OVERLOADED"
	case ErrResultTooLarge:
		return "RESULT_TOO_LARGE"
	case ErrDuplicateKey:
		return "DUPLICATE_KEY"
	case ErrFieldType:
		return "FIELD_TYPE"
	case ErrBatchTooLarge:
		return "BATCH_TOO_LARGE"
	case ErrDocumentExists:
		return "DOCUMENT_EXISTS"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(c))
	}
}

// AuthFailReason is the reason code carried by an OpAuthFail payload. A nil
// payload means AuthFailCredentials (today's behaviour and still valid); the
// code exists so a driver can tell "retry with the password, once"
// (TOKEN_EXPIRED/INVALID/UNSUPPORTED) from "back off" (RATE_LIMITED) from
// "stop, do not retry" (CREDENTIALS/PROTOCOL_MISMATCH). Without it every
// failure becomes a password retry, which is the credential-stuffing pattern
// the rate limiter exists to stop (spec 3.3c).
//
// The zero value is deliberately not a valid reason: 0 must never be emitted
// (spec 10.3 is the same class of bug one layer up).
type AuthFailReason uint8

const (
	// AuthFailCredentials is bad username/password (also the nil-payload
	// meaning).
	AuthFailCredentials AuthFailReason = 1
	// AuthFailTokenExpired is a signature-valid token past exp or abs.
	AuthFailTokenExpired AuthFailReason = 2
	// AuthFailTokenInvalid is a bad signature, malformed parse, unknown key,
	// wrong epoch, or a username that disagrees with the token subject.
	AuthFailTokenInvalid AuthFailReason = 3
	// AuthFailTokenUnsupported is a server without the auth_token capability.
	AuthFailTokenUnsupported AuthFailReason = 4
	// AuthFailProtocolMismatch is a protocol-version assertion failure.
	AuthFailProtocolMismatch AuthFailReason = 5
	// AuthFailRateLimited means back off; it is not a credential failure. It
	// asserts that no credential was checked: the TCP listener keeps it out of
	// the per-IP failed-login ledger, so a handler that returned it
	// after checking a password would hand out unlimited guesses.
	AuthFailRateLimited AuthFailReason = 6
	// AuthFailAlreadyAuthed is a password AUTH on an authenticated connection.
	AuthFailAlreadyAuthed AuthFailReason = 7
	// AuthFailInternal is a server-side/decode failure.
	AuthFailInternal AuthFailReason = 8
)

// String returns a human-readable name for the auth failure reason.
func (r AuthFailReason) String() string {
	switch r {
	case AuthFailCredentials:
		return "CREDENTIALS"
	case AuthFailTokenExpired:
		return "TOKEN_EXPIRED"
	case AuthFailTokenInvalid:
		return "TOKEN_INVALID"
	case AuthFailTokenUnsupported:
		return "TOKEN_UNSUPPORTED"
	case AuthFailProtocolMismatch:
		return "PROTOCOL_MISMATCH"
	case AuthFailRateLimited:
		return "RATE_LIMITED"
	case AuthFailAlreadyAuthed:
		return "ALREADY_AUTHED"
	case AuthFailInternal:
		return "INTERNAL"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(r))
	}
}

// ErrOverloaded is fatal for the *connection* — the server refused it and
// the client must dial again later — but it is deliberately NOT a reason
// for a driver to tear down its whole pool: one busy server behind a load
// balancer refusing a new socket does not mean the other pooled
// connections are broken. Drivers will copy this decision, which is why
// the comment records it here rather than in each driver.
//
// The TCP listener completes the TLS handshake and sends one
// OpError(OVERLOADED) frame with the cap and retry guidance before closing,
// so an overload refusal is distinguishable from a TLS misconfiguration.

// WireError represents an error received over the wire (OpError opcode).
type WireError struct {
	Code    ErrorCode
	Message string
}

// Error implements the error interface.
func (e *WireError) Error() string {
	return fmt.Sprintf("hoardDB wire error [%s]: %s", e.Code, e.Message)
}
