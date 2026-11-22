package ringclient

// CallOption adjusts one request.
type CallOption func(*callConfig)

type callConfig struct {
	requestID    [16]byte
	hasRequestID bool
	deadlineMs   uint32
	guard        Guard
	returnMode   ReturnMode
}

// If makes an Update or Delete apply only when the stored document still holds every
// equality of guard. When one differs the owner changes nothing and the result has
// NotApplied set with the current document. Insert and Upsert refuse it (INVALID_ARGUMENT).
func If(guard Guard) CallOption {
	return func(config *callConfig) { config.guard = guard }
}

// Return asks an Update or Delete to send the document back: [ReturnAfter] or
// [ReturnBefore]. Insert and Upsert refuse it (INVALID_ARGUMENT).
func Return(mode ReturnMode) CallOption {
	return func(config *callConfig) { config.returnMode = mode }
}

// WithRequestID sends the request under id instead of a fresh one. Retry a write whose
// outcome you do not know (a timeout, a dropped connection) with the same id and the owner
// answers from its dedupe window rather than applying it twice. Get one with
// [Client.NextRequestID].
func WithRequestID(id [16]byte) CallOption {
	return func(config *callConfig) { config.requestID, config.hasRequestID = id, true }
}

// WithWriteDeadline bounds the owner's wait for write concern, in milliseconds; 0 means no
// deadline.
func WithWriteDeadline(ms uint32) CallOption {
	return func(config *callConfig) { config.deadlineMs = ms }
}

func newCallConfig(options []CallOption) callConfig {
	var config callConfig
	for _, option := range options {
		option(&config)
	}
	return config
}
