package cli

// RingOutput is what a ring statement prints, one entry per line, without
// the trailing newline.
type RingOutput struct {
	// Lines are the printed lines in order.
	Lines []string
}

// RingError is a failed ring statement: the text to print and the exit status
// a piped run ends with.
type RingError struct {
	// Code is the ring code name (NOT_FOUND, DOCUMENT_EXISTS ...), PARSE_ERROR
	// for a statement the grammar refused, or empty for a local refusal.
	Code string
	// Message is the text, which names the fix.
	Message string
	// Exit is the exit status of conventions Errors.
	Exit int
	// Err is the underlying error, if any.
	Err error
}

// Error returns the code and message.
func (e *RingError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// Unwrap returns the underlying error.
func (e *RingError) Unwrap() error { return e.Err }
