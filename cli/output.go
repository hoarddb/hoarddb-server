package cli

import (
	"bytes"
	"io"
)

// sessionOutput is where a session's statement output goes. The CLI and the
// command handler share one, so a whole command's output can be held and paged
// as a unit: paging has to know how tall what it is showing is, and that means
// holding it before deciding.
//
// Commentary — warnings, debug lines, "Connected to", a trust question — does
// not come through here. It goes to the chatter writer instead, because it must
// reach the user immediately even while a result is being held, and because a
// question that is held is a question nobody can answer.
//
// One goroutine at a time: statements run in the session's own goroutine, and a
// hold is begun and released inside it.
type sessionOutput struct {
	terminal io.Writer
	held     *bytes.Buffer
}

// Write sends p to the terminal, or to the hold while one is in progress.
func (o *sessionOutput) Write(p []byte) (int, error) {
	if o.held != nil {
		return o.held.Write(p)
	}
	return o.terminal.Write(p)
}

// hold starts capturing what is written this way, and returns the function that
// stops capturing and hands back what was captured.
func (o *sessionOutput) hold() func() string {
	o.held = &bytes.Buffer{}
	return func() string {
		captured := o.held.String()
		o.held = nil
		return captured
	}
}
