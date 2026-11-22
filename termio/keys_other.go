//go:build !linux

package termio

import (
	"errors"
	"os"
)

// Keys exists on platforms without a termios implementation so that callers
// compile, but NewKeys always refuses: without raw mode every key would wait for
// Enter, and a pager that cannot read a keypress one at a time is not paging.
// Callers treat the failure as "print everything and page nothing".
type Keys struct{}

// ErrNoRawMode is returned by NewKeys where the platform has no way to read
// single keystrokes.
var ErrNoRawMode = errors.New("this platform has no termios, so keys cannot be read one at a time")

// NewKeys refuses on this platform.
func NewKeys(f *os.File) (*Keys, error) { return nil, ErrNoRawMode }

// Read can never be reached, because NewKeys refuses first.
func (k *Keys) Read() (string, error) { return "", ErrNoRawMode }

// Close is a no-op: nothing was changed to restore.
func (k *Keys) Close() error { return nil }
