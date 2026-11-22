//go:build linux

package termio

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// escapeTimeout is how long a read that has seen ESC waits for the rest of a
// sequence before deciding the key was a real Escape. Every key that arrives as
// an escape sequence — the arrows, Home/End, Page Up/Down, the function keys —
// starts with ESC followed by more bytes, and the two cases cannot be told
// apart without waiting: a terminal sends nothing after a real Escape key.
const escapeTimeout = 50 * time.Millisecond

// maxKeyBytes bounds an escape sequence. A CSI sequence is ESC '[' <parameters>
// <final>, and the longest key this reader has to carry whole is six bytes.
const maxKeyBytes = 8

// escByte is the byte every escape sequence starts with.
const escByte = 0x1b

// Keys hands over one keystroke at a time, for a program that consumes keys
// rather than lines. Raw mode is on from NewKeys until Close: the canonical line
// discipline would otherwise hold every key back until the user pressed Enter,
// which turns a pager into a prompt.
type Keys struct {
	f        *os.File
	original *unix.Termios
}

// NewKeys puts f's terminal into raw mode, with echo off and ISIG off: Ctrl-C
// then arrives as a byte the caller can treat as "quit paging" rather than a
// signal that kills the process mid-screen and leaves the terminal in whatever
// state paging had put it in.
func NewKeys(f *os.File) (*Keys, error) {
	fd := int(f.Fd())
	original, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return nil, fmt.Errorf("read terminal attributes: %w", err)
	}
	raw := *original
	raw.Lflag &^= unix.ICANON | unix.ECHO | unix.ISIG
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &raw); err != nil {
		return nil, fmt.Errorf("enter raw mode: %w", err)
	}
	return &Keys{f: f, original: original}, nil
}

// Read returns the next keystroke. An ordinary key is one byte; a key that
// arrives as an escape sequence comes back whole.
//
// Returning the first byte alone is not an option: an arrow key starts with ESC,
// so a caller that saw only that byte would read an arrow as "quit" and leave
// "[B" in the terminal, where it lands in the next prompt. So a read that starts
// with ESC waits escapeTimeout for the rest and returns the whole sequence,
// while a read followed by nothing within the timeout is a real Escape key.
func (k *Keys) Read() (string, error) {
	first, err := k.readByte()
	if err != nil {
		return "", err
	}
	if first != escByte {
		return string(first), nil
	}

	seq := []byte{escByte}
	for len(seq) < maxKeyBytes {
		next, arrived, err := k.readByteWithin(escapeTimeout)
		if err != nil {
			return "", err
		}
		if !arrived {
			break
		}
		seq = append(seq, next)
		if sequenceComplete(seq) {
			break
		}
	}
	return string(seq), nil
}

// readByte reads one byte, blocking until one arrives.
func (k *Keys) readByte() (byte, error) {
	for {
		var b [1]byte
		n, err := k.f.Read(b[:])
		if err != nil {
			return 0, fmt.Errorf("read key: %w", err)
		}
		if n == 1 {
			return b[0], nil
		}
	}
}

// readByteWithin returns the next byte if one arrives within d, and reports
// whether it did. unix.Poll is used rather than the terminal's own VTIME,
// because VTIME counts whole deciseconds and cannot express this wait.
func (k *Keys) readByteWithin(d time.Duration) (b byte, arrived bool, err error) {
	fd := int32(k.f.Fd())
	poll := []unix.PollFd{{Fd: fd, Events: unix.POLLIN}}
	n, err := unix.Poll(poll, int(d.Milliseconds()))
	if err != nil {
		if err == unix.EINTR {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("wait for key: %w", err)
	}
	if n == 0 {
		return 0, false, nil
	}
	var buf [1]byte
	read, err := k.f.Read(buf[:])
	if err != nil {
		return 0, false, fmt.Errorf("read key: %w", err)
	}
	if read == 0 {
		return 0, false, nil
	}
	return buf[0], true, nil
}

// sequenceComplete reports whether seq is a whole escape sequence rather than a
// prefix of one.
func sequenceComplete(seq []byte) bool {
	if len(seq) < 2 {
		return false
	}
	switch seq[1] {
	case '[':
		// CSI: parameters and intermediate bytes, then one final byte in @..~.
		if len(seq) < 3 {
			return false
		}
		last := seq[len(seq)-1]
		return last >= 0x40 && last <= 0x7E
	case 'O':
		// SS3: exactly one byte follows the introducer.
		return len(seq) >= 3
	}
	// ESC followed by anything else — an Alt-key, or a terminal's own two-byte
	// sequence: the pair is the whole key.
	return true
}

// Close restores the terminal's attributes. The caller must reach it on every
// exit path, including a failed read, or the user's shell is left without echo.
func (k *Keys) Close() error {
	if err := unix.IoctlSetTermios(int(k.f.Fd()), unix.TCSETS, k.original); err != nil {
		return fmt.Errorf("restore terminal attributes: %w", err)
	}
	return nil
}
