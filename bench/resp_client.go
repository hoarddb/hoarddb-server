// bench/resp_client.go — Minimal RESP (Redis/Valkey) client for benchmarking.
//
// Why hand-rolled instead of a driver library: it keeps this benchmark
// dependency-free (an external driver would be one more license to vet against
// AGENTS.md's allowlist) and, more importantly, it exposes pipelining directly.
// Pipelining is the whole point of this file: it is the difference between
// measuring a round trip and measuring throughput.
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

type respConn struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func dialRESP(addr string) (*respConn, error) {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		// Nagle would coalesce and distort pipelined writes.
		_ = tc.SetNoDelay(true)
	}
	return &respConn{
		c: c,
		r: bufio.NewReaderSize(c, 1<<20),
		w: bufio.NewWriterSize(c, 1<<20),
	}, nil
}

func (rc *respConn) close() { _ = rc.c.Close() }

// queue appends a command to the write buffer but does NOT flush it. Not
// flushing is what makes pipelining possible: N queued commands become one
// network write and therefore one round trip.
func (rc *respConn) queue(args ...string) {
	_, _ = fmt.Fprintf(rc.w, "*%d\r\n", len(args))
	for _, a := range args {
		_, _ = fmt.Fprintf(rc.w, "$%d\r\n%s\r\n", len(a), a)
	}
}

func (rc *respConn) flush() error { return rc.w.Flush() }

// reply reads exactly one RESP reply. Returns the payload; "" means the reply
// carried no value (nil bulk / null array), which for GET means "not found".
func (rc *respConn) reply() (string, error) {
	line, err := rc.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", fmt.Errorf("empty reply")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case ':':
		return line[1:], nil
	case '-':
		return "", fmt.Errorf("server error: %s", line[1:])
	case '$':
		var n int
		if _, err := fmt.Sscanf(line[1:], "%d", &n); err != nil {
			return "", fmt.Errorf("bad bulk length %q: %w", line, err)
		}
		if n < 0 {
			return "", nil // nil bulk => key absent
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(rc.r, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	case '*':
		var n int
		if _, err := fmt.Sscanf(line[1:], "%d", &n); err != nil {
			return "", fmt.Errorf("bad array length %q: %w", line, err)
		}
		for i := 0; i < n; i++ {
			if _, err := rc.reply(); err != nil {
				return "", err
			}
		}
		return "", nil
	}
	return "", fmt.Errorf("unexpected reply %q", line)
}
