package transport

import (
	"fmt"
	"io"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// WriteFrame writes a frame to the given writer (a TCP+TLS connection).
func WriteFrame(w io.Writer, f *Frame) error {
	_, err := w.Write(f.Encode())
	return err
}

// WriteErrorResponse writes an OpError frame to the writer.
func WriteErrorResponse(w io.Writer, code ErrorCode, message string, requestID [RequestIDLen]byte) error {
	errDoc := bson.M{
		"code":    uint8(code),
		"message": message,
	}
	payload, err := bson.Marshal(errDoc)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}
	f := NewFrameWithID(OpError, requestID, payload)
	return WriteFrame(w, f)
}
