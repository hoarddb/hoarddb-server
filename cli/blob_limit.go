package cli

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/config"
)

// checkBlobFitsAServer refuses a blob of size bytes that no hoardDB server
// could accept, before any of it is sent. It takes a size, not the bytes, so
// a caller holding a file can pass its stat size and refuse before reading it.
//
// It checks the hard ceiling only. A node's configured cap
// (storage.max_blob_bytes, 16 MiB unless raised) is node configuration the
// protocol does not carry, so learning it would take a new round trip or a
// new field; the server enforces it, and its refusal names the value it is
// running with. What the ceiling check buys is never sending a payload that
// cannot fit a frame.
func checkBlobFitsAServer(key string, size int64) error {
	if size <= config.MaxBlobBytesCeiling {
		return nil
	}
	// The message is a sentence for the developer, so it is built apart from
	// the error string that lint rules hold to a lowercase, unpunctuated form.
	refusal := fmt.Sprintf(`Blob %q is %s; the largest blob any hoardDB server accepts is %s (a server's own limit is storage.max_blob_bytes, 16 MiB unless its operator raised it). For larger files, keep the file on your own storage and store a pointer document — see "Large file stores" in the docs.`,
		key, humanBytes(size), humanBytes(config.MaxBlobBytesCeiling))
	return fmt.Errorf("%s", refusal)
}
