package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
)

// AssertBadgerStructureEncrypted verifies that the node's store, the one Badger
// directory under dataDir, was encrypted. This is the §7.2 rule-2 structural
// assertion: it parses the MANIFEST and requires KeyID != 0 for every table,
// then opens the key registry with an empty key and requires
// ErrEncryptionKeyMismatch.
//
// Every engine lives in that one store (shared-storage build Steps 8 to 13),
// so one call proves every bucket's documents and index entries are sealed —
// and proves nothing about blob payload files, which are outside it (spec §5.5).
// It takes the data directory, not a Badger path, so a caller cannot point it
// at a directory that is not the store.
func AssertBadgerStructureEncrypted(t testingT, dataDir string) {
	for _, problem := range storeStructureProblems(filepath.Join(dataDir, nodeStoreDirName)) {
		t.Errorf("%s", problem)
	}
}

// storeStructureProblems is the structural check behind both the acceptance
// assertion and the status the node reports: everything wrong with the claim
// "this store is sealed", empty when the claim holds. A file it cannot read is
// a problem, never a pass, so a status built on it fails closed.
//
// It reads the MANIFEST, KEYREGISTRY and log heads of the Badger store in
// badgerPath without opening it, so it is safe beside a running Badger: the
// node computes it once for each store, right after the store opens, and a
// require node runs it on the ring store before it opens (ring encryption at
// rest spec §5.7).
func storeStructureProblems(badgerPath string) []string {
	manifestPath := filepath.Join(badgerPath, "MANIFEST")
	fp, err := os.Open(manifestPath)
	if err != nil {
		return []string{fmt.Sprintf("opening MANIFEST at %s: %v", manifestPath, err)}
	}
	defer func() { _ = fp.Close() }()

	// Parse the MANIFEST file. We pass empty Options since we're only reading metadata.
	manifest, _, err := badger.ReplayManifestFile(fp, 0, badger.DefaultOptions(""))
	if err != nil {
		return []string{fmt.Sprintf("parsing MANIFEST at %s: %v", manifestPath, err)}
	}

	var problems []string
	// A plaintext Badger directory has KeyID == 0 for all tables.
	for tableID, tableManifest := range manifest.Tables {
		if tableManifest.KeyID == 0 {
			problems = append(problems, fmt.Sprintf("table %d has KeyID == 0; expected encrypted table to have KeyID != 0", tableID))
		}
	}

	// Attempt to open the key registry with an empty key.
	// This must fail with ErrEncryptionKeyMismatch, proving that Badger
	// expected a key to decrypt its data.
	_, err = badger.OpenKeyRegistry(badger.KeyRegistryOptions{
		Dir:           badgerPath,
		EncryptionKey: []byte{}, // empty key to trigger mismatch
		ReadOnly:      true,
	})
	if !errors.Is(err, badger.ErrEncryptionKeyMismatch) {
		problems = append(problems, fmt.Sprintf("opening key registry with empty key: got %v, want ErrEncryptionKeyMismatch", err))
	}

	return append(problems, logFileKeyIDProblems(badgerPath)...)
}

// logFileKeyIDProblems is rule 2's third half. Badger starts every value
// log and memtable log with the id of the datakey that seals it
// (badger/v4 memtable.go:logFile.bootstrap writes keyID(8) | baseIV(12)), and
// a log written without a key records 0. The MANIFEST lists tables only, so a
// log opened without the key is invisible to the first two halves. A closed
// store may hold no .mem file at all (a memtable's log is deleted when it
// flushes); it always holds a value log, and none is a failure, because then
// this half checked nothing.
func logFileKeyIDProblems(badgerPath string) []string {
	valueLogs, err := filepath.Glob(filepath.Join(badgerPath, "*.vlog"))
	if err != nil {
		return []string{fmt.Sprintf("listing value logs in %s: %v", badgerPath, err)}
	}
	memtableLogs, err := filepath.Glob(filepath.Join(badgerPath, "*.mem"))
	if err != nil {
		return []string{fmt.Sprintf("listing memtable logs in %s: %v", badgerPath, err)}
	}
	var problems []string
	if len(valueLogs) == 0 {
		problems = append(problems, fmt.Sprintf("no value log in %s: the log files' key ids cannot be checked", badgerPath))
	}
	for _, logPath := range append(valueLogs, memtableLogs...) {
		keyID, err := readLogFileKeyID(logPath)
		if err != nil {
			problems = append(problems, fmt.Sprintf("reading the key id at the head of %s: %v", logPath, err))
			continue
		}
		if keyID == 0 {
			problems = append(problems, fmt.Sprintf("%s has key id 0: it was written without the store key", logPath))
		}
	}
	return problems
}

// readLogFileKeyID reads the big-endian datakey id in the first 8 bytes of a
// Badger log file. A file shorter than that is an error, not a zero.
func readLogFileKeyID(logPath string) (uint64, error) {
	logFile, err := os.Open(logPath)
	if err != nil {
		return 0, err
	}
	head := make([]byte, 8)
	_, readErr := io.ReadFull(logFile, head)
	if closeErr := logFile.Close(); closeErr != nil && readErr == nil {
		readErr = closeErr
	}
	if readErr != nil {
		return 0, readErr
	}
	return binary.BigEndian.Uint64(head), nil
}

// testingT is the minimal interface we need from *testing.T.
// This allows the helper to work in both test files and standalone assertion code.
type testingT interface {
	Errorf(format string, args ...interface{})
}
