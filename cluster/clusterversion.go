package cluster

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	hraft "github.com/hashicorp/raft"
	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/x/bsonx/bsoncore"

	"github.com/hoarddb/hoarddb-server/storage"
)

// ClusterVersionError is the refusal of a cluster version this build cannot run: a data directory,
// a snapshot, a BOOTSTRAP entry or a SET_CLUSTER_VERSION entry whose version is outside the build's
// range (cluster-version spec 4.4, and refusal 3 of 4.3). The node stops with DivergedExitCode, the status that means "wrong build for this
// directory, do not restart it". It unwraps to a CLUSTER_MISMATCH *Error, so a caller that
// branches on the code sees one.
type ClusterVersionError struct {
	// Version is the cluster version the directory, snapshot or entry is at, or moves to.
	Version uint8
	// Range is the range this build declares (only Min and Max are read).
	Range StateRange
	// Marker is the path of the CLUSTER_VERSION marker when Version was read from it alone (the
	// open check, with no raft snapshot at that version), "" otherwise.
	Marker string
	// Bootstrapped is set when Version is the one a BOOTSTRAP entry carries: the cluster began at a
	// version above this build's Max, so "finalized past this build" would be wrong.
	Bootstrapped bool
}

// Error is the text of 4.4: what the operator reads, ending with the exit status. It quotes no
// statement this release's grammar lacks, and words a level no release records by the state
// range hoarddb-server -v prints.
func (e *ClusterVersionError) Error() string {
	head := fmt.Sprintf("the cluster version is %d and this build runs versions %d to %d", e.Version, e.Range.Min, e.Range.Max)
	if e.Version > e.Range.Max {
		start := fmt.Sprintf("a build whose state_range includes %d (hoarddb-server -v prints it)", e.Version)
		if release := ReleaseOf(e.Version); release != "" {
			start = fmt.Sprintf("%s or later (hoarddb-server -v prints state_range)", release)
		}
		past := "the cluster was finalized past this build."
		if e.Bootstrapped {
			past = "the cluster was bootstrapped at a version past this build."
		}
		return fmt.Sprintf("%s: %s%s Start this node on %s. Keep the data directory. Exit code %d.", head, past, e.markerNote(), start, DivergedExitCode)
	}
	if release := ReleaseOf(e.Range.Min); release != "" {
		return fmt.Sprintf("%s: the cluster was not finalized at %s before this upgrade. Start this node again on %s, whose range includes %d, finalize the cluster to version %d while every member runs it, then upgrade. Keep the data directory. Exit code %d.",
			head, release, release, e.Version, e.Range.Min, DivergedExitCode)
	}
	return fmt.Sprintf("%s: the cluster was not finalized at version %d before this upgrade. Start this node again on a build whose state_range includes %d (hoarddb-server -v prints it), finalize the cluster to version %d while every member runs it, then upgrade. Keep the data directory. Exit code %d.",
		head, e.Range.Min, e.Version, e.Range.Min, DivergedExitCode)
}

// markerNote is the sentence that says the version came from the marker file, and what a marker
// above the raft state means, or "" when the version was not read from the marker alone.
func (e *ClusterVersionError) markerNote() string {
	if e.Marker == "" {
		return ""
	}
	return fmt.Sprintf(" The version was read from the marker file %s; if this node's raft state is at an older version, the marker is stale: the raft directory was lost or restored from an older backup. If it is, move %s aside and start this build again.", e.Marker, e.Marker)
}

// Unwrap gives the CLUSTER_MISMATCH refusal with the same text.
func (e *ClusterVersionError) Unwrap() error {
	return &Error{Code: CodeClusterMismatch, Message: e.Error()}
}

// The cluster version marker: one line in <dir>/CLUSTER_VERSION, beside the ring FORMAT marker,
// rewritten durably whenever an applied BOOTSTRAP, SET_CLUSTER_VERSION or restored snapshot
// changes the version. It is read before the node writes anything, so an older build on a
// directory the cluster finalized past it stops with the directory untouched. It is a lower
// bound: a crash between an apply and the rewrite leaves it one change behind, which the checks
// in Restore and Apply catch once the stores are open.
const (
	clusterVersionFile   = "CLUSTER_VERSION"
	clusterVersionPrefix = "hoarddb cluster version "
)

// ClusterVersionMarkerError is the refusal of a CLUSTER_VERSION marker this build did not write: a
// restart cannot fix the file, so the server stops with DivergedExitCode and the operator moves it
// aside.
type ClusterVersionMarkerError struct {
	// Path is the marker file.
	Path string
	// Found is what the file says, trimmed.
	Found string
}

// Error names the file, what it says, the line this build writes and the fix.
func (e *ClusterVersionMarkerError) Error() string {
	return fmt.Sprintf("the cluster version marker %s says %q, which is not a line this build writes (%q). "+
		"Move the file aside and start the node again: the node reads its cluster version from its raft state as well, and rewrites the marker. Exit code %d.",
		e.Path, e.Found, clusterVersionPrefix+"<version>", DivergedExitCode)
}

// readClusterVersionMarker returns the version the marker records, 0 when there is none.
func readClusterVersionMarker(dir string) (uint8, error) {
	path := filepath.Join(dir, clusterVersionFile)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading the cluster version marker %s: %w", path, err)
	}
	line, hasNewline := strings.CutSuffix(string(raw), "\n")
	digits, hasPrefix := strings.CutPrefix(line, clusterVersionPrefix)
	number, convErr := strconv.Atoi(digits)
	if !hasNewline || !hasPrefix || convErr != nil || number < 1 || number > 255 || strconv.Itoa(number) != digits {
		return 0, &ClusterVersionMarkerError{Path: path, Found: strings.TrimSpace(string(raw))}
	}
	return uint8(number), nil
}

// writeClusterVersionMarker records version durably: a temporary file, fsync, rename and a
// directory fsync.
func writeClusterVersionMarker(dir string, version uint8) error {
	path := filepath.Join(dir, clusterVersionFile)
	if err := storage.WriteFileDurably(path, []byte(clusterVersionPrefix+strconv.Itoa(int(version))+"\n")); err != nil {
		return fmt.Errorf("writing the cluster version marker %s: %w", path, err)
	}
	return nil
}

// snapshotStoreDir is the directory OpenRaft opens the library's snapshot store at, for a node
// whose directory is dir. The open check reads the files under it before the store is opened, so
// both take the path from here.
func snapshotStoreDir(dir string) string { return filepath.Join(dir, "raft", "snapshots") }

// snapshotDir is where the library keeps the snapshot directories: it adds its own "snapshots"
// directory under the store's.
func snapshotDir(dir string) string { return filepath.Join(snapshotStoreDir(dir), "snapshots") }

// newestSnapshotClusterVersion is the cluster version of the newest raft snapshot under dir, read
// from the files without opening the Raft store, 0 when there is none or it cannot be read (a
// damaged or foreign snapshot is the first look's and the library's to report). Newest is the
// library's own order (FileSnapshotStore.List): directories that are not temporary, with
// readable metadata of a supported version, by term, then index, then id.
func newestSnapshotClusterVersion(dir string) (uint8, error) {
	root := snapshotDir(dir)
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("listing the raft snapshots in %s: %w", root, err)
	}
	var newest *hraft.SnapshotMeta
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, entry.Name(), "meta.json"))
		if err != nil {
			continue
		}
		var meta hraft.SnapshotMeta
		if err := json.Unmarshal(raw, &meta); err != nil || meta.Version < hraft.SnapshotVersionMin || meta.Version > hraft.SnapshotVersionMax {
			continue
		}
		if newest == nil || compareSnapshotMeta(&meta, newest) > 0 {
			newest = &meta
		}
	}
	if newest == nil {
		return 0, nil
	}
	file, err := os.Open(filepath.Join(root, newest.ID, "state.bin"))
	if err != nil {
		return 0, nil
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxSnapshotBytes))
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, nil
	}
	version, _ := snapshotClusterVersion(raw)
	return version, nil
}

// compareSnapshotMeta orders two snapshots the way the library's snapMetaSlice does.
func compareSnapshotMeta(a, b *hraft.SnapshotMeta) int {
	if c := cmp.Compare(a.Term, b.Term); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Index, b.Index); c != 0 {
		return c
	}
	return cmp.Compare(a.ID, b.ID)
}

// snapshotClusterVersion reads the cluster version out of a snapshot file without decoding the
// rest: the magic, the length and the checksum are checked, then the body's second field. A later
// build's snapshot that this build cannot decode still names its version this way, which is what
// lets the refusal say which build to run. ok is false for a file that is not a snapshot or
// carries no cluster version there.
func snapshotClusterVersion(raw []byte) (version uint8, ok bool) {
	if len(raw) < snapshotHeaderBytes+snapshotCRCBytes || !bytes.Equal(raw[:4], []byte(snapshotMagic)) {
		return 0, false
	}
	bodyLen := binary.BigEndian.Uint32(raw[24:])
	if uint64(bodyLen)+snapshotHeaderBytes+snapshotCRCBytes != uint64(len(raw)) {
		return 0, false
	}
	crcAt := len(raw) - snapshotCRCBytes
	if crc32.Checksum(raw[:crcAt], snapshotCRCTable) != binary.BigEndian.Uint32(raw[crcAt:]) {
		return 0, false
	}
	body := bsoncore.Document(raw[snapshotHeaderBytes:crcAt])
	if body.Validate() != nil {
		return 0, false
	}
	elems, err := body.Elements()
	if err != nil || len(elems) < 2 || elems[1].Key() != "cluster_version" || elems[1].Value().Type != bson.TypeInt32 {
		return 0, false
	}
	number, isInt := elems[1].Value().Int32OK()
	if !isInt || number < 1 || number > 255 {
		return 0, false
	}
	return uint8(number), true
}

// CheckDataDirClusterVersion is the open check of cluster-version spec 4.4: the version a node's
// directory is at, the larger of the marker and the newest raft snapshot, against the build's Max.
// It reads files only and writes nothing, so OpenNode runs it before anything touches the
// directory; a directory with neither is not judged here (the Restore and Apply checks judge what
// the log holds). The refusal is a *ClusterVersionError.
//
// Only the upper bound. Both files are lower bounds of the version the directory's log ends at:
// the marker is the version at its last rewrite (one change behind after a crash between an apply
// and the rewrite) and a snapshot is the version when it was taken. The version only rises, so a
// value above Max proves the final one is above Max, and a value below Min proves nothing: every
// value a node holds locally is at most the cluster's committed version, so the lower bound is
// judged only by a holder of that version (cluster-version spec 4.4).
func CheckDataDirClusterVersion(dir string, levels StateRange) error {
	marker, err := readClusterVersionMarker(dir)
	if err != nil {
		return err
	}
	snapshot, err := newestSnapshotClusterVersion(dir)
	if err != nil {
		return err
	}
	version := slices.Max([]uint8{marker, snapshot})
	if version <= levels.Max {
		return nil
	}
	refusal := &ClusterVersionError{Version: version, Range: levels}
	if marker > snapshot {
		refusal.Marker = filepath.Join(dir, clusterVersionFile)
	}
	return refusal
}
