package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// A position a peer holds in this log is a fact about one life of it (spec
// §19.3.2 as §20.4 replaces its table). The log keeps a chain of links in its
// directory, oldest first; the last link is the current incarnation, and every
// open of the log mints one:
//
//	OpenWAL finds no segment and no chain   {new id, base 0}
//	OpenWAL finds segments and no chain     {new id, base = last sequence found, legacy}
//	OpenWAL finds a chain                   {new id, base = max(last sequence found, previous base)}
//
// base is the length of the prefix the link shares with the log before it: the
// last sequence found when the log was opened, which is the only base that is
// right, because the log grows after it opens and a base read later would
// count what this process wrote as shared with the last one. A position learned
// under link j is valid up to the lowest base of any later link (ValidThrough).
// Nothing mints while the log is open, and nothing a peer says makes it mint.
//
// The chain grows by one link per process start and is not compacted in 1.0.
//
// Beside the links the file keeps the log's skips (BUG-0481): runs of
// sequences the log passed over without writing, because a peer reported a
// position in this log above where the log ended (a VM snapshot restore, or a
// lost log). A sequence in a skip is never written, so a reader that finds the
// next entry past a skip is not looking at a hole, and the next open starts
// above the highest skip as it starts above the last entry.

// WALIncarnation is one link of a log's incarnation chain.
type WALIncarnation struct {
	// ID is random and names this life of the log; it is not derived from a
	// node id, which survives a wipe.
	ID [16]byte
	// Base is the last sequence the log held when this link was minted.
	Base uint64
	// Legacy marks the link minted on the first open of a log written before
	// chains existed: positions peers learned then carry no id at all.
	Legacy bool
}

// walIncarnationFile is the chain's file in the log's directory.
const walIncarnationFile = "incarnation.bson"

// walIncarnationLinkLen is one link on disk: the id, the base as eight
// big-endian bytes, and a flags byte whose low bit is Legacy.
const walIncarnationLinkLen = 16 + 8 + 1

// WALSkip is a run of sequences the log passed over without writing: every
// sequence above After and at or below Through.
type WALSkip struct {
	After   uint64
	Through uint64
}

// walSkipLen is one skip on disk: After and Through as eight big-endian bytes
// each.
const walSkipLen = 16

// walIncarnationChain is the file's BSON document. The links are one packed
// binary value, not an array of documents, so a link costs its 25 bytes and not
// the 53 a document per link would. Skips are packed the same way, 16 bytes
// each, and absent while the log has none: a file written before skips existed
// reads as a log with none.
type walIncarnationChain struct {
	Links []byte `bson:"links"`
	Skips []byte `bson:"skips,omitempty"`
}

// Incarnations returns the log's incarnation chain, oldest first. The chain is
// fixed for as long as the log is open.
func (w *WAL) Incarnations() []WALIncarnation {
	return append([]WALIncarnation(nil), w.incarnations...)
}

// LogID returns the id of the chain's first link. It names the log across
// every restart and changes only when the log is created again.
func (w *WAL) LogID() [16]byte {
	return w.incarnations[0].ID
}

// Incarnation returns the id of the current link: the incarnation every
// position this process hands out is learned under.
func (w *WAL) Incarnation() [16]byte {
	return w.incarnations[len(w.incarnations)-1].ID
}

// ValidThrough returns how far a position learned under the link reported
// names is valid in the log the chain describes: up to the lowest base of any
// later link, or without limit (math.MaxUint64) when reported is the current
// link. An empty reported id is a position learned before chains existed, and
// is found only when the chain's first link is legacy. found is false for an
// id the chain does not hold.
func ValidThrough(chain []WALIncarnation, reported []byte) (limit uint64, found bool) {
	learnedUnder := -1
	switch {
	case len(reported) == 0:
		if len(chain) > 0 && chain[0].Legacy {
			learnedUnder = 0
		}
	default:
		for i := range chain {
			if bytes.Equal(chain[i].ID[:], reported) {
				learnedUnder = i
				break
			}
		}
	}
	if learnedUnder < 0 {
		return 0, false
	}
	limit = math.MaxUint64
	for _, later := range chain[learnedUnder+1:] {
		limit = min(limit, later.Base)
	}
	return limit, true
}

// mintIncarnation appends a link to the chain in dir and makes the file
// durable — written through a temporary file, fsynced, renamed, and the
// directory fsynced — before it returns. foundSegments says whether the log
// had any segment when it was opened, which decides whether a log with no
// chain is new or was written before chains existed. The link's base is the
// highest of the last sequence found, the top of the highest skip, and the
// previous link's base: a skip recorded by a process that then wrote nothing is
// still passed over, and a log that reads back less than an earlier process
// found never gives a sequence peers may already hold again (the RC-4 floor,
// spec §3.4 step 4). A floor above the entries on disk shows up as a hole that
// ReadBatch reports, where a sequence given twice would be silent divergence.
// In a consistent log the floor is never above the last sequence found.
func mintIncarnation(dir string, lastSeq uint64, foundSegments bool) ([]WALIncarnation, []WALSkip, error) {
	chain, skips, found, err := loadIncarnationChain(dir)
	if err != nil {
		return nil, nil, err
	}
	if len(skips) > 0 {
		lastSeq = max(lastSeq, skips[len(skips)-1].Through)
	}
	if len(chain) > 0 {
		lastSeq = max(lastSeq, chain[len(chain)-1].Base)
	}
	link := WALIncarnation{Base: lastSeq, Legacy: !found && foundSegments}
	if _, err := rand.Read(link.ID[:]); err != nil {
		return nil, nil, fmt.Errorf("minting an incarnation of the log in %s: %w", dir, err)
	}
	chain = append(chain, link)
	if err := writeIncarnationChain(dir, chain, skips); err != nil {
		return nil, nil, fmt.Errorf("recording a new incarnation of the log in %s: %w", dir, err)
	}
	return chain, skips, nil
}

// writeIncarnationChain replaces the chain file in dir with chain and skips,
// durably.
func writeIncarnationChain(dir string, chain []WALIncarnation, skips []WALSkip) error {
	encoded, err := bson.Marshal(walIncarnationChain{Links: encodeIncarnationLinks(chain), Skips: encodeSkips(skips)})
	if err != nil {
		return fmt.Errorf("encoding the incarnation chain of the log in %s: %w", dir, err)
	}
	return WriteFileDurably(filepath.Join(dir, walIncarnationFile), encoded)
}

// loadIncarnationChain reads the chain and the skips in dir; found is false
// when there is no chain. A file that is there and cannot be read is an error
// and never an empty chain: an empty chain would make this log a new one, and
// every peer's position in it would be thrown away or, worse, trusted.
func loadIncarnationChain(dir string) (chain []WALIncarnation, skips []WALSkip, found bool, err error) {
	path := filepath.Join(dir, walIncarnationFile)
	stored, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, fmt.Errorf("reading %s: %w", path, err)
	}
	var document walIncarnationChain
	if err := bson.Unmarshal(stored, &document); err != nil {
		return nil, nil, false, incarnationFileDamaged(path, err)
	}
	if len(document.Links) == 0 || len(document.Links)%walIncarnationLinkLen != 0 {
		return nil, nil, false, incarnationFileDamaged(path, fmt.Errorf("its links are %d bytes, not a whole number of %d-byte links",
			len(document.Links), walIncarnationLinkLen))
	}
	skips, err = decodeSkips(document.Skips)
	if err != nil {
		return nil, nil, false, incarnationFileDamaged(path, err)
	}
	return decodeIncarnationLinks(document.Links), skips, true, nil
}

// encodeSkips packs skips, or returns nil for none so the field is left out.
func encodeSkips(skips []WALSkip) []byte {
	if len(skips) == 0 {
		return nil
	}
	encoded := make([]byte, 0, len(skips)*walSkipLen)
	for _, skip := range skips {
		encoded = binary.BigEndian.AppendUint64(encoded, skip.After)
		encoded = binary.BigEndian.AppendUint64(encoded, skip.Through)
	}
	return encoded
}

// decodeSkips unpacks skips and refuses any that are not ascending runs that
// neither touch nor overlap: a reader passes over exactly what a skip names,
// so a skip read wrong would hide a real hole in the log.
func decodeSkips(encoded []byte) ([]WALSkip, error) {
	if len(encoded)%walSkipLen != 0 {
		return nil, fmt.Errorf("its skips are %d bytes, not a whole number of %d-byte skips", len(encoded), walSkipLen)
	}
	skips := make([]WALSkip, 0, len(encoded)/walSkipLen)
	for start := 0; start < len(encoded); start += walSkipLen {
		skip := WALSkip{After: binary.BigEndian.Uint64(encoded[start:]), Through: binary.BigEndian.Uint64(encoded[start+8:])}
		if skip.Through <= skip.After {
			return nil, fmt.Errorf("its skip after %d ends at %d, which is not above it", skip.After, skip.Through)
		}
		if len(skips) > 0 && skip.After <= skips[len(skips)-1].Through {
			return nil, fmt.Errorf("its skip after %d does not start above the skip before it, which ends at %d", skip.After, skips[len(skips)-1].Through)
		}
		skips = append(skips, skip)
	}
	return skips, nil
}

// pastSkip is seq, or the first sequence after the skip seq lies in. Skips
// never touch, so one pass finds it.
func pastSkip(skips []WALSkip, seq uint64) uint64 {
	for _, skip := range skips {
		if seq > skip.After && seq <= skip.Through {
			return skip.Through + 1
		}
	}
	return seq
}

// incarnationFileDamaged is the error for a chain that cannot be read, with
// what an operator can do about it.
func incarnationFileDamaged(path string, cause error) error {
	return fmt.Errorf("%s cannot be read: %w. It names this node's write-ahead log to its peers, and without it the node "+
		"cannot tell which of their positions in the log are current. %s", path, cause,
		adviceForDirectory(filepath.Dir(path), "Restore the file from a copy of this data directory, or keep this data directory"))
}

func encodeIncarnationLinks(chain []WALIncarnation) []byte {
	encoded := make([]byte, 0, len(chain)*walIncarnationLinkLen)
	for _, link := range chain {
		encoded = append(encoded, link.ID[:]...)
		encoded = binary.BigEndian.AppendUint64(encoded, link.Base)
		var flags byte
		if link.Legacy {
			flags = 1
		}
		encoded = append(encoded, flags)
	}
	return encoded
}

func decodeIncarnationLinks(encoded []byte) []WALIncarnation {
	chain := make([]WALIncarnation, 0, len(encoded)/walIncarnationLinkLen)
	for start := 0; start < len(encoded); start += walIncarnationLinkLen {
		link := encoded[start : start+walIncarnationLinkLen]
		var decoded WALIncarnation
		copy(decoded.ID[:], link[:16])
		decoded.Base = binary.BigEndian.Uint64(link[16:24])
		decoded.Legacy = link[24]&1 != 0
		chain = append(chain, decoded)
	}
	return chain
}
