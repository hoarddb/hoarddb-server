package main

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-server/storage"
)

type verdictKind int

const (
	verdictEqual verdictKind = iota
	verdictDifferent
	verdictNotComparable
)

func (kind verdictKind) String() string {
	switch kind {
	case verdictEqual:
		return "equal"
	case verdictDifferent:
		return "different"
	default:
		return "not comparable"
	}
}

// pairVerdict is the answer for one pair held by more than one node.
type pairVerdict struct {
	Slot     uint16
	Database uint64
	Kind     verdictKind
	Detail   string
	// Nodes are the nodes the verdict is about, in the order the digest files were given.
	Nodes []string
}

// heldPair is one node's digest of one pair.
type heldPair struct {
	node string
	pair pairDigest
}

// compareDigests judges every pair held by more than one of the nodes. A retired or retiring
// pair is no longer held by its node and is left out; a pair held by one node is not a
// comparison and is left out too. The result is ordered by (slot, database).
func compareDigests(files []digestFile) []pairVerdict {
	held := map[pairKey][]heldPair{}
	for _, file := range files {
		for _, pair := range file.Pairs {
			if pair.State == "retired" || pair.State == "retiring" {
				continue
			}
			key := pairKey{pair.Slot, pair.Database}
			held[key] = append(held[key], heldPair{file.Node, pair})
		}
	}
	var verdicts []pairVerdict
	for key, copies := range held {
		if len(copies) > 1 {
			verdicts = append(verdicts, judgePair(key, copies))
		}
	}
	slices.SortFunc(verdicts, func(left, right pairVerdict) int {
		return cmp.Or(cmp.Compare(left.Slot, right.Slot), cmp.Compare(left.Database, right.Database))
	})
	return verdicts
}

func judgePair(key pairKey, copies []heldPair) pairVerdict {
	verdict := pairVerdict{Slot: key.slot, Database: key.database}
	for _, held := range copies {
		verdict.Nodes = append(verdict.Nodes, held.node)
	}
	if reason := notComparableReason(copies); reason != "" {
		verdict.Kind, verdict.Detail = verdictNotComparable, reason
		return verdict
	}
	first := copies[0].pair
	for _, other := range copies[1:] {
		if other.pair.Digest != first.Digest || other.pair.LiveKeys != first.LiveKeys {
			verdict.Kind = verdictDifferent
			verdict.Detail = describeCopies(copies, func(pair pairDigest) string {
				return fmt.Sprintf("%d keys, digest %.12s", pair.LiveKeys, pair.Digest)
			})
			return verdict
		}
	}
	verdict.Kind = verdictEqual
	verdict.Detail = fmt.Sprintf("%d nodes, %d keys, applied %d:%d", len(copies), first.LiveKeys, first.AppliedEpoch, first.AppliedSeq)
	return verdict
}

// notComparableReason says why the content of these copies cannot be judged, or "" when it can:
// a copy that is still seeding or has no pair record holds an incomplete or unrecorded copy,
// copies at different positions have applied different numbers of writes, and a copy with delete
// sweeps pending still holds keys of documents the others have removed.
func notComparableReason(copies []heldPair) string {
	for _, held := range copies {
		switch held.pair.State {
		case "seeding":
			return fmt.Sprintf("%s is still seeding this pair; wait until it is ready and stop it again", held.node)
		case "no-record":
			return fmt.Sprintf("%s holds keys of this pair and no pair record, so there is no position to compare", held.node)
		}
	}
	first := copies[0].pair
	for _, other := range copies[1:] {
		if other.pair.AppliedEpoch != first.AppliedEpoch || other.pair.AppliedSeq != first.AppliedSeq {
			return "positions differ (" + describeCopies(copies, func(pair pairDigest) string {
				return fmt.Sprintf("applied %d:%d", pair.AppliedEpoch, pair.AppliedSeq)
			}) + "); stop the load, let the copies catch up, then digest again"
		}
	}
	for _, held := range copies {
		if held.pair.ReclaimPending > 0 {
			return fmt.Sprintf("%s still has %d deleted documents to sweep, which leaves keys the other copy has already removed; wait for the sweep, stop the node again and digest again",
				held.node, held.pair.ReclaimPending)
		}
	}
	return ""
}

// singleHolderPairs counts the pairs only one of the files holds. They are not compared (the
// nodes of a larger cluster share only some pairs), but a copy that lost a whole pair shows up
// here, so the summary says how many there are.
func singleHolderPairs(files []digestFile) int {
	holders := map[pairKey]int{}
	for _, file := range files {
		for _, pair := range file.Pairs {
			if pair.State != "retired" && pair.State != "retiring" {
				holders[pairKey{pair.Slot, pair.Database}]++
			}
		}
	}
	single := 0
	for _, count := range holders {
		if count == 1 {
			single++
		}
	}
	return single
}

func describeCopies(copies []heldPair, describe func(pairDigest) string) string {
	parts := make([]string, len(copies))
	for index, held := range copies {
		parts[index] = held.node + " " + describe(held.pair)
	}
	return strings.Join(parts, ", ")
}

// keyDifference is one key the two copies of a pair do not share.
type keyDifference struct {
	Key []byte
	// MissingOnSecond and MissingOnFirst say which copy lacks the key; both false means the
	// key is in both with different values, of FirstBytes and SecondBytes bytes.
	MissingOnFirst, MissingOnSecond bool
	FirstBytes, SecondBytes         int
}

// String names the key and the difference, calling the copies "first" and "second".
func (difference keyDifference) String() string { return difference.Describe("first", "second") }

// Describe names the key and the difference using the two node names.
func (difference keyDifference) Describe(first, second string) string {
	switch {
	case difference.MissingOnFirst:
		return fmt.Sprintf("missing on %s: %s", first, describeKey(difference.Key))
	case difference.MissingOnSecond:
		return fmt.Sprintf("missing on %s: %s", second, describeKey(difference.Key))
	default:
		return fmt.Sprintf("value differs (%s %d bytes, %s %d bytes): %s", first, difference.FirstBytes, second, difference.SecondBytes, describeKey(difference.Key))
	}
}

// describeKey spells a storage key as the slot, database, bucket, namespace and body it holds.
func describeKey(key []byte) string {
	if len(key) <= namespaceOffset {
		return fmt.Sprintf("key %x", key)
	}
	namespace := namespaceNames[key[namespaceOffset]]
	if namespace == "" {
		namespace = fmt.Sprintf("namespace 0x%02x", key[namespaceOffset])
	}
	return fmt.Sprintf("slot %d database %d bucket %d %s %s",
		binary.BigEndian.Uint16(key[0:2]), binary.BigEndian.Uint64(key[2:10]), binary.BigEndian.Uint64(key[10:18]),
		namespace, strconv.Quote(string(key[namespaceOffset+1:])))
}

// firstDifferences opens two stopped store directories read-only and returns up to limit keys
// of the pair (slot, database) that the copies do not share or hold with different values, in
// key order.
func firstDifferences(firstDir, secondDir string, slot uint16, database uint64, limit int) ([]keyDifference, error) {
	firstStore, err := openReadOnly(firstDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = firstStore.Close() }() // read-only: nothing to flush
	secondStore, err := openReadOnly(secondDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = secondStore.Close() }() // read-only: nothing to flush

	prefix := storage.PairPrefix(slot, database)
	first := newReplicatedCursor(firstStore, prefix, map[string]uint64{})
	defer first.close()
	second := newReplicatedCursor(secondStore, prefix, map[string]uint64{})
	defer second.close()
	haveFirst, haveSecond := first.advance(), second.advance()
	var differences []keyDifference
	for (haveFirst || haveSecond) && len(differences) < limit {
		order := 0
		switch {
		case !haveSecond:
			order = -1
		case !haveFirst:
			order = 1
		default:
			order = bytes.Compare(first.key, second.key)
		}
		switch {
		case order < 0:
			differences = append(differences, keyDifference{Key: bytes.Clone(first.key), MissingOnSecond: true})
			haveFirst = first.advance()
		case order > 0:
			differences = append(differences, keyDifference{Key: bytes.Clone(second.key), MissingOnFirst: true})
			haveSecond = second.advance()
		default:
			if !bytes.Equal(first.value, second.value) {
				differences = append(differences, keyDifference{Key: bytes.Clone(first.key), FirstBytes: len(first.value), SecondBytes: len(second.value)})
			}
			haveFirst, haveSecond = first.advance(), second.advance()
		}
	}
	if first.err != nil || second.err != nil {
		return nil, fmt.Errorf("second pass over slot %d database %d: %w", slot, database, firstError(first.err, second.err))
	}
	return differences, nil
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
