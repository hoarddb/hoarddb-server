package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
)

// everyStoredKey returns every key a Badger instance holds, Badger's own
// internal keys included (InternalAccess), every version (AllVersions).
func everyStoredKey(t *testing.T, db *badger.DB) [][]byte {
	t.Helper()
	var keys [][]byte
	err := db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.IteratorOptions{AllVersions: true, InternalAccess: true})
		defer iterator.Close()
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			keys = append(keys, iterator.Item().KeyCopy(nil))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking every stored key: %v", err)
	}
	return keys
}

// TestBandIsDerivedFromBadger is spec T-28(c). badgerPrefix is unexported, so
// the band cannot be read from Badger at compile time; this test proves the
// literal it is derived from is still the prefix Badger reserves, and fails on
// a Badger upgrade that changes it instead of letting the band drift.
func TestBandIsDerivedFromBadger(t *testing.T) {
	// The store opens with the node's options plus a namespace offset, which
	// only enables BanNamespace below; see clause (2).
	opts, err := badgerOptions(t.TempDir(), BadgerTuning{MemTableMB: 32}, nil)
	if err != nil {
		t.Fatalf("building badger options: %v", err)
	}
	db, err := badger.Open(opts.WithNamespaceOffset(0))
	if err != nil {
		t.Fatalf("opening badger: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing badger: %v", err)
		}
	}()

	// (1) Badger refuses the literal as a key prefix.
	err = db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(badgerReservedPrefix+"x"), []byte("v"))
	})
	// Errorf, not Fatalf: a changed literal must fail clause (2) too, and
	// the report of both is what says which half moved.
	if !errors.Is(err, badger.ErrInvalidKey) {
		t.Errorf("setting %q returned %v, want badger.ErrInvalidKey: the band's literal is not what Badger reserves", badgerReservedPrefix+"x", err)
	}

	// (2) Every key Badger writes for itself lies inside the band. The spec's
	// source of such a key, the "!badger!txn" marker of a committed multi-key
	// transaction, never reaches an iterator on v4.9.6: memTable.Put writes
	// the marker (bitFinTxn) to the WAL and returns before the skiplist. So
	// the clause also bans a namespace, which writes "!badger!banned…" into
	// the LSM, where InternalAccess can see it; without a visible internal key
	// the clause could not fail.
	written := map[string]bool{"user-a-000": true, "user-b-000": true, "user-c-000": true}
	// Clause (1)'s key, if a changed literal let Badger accept it.
	clauseOneKey := badgerReservedPrefix + "x"
	err = db.Update(func(txn *badger.Txn) error {
		for key := range written {
			if err := txn.Set([]byte(key), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("committing a multi-key transaction: %v", err)
	}
	if err := db.BanNamespace(7); err != nil {
		t.Fatalf("banning a namespace: %v", err)
	}
	internal := 0
	for _, key := range everyStoredKey(t, db) {
		if written[string(key)] || string(key) == clauseOneKey {
			continue
		}
		internal++
		t.Logf("Badger-internal key %q", key)
		if len(key) < 8 {
			t.Fatalf("Badger's internal key %q is shorter than an id", key)
		}
		asID := binary.BigEndian.Uint64(key[:8])
		if asID < ReservedBandLow || asID >= ReservedBandHigh {
			t.Fatalf("Badger's internal key %q reads as id %#x, outside the reserved band [%#x, %#x)", key, asID, ReservedBandLow, ReservedBandHigh)
		}
	}
	if internal == 0 {
		t.Fatalf("the walk found no Badger-internal key, so clause (2) could not fail")
	}

	// (3) The allocator steps over the band.
	next, err := NextBucketID(ReservedBandLow - 1)
	if err != nil || next != ReservedBandHigh {
		t.Fatalf("NextBucketID(ReservedBandLow-1) = %#x, %v, want %#x", next, err, ReservedBandHigh)
	}
}

func TestNextBucketIDRefusesAtTheCeiling(t *testing.T) {
	if _, err := NextBucketID(MaxBucketID); !errors.Is(err, ErrBucketIDsExhausted) {
		t.Fatalf("NextBucketID(MaxBucketID) returned %v, want ErrBucketIDsExhausted", err)
	}
	next, err := NextBucketID(MaxBucketID - 1)
	if err != nil || next != MaxBucketID {
		t.Fatalf("NextBucketID(MaxBucketID-1) = %d, %v, want %d", next, err, MaxBucketID)
	}
	// An id past the ceiling must refuse too, never wrap to 1.
	if _, err := NextBucketID(DatabaseDropID); !errors.Is(err, ErrBucketIDsExhausted) {
		t.Fatalf("NextBucketID(DatabaseDropID) returned %v, want ErrBucketIDsExhausted", err)
	}
	if first, err := NextBucketID(0); err != nil || first != 1 {
		t.Fatalf("NextBucketID(0) = %d, %v, want 1", first, err)
	}
}

func TestSentinelsLieAboveTheCeiling(t *testing.T) {
	for name, sentinel := range map[string]uint64{"OrphanCursorID": OrphanCursorID, "DatabaseDropID": DatabaseDropID} {
		if sentinel <= MaxBucketID {
			t.Fatalf("%s = %#x is not above MaxBucketID %#x, so the allocator could hand it out", name, sentinel, MaxBucketID)
		}
	}
}

// TestEveryKeyComesFromOnePrefix builds one document key by hand and compares
// it with what Key, Scope and PairPrefix return, so the 19-byte data prefix
// and the 10-byte pair prefix appear in exactly one place.
func TestEveryKeyComesFromOnePrefix(t *testing.T) {
	const (
		slot       = uint16(0x0ABC)
		databaseID = uint64(0x0102030405060708)
		bucketID   = uint64(0x1112131415161718)
	)
	hand := []byte{0x0A, 0xBC, 1, 2, 3, 4, 5, 6, 7, 8, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, NSDocuments, 'd', 'o', 'c'}
	key := Key(slot, databaseID, bucketID, NSDocuments, []byte("doc"))
	if !bytes.Equal(key, hand) {
		t.Fatalf("Key = %x, want %x: slot, database id, bucket id, namespace, body", key, hand)
	}
	if scope := Scope(slot, databaseID, bucketID); !bytes.Equal(scope, hand[:18]) || !bytes.HasPrefix(key, scope) {
		t.Fatalf("Scope = %x, want the first 18 bytes %x of the key", scope, hand[:18])
	}
	if pair := PairPrefix(slot, databaseID); !bytes.Equal(pair, hand[:10]) || !bytes.HasPrefix(key, pair) {
		t.Fatalf("PairPrefix = %x, want the first 10 bytes %x of the key", pair, hand[:10])
	}
	node := NodeKey(NSCatalog, []byte("x"))
	if want := []byte{0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, NSCatalog, 'x'}; !bytes.Equal(node, want) {
		t.Fatalf("NodeKey = %x, want %x", node, want)
	}
}

// nodeRecordNamespaces returns every NS* constant declared in the block of
// keys.go that follows the comment "Namespaces under [NodeRecordSlot][0] only",
// by name. The block is read from the source and not listed here, so a
// namespace nobody remembered to list cannot escape the distinctness check.
func nodeRecordNamespaces(t *testing.T) map[string]int64 {
	t.Helper()
	return namespacesInBlock(t, "Namespaces under [NodeRecordSlot][0] only")
}

// bucketNamespaces is nodeRecordNamespaces for the block whose doc comment
// begins "Namespaces inside a bucket's range".
func bucketNamespaces(t *testing.T) map[string]int64 {
	t.Helper()
	return namespacesInBlock(t, "Namespaces inside a bucket's range")
}

// namespacesInBlock reads the const block of keys.go whose doc comment contains
// marker and returns its integer literals by name.
func namespacesInBlock(t *testing.T, marker string) map[string]int64 {
	t.Helper()
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "keys.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parsing keys.go: %v", err)
	}
	namespaces := make(map[string]int64)
	for _, declaration := range parsed.Decls {
		block, isGeneral := declaration.(*ast.GenDecl)
		if !isGeneral || block.Tok != token.CONST || block.Doc == nil {
			continue
		}
		if !strings.Contains(block.Doc.Text(), marker) {
			continue
		}
		for _, spec := range block.Specs {
			valueSpec, isValue := spec.(*ast.ValueSpec)
			if !isValue || len(valueSpec.Values) != len(valueSpec.Names) {
				t.Fatalf("a namespace constant in the block %q has no literal value of its own", marker)
			}
			for index, name := range valueSpec.Names {
				literal, isLiteral := valueSpec.Values[index].(*ast.BasicLit)
				if !isLiteral || literal.Kind != token.INT {
					t.Fatalf("%s is not declared as an integer literal", name.Name)
				}
				number, err := strconv.ParseInt(literal.Value, 0, 16)
				if err != nil {
					t.Fatalf("%s = %s: %v", name.Name, literal.Value, err)
				}
				namespaces[name.Name] = number
			}
		}
	}
	return namespaces
}

// TestNodeRecordNamespacesAreDistinct (T-74): the namespaces under
// [NodeRecordSlot][0] share one key space, so two equal bytes would make one
// record kind overwrite another's on disk. NSReplication is 0x0C and frozen:
// it is the byte every node that has replicated already carries.
func TestNodeRecordNamespacesAreDistinct(t *testing.T) {
	namespaces := nodeRecordNamespaces(t)
	if len(namespaces) < 6 {
		t.Fatalf("read %d node-record namespaces from keys.go (%v), want at least the six it declares", len(namespaces), namespaces)
	}
	owners := make(map[int64]string, len(namespaces))
	for name, number := range namespaces {
		if other, taken := owners[number]; taken {
			t.Errorf("%s and %s are both %#02x", name, other, number)
		}
		owners[number] = name
	}
	replication, declared := namespaces["NSReplication"]
	if !declared {
		t.Fatalf("NSReplication is not declared among the node-record namespaces %v", namespaces)
	}
	if replication != 0x0C {
		t.Errorf("NSReplication = %#02x, want 0x0C (spec §19.3.8)", replication)
	}
}

// TestBucketNamespacesAreDistinct (claim-ack-spec §3.1): the namespaces inside
// one bucket's range share a key space, so two equal bytes would make one
// record kind overwrite another's. NSDelivery is 0x0D, and 0x08 to 0x0C stay out
// of the bucket block: the node-record block and the gap note's local summaries
// claim them.
func TestBucketNamespacesAreDistinct(t *testing.T) {
	namespaces := bucketNamespaces(t)
	if len(namespaces) < 8 {
		t.Fatalf("read %d bucket namespaces from keys.go (%v), want at least the seven it declared before NSDelivery and NSDelivery itself", len(namespaces), namespaces)
	}
	owners := make(map[int64]string, len(namespaces))
	for name, number := range namespaces {
		if other, taken := owners[number]; taken {
			t.Errorf("%s and %s are both %#02x", name, other, number)
		}
		owners[number] = name
		if number >= 0x08 && number <= 0x0C {
			t.Errorf("%s = %#02x: 0x08 to 0x0C are not bucket namespaces (the node-record block and the local-summaries note use them)", name, number)
		}
	}
	delivery, declared := namespaces["NSDelivery"]
	if !declared {
		t.Fatalf("NSDelivery is not declared among the bucket namespaces %v", namespaces)
	}
	if delivery != 0x0D || NSDelivery != 0x0D {
		t.Errorf("NSDelivery = %#02x (declared %#02x), want 0x0D", NSDelivery, delivery)
	}
}

// TestBucketRefScopeMatchesScope: a BucketRef is the three arguments of Scope.
func TestBucketRefScopeMatchesScope(t *testing.T) {
	ref := BucketRef{Slot: 0x0ABC, DatabaseID: 7, BucketID: 9}
	if got, want := ref.Scope(), Scope(0x0ABC, 7, 9); !bytes.Equal(got, want) {
		t.Fatalf("BucketRef.Scope = %x, want %x", got, want)
	}
}
