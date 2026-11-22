package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster/machine"
)

// The declaration types are the machine's, not copies: the machine cannot import
// store, so the one definition lives there and the store names it here. store.FieldEntry
// is already an engine read result, so a declared field is reached as
// machine.FieldEntry through FieldTable.Entries.
type (
	// FieldTable is the decoded packed field table of conv 19.
	FieldTable = machine.FieldTable
	// Pending is an alter's table (field 8), inert until its version is published.
	Pending = machine.Pending
	// Declaration is one declaration document, decoded.
	Declaration = machine.Declaration
	// RetiredID is a retired field id with the epoch of the alter that retired it.
	RetiredID = machine.RetiredID
)

// MaxFieldTableEntries is the entry cap of a field table, `_id_` included (conv 19).
const MaxFieldTableEntries = 1000

// SweptBytes is the size of the field 6 drop-progress bitmap: one bit per slot, 8192 slots.
const SweptBytes = 1024

// Declaration field ids (conv 19). Field 1 is `_id_` and has only the header key; field 7
// belongs to the reverse-index family.
const (
	declFieldBucketID uint16 = 2
	declFieldVersion  uint16 = 3
	declFieldState    uint16 = 4
	declFieldFields   uint16 = 5
	declFieldSwept    uint16 = 6
	declFieldPending  uint16 = 8
	declFieldIndexes  uint16 = 10
	// declFieldExpire is the bucket's expiry declaration (expiry spec §2.2). Absent means
	// no expiry, so a declaration written before expiry existed decodes unchanged.
	declFieldExpire uint16 = 11
	// declFieldReferences holds the fields declared 'of' another bucket (fetch-through).
	// Absent means none, so a declaration written without references decodes unchanged.
	// Field 12 belongs to the store aggregate.
	declFieldReferences uint16 = 13
)

// maxReferences caps the references of one bucket, the count the field 13 layout holds in
// one byte.
const maxReferences = 255

// referenceEntryBytes is [field_id:2 BE][bucket_id:8 BE].
const referenceEntryBytes = 10

// indexTableFormat is the version byte of field 10, the ordered-index table.
const indexTableFormat byte = 1

// expireTableFormat is the first byte of the field 11 table; a decoder refuses any other.
const expireTableFormat byte = 1

// maxExpireStores caps the expiring store fields of one bucket, the count the field 11
// layout holds in one byte.
const maxExpireStores = 64

const (
	fieldTableFormat = 1
	// minEntryBytes is [id:2][typeLen:1][nameLen:1], an entry with an empty type and name.
	minEntryBytes = 4
	retiredBytes  = 10
	pendingHeader = 8
)

// idEntry is entry 1 of every non-empty table: `_id_` is declared `any` in every bucket
// (conv 19). The decoder insists on it so a mutated name is refused rather than carried.
var idEntry = machine.FieldEntry{ID: 1, Type: "any", Name: "_id_"}

// BSON element type bytes of the stored value envelope (placement 3).
const (
	bsonString byte = 0x02
	bsonBinary byte = 0x05
	bsonInt64  byte = 0x12
)

// ErrMalformedDeclaration wraps every refusal to decode a declaration document or its
// packed field table.
var ErrMalformedDeclaration = errors.New("malformed declaration")

func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMalformedDeclaration, fmt.Sprintf(format, args...))
}

// EncodeFieldTable packs table as conv 19 lays it out. The caller has checked the table
// with the limits EncodeDeclaration enforces (entries, string lengths, retired count); a
// table beyond them does not round trip.
func EncodeFieldTable(table FieldTable) []byte {
	size := 1 + 2 + 2 + 2 + len(table.Retired)*retiredBytes
	for _, entry := range table.Entries {
		size += minEntryBytes + len(entry.Type) + len(entry.Name)
	}
	out := make([]byte, 0, size)
	out = append(out, fieldTableFormat)
	out = binary.BigEndian.AppendUint16(out, uint16(len(table.Entries)))
	out = binary.BigEndian.AppendUint16(out, table.NextFieldID)
	for _, entry := range table.Entries {
		out = binary.BigEndian.AppendUint16(out, entry.ID)
		out = append(out, byte(len(entry.Type)))
		out = append(out, entry.Type...)
		out = append(out, byte(len(entry.Name)))
		out = append(out, entry.Name...)
	}
	out = binary.BigEndian.AppendUint16(out, uint16(len(table.Retired)))
	for _, retired := range table.Retired {
		out = binary.BigEndian.AppendUint16(out, retired.ID)
		out = binary.BigEndian.AppendUint64(out, retired.Epoch)
	}
	return out
}

// DecodeFieldTable reads a table EncodeFieldTable wrote. It never panics, checks every
// count against the bytes left before allocating for it, and refuses trailing bytes, so
// an accepted input re-encodes to itself.
func DecodeFieldTable(encoded []byte) (FieldTable, error) {
	var table FieldTable
	if len(encoded) < 5 {
		return table, malformed("a field table is at least 5 bytes, got %d", len(encoded))
	}
	if encoded[0] != fieldTableFormat {
		return table, malformed("field table format %d, want %d", encoded[0], fieldTableFormat)
	}
	count := int(binary.BigEndian.Uint16(encoded[1:3]))
	table.NextFieldID = binary.BigEndian.Uint16(encoded[3:5])
	if count > MaxFieldTableEntries {
		return FieldTable{}, malformed("%d entries, the cap is %d", count, MaxFieldTableEntries)
	}
	rest := encoded[5:]
	if count*minEntryBytes > len(rest) {
		return FieldTable{}, malformed("%d entries do not fit in %d bytes", count, len(rest))
	}
	if count > 0 {
		table.Entries = make([]machine.FieldEntry, 0, count)
	}
	for position := 0; position < count; position++ {
		var entry machine.FieldEntry
		var err error
		entry, rest, err = decodeTableEntry(rest)
		if err != nil {
			return FieldTable{}, fmt.Errorf("entry %d: %w", position, err)
		}
		if position == 0 && entry != idEntry {
			return FieldTable{}, malformed("entry 1 is %d %q %q, conv 19 fixes it as 1 \"any\" \"_id_\"", entry.ID, entry.Type, entry.Name)
		}
		table.Entries = append(table.Entries, entry)
	}
	if len(rest) < 2 {
		return FieldTable{}, malformed("no retired count after %d entries", count)
	}
	retiredCount := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if retiredCount*retiredBytes != len(rest) {
		return FieldTable{}, malformed("%d retired ids need %d bytes, %d remain", retiredCount, retiredCount*retiredBytes, len(rest))
	}
	if retiredCount > 0 {
		table.Retired = make([]RetiredID, retiredCount)
	}
	for position := range table.Retired {
		table.Retired[position] = RetiredID{
			ID:    binary.BigEndian.Uint16(rest),
			Epoch: binary.BigEndian.Uint64(rest[2:]),
		}
		rest = rest[retiredBytes:]
	}
	return table, nil
}

func decodeTableEntry(rest []byte) (machine.FieldEntry, []byte, error) {
	var entry machine.FieldEntry
	if len(rest) < minEntryBytes {
		return entry, nil, malformed("truncated entry")
	}
	entry.ID = binary.BigEndian.Uint16(rest)
	typeLen := int(rest[2])
	rest = rest[3:]
	if len(rest) < typeLen+1 {
		return entry, nil, malformed("truncated type word")
	}
	entry.Type = string(rest[:typeLen])
	nameLen := int(rest[typeLen])
	rest = rest[typeLen+1:]
	if len(rest) < nameLen {
		return entry, nil, malformed("truncated field name")
	}
	entry.Name = string(rest[:nameLen])
	return entry, rest[nameLen:], nil
}

// checkFieldTable refuses a table EncodeFieldTable cannot pack faithfully.
func checkFieldTable(table FieldTable) error {
	if len(table.Entries) > MaxFieldTableEntries {
		return fmt.Errorf("%d fields, the cap is %d including _id_", len(table.Entries), MaxFieldTableEntries)
	}
	if len(table.Retired) > 0xFFFF {
		return fmt.Errorf("%d retired ids, the cap is 65535", len(table.Retired))
	}
	for _, entry := range table.Entries {
		if len(entry.Type) > 255 || len(entry.Name) > 255 {
			return fmt.Errorf("field %d: the type word and the name are at most 255 bytes each", entry.ID)
		}
	}
	return nil
}

// EncodeDeclaration returns the stored value (bsonType ‖ BSON value bytes) of every
// scalar field the declaration owns, keyed by field id: 2 to 5 always, 8 only while an
// alter's table is pending. Field 1 is the header (the caller's `_id_`) and it never
// writes field 6: a caller that rewrites a dropping declaration must use
// EncodeDeclarationWithSwept, or the drop progress is lost.
func EncodeDeclaration(decl Declaration) (map[uint16][]byte, error) {
	return EncodeDeclarationWithSwept(decl, nil)
}

// EncodeDeclarationWithSwept is EncodeDeclaration plus field 6. It writes the bitmap
// exactly when swept is non-nil, and refuses a bitmap on a declaration that is not
// dropping or one that is not SweptBytes long.
func EncodeDeclarationWithSwept(decl Declaration, swept []byte) (map[uint16][]byte, error) {
	if swept != nil {
		if decl.State != machine.StateDropping {
			return nil, malformed("a swept bitmap exists only while a bucket is dropping, and this one is %q", decl.State)
		}
		if len(swept) != SweptBytes {
			return nil, malformed("swept is %d bytes, want %d", len(swept), SweptBytes)
		}
	}
	if decl.State != machine.StateActive && decl.State != machine.StateDropping {
		return nil, fmt.Errorf("declaration state %q: want %q or %q", decl.State, machine.StateActive, machine.StateDropping)
	}
	if decl.Version < 0 {
		return nil, fmt.Errorf("declaration version %d is negative", decl.Version)
	}
	if err := checkFieldTable(decl.Fields); err != nil {
		return nil, fmt.Errorf("encoding the field table: %w", err)
	}
	var bucketID [8]byte
	binary.BigEndian.PutUint64(bucketID[:], decl.BucketID)
	stored := map[uint16][]byte{
		declFieldBucketID: storedBinary(bucketID[:]),
		declFieldVersion:  storedInt64(decl.Version),
		declFieldState:    storedString(decl.State),
		declFieldFields:   storedBinary(EncodeFieldTable(decl.Fields)),
	}
	if decl.Pending != nil {
		if decl.Pending.Version < 0 {
			return nil, fmt.Errorf("pending version %d is negative", decl.Pending.Version)
		}
		if err := checkFieldTable(decl.Pending.Table); err != nil {
			return nil, fmt.Errorf("encoding the pending table: %w", err)
		}
		pending := binary.BigEndian.AppendUint64(nil, uint64(decl.Pending.Version))
		if decl.Pending.Expire == nil {
			pending = append(pending, EncodeFieldTable(decl.Pending.Table)...)
		} else {
			// [version:8][pendingExpireMarker][tableLen:4][field table][expire table]: a marker no
			// field-table format byte uses, so a pending without the section keeps its old bytes.
			if err := checkExpireTable(*decl.Pending.Expire); err != nil {
				return nil, fmt.Errorf("encoding the pending expire table: %w", err)
			}
			table := EncodeFieldTable(decl.Pending.Table)
			pending = append(pending, pendingExpireMarker)
			pending = binary.BigEndian.AppendUint32(pending, uint32(len(table)))
			pending = append(pending, table...)
			pending = append(pending, EncodeExpireTable(*decl.Pending.Expire)...)
		}
		stored[declFieldPending] = storedBinary(pending)
	}
	if decl.Expire.BucketField != 0 || len(decl.Expire.Stores) > 0 {
		if err := checkExpireTable(decl.Expire); err != nil {
			return nil, fmt.Errorf("encoding the expire table: %w", err)
		}
		stored[declFieldExpire] = storedBinary(EncodeExpireTable(decl.Expire))
	}
	if len(decl.Aggregates.Sets) > 0 {
		if err := checkAggregateTable(decl.Aggregates, decl.Fields); err != nil {
			return nil, fmt.Errorf("encoding the aggregate table: %w", err)
		}
		stored[declFieldAggregates] = storedBinary(EncodeAggregateTable(decl.Aggregates))
	}
	if len(decl.References) > 0 {
		if err := checkReferences(decl.References); err != nil {
			return nil, fmt.Errorf("encoding the references: %w", err)
		}
		stored[declFieldReferences] = storedBinary(EncodeReferences(decl.References))
	}
	if swept != nil {
		stored[declFieldSwept] = storedBinary(swept)
	}
	// An empty table with no history writes nothing, so a bucket without an ordered index
	// keeps the 436-byte declaration of conventions §19.
	if len(decl.Indexes.Entries) > 0 || decl.Indexes.NextIndexID > 1 {
		if len(decl.Indexes.Entries) > 0 {
			if err := checkIndexTable(decl.Indexes, decl.Fields); err != nil {
				return nil, fmt.Errorf("encoding the index table: %w", err)
			}
		}
		stored[declFieldIndexes] = storedBinary(EncodeIndexTable(decl.Indexes))
	}
	return stored, nil
}

// EncodeExpireTable packs field 11: [v:1=1][bucket_field_id:2 BE][n:1] ([field_id:2 BE]
// [pLen:1][path])×n. The caller checks the table first; EncodeDeclarationWithSwept does.
func EncodeExpireTable(table machine.ExpireTable) []byte {
	out := []byte{expireTableFormat}
	out = binary.BigEndian.AppendUint16(out, table.BucketField)
	out = append(out, byte(len(table.Stores)))
	for _, store := range table.Stores {
		out = binary.BigEndian.AppendUint16(out, store.FieldID)
		out = append(out, byte(len(store.Path)))
		out = append(out, store.Path...)
	}
	return out
}

// DecodeExpireTable is the inverse of EncodeExpireTable. It refuses a format other than 1,
// more than maxExpireStores stores, an empty or over-long path, a path of more than three
// keys, a field id repeated, and trailing bytes.
func DecodeExpireTable(encoded []byte) (machine.ExpireTable, error) {
	if len(encoded) < 4 {
		return machine.ExpireTable{}, malformed("expire table is %d bytes, it needs at least 4", len(encoded))
	}
	if encoded[0] != expireTableFormat {
		return machine.ExpireTable{}, malformed("expire table format %d: want %d", encoded[0], expireTableFormat)
	}
	table := machine.ExpireTable{BucketField: binary.BigEndian.Uint16(encoded[1:3])}
	count := int(encoded[3])
	if count > maxExpireStores {
		return machine.ExpireTable{}, malformed("expire table names %d stores, the cap is %d", count, maxExpireStores)
	}
	rest := encoded[4:]
	for range count {
		if len(rest) < 3 {
			return machine.ExpireTable{}, malformed("expire table ends inside a store entry")
		}
		fieldID := binary.BigEndian.Uint16(rest[0:2])
		pathLen := int(rest[2])
		if pathLen == 0 || len(rest) < 3+pathLen {
			return machine.ExpireTable{}, malformed("expire store %d has path length %d with %d bytes left", fieldID, pathLen, len(rest)-3)
		}
		table.Stores = append(table.Stores, machine.ExpireStore{FieldID: fieldID, Path: string(rest[3 : 3+pathLen])})
		rest = rest[3+pathLen:]
	}
	if len(rest) != 0 {
		return machine.ExpireTable{}, malformed("expire table has %d trailing bytes", len(rest))
	}
	if err := checkExpireTable(table); err != nil {
		return machine.ExpireTable{}, err
	}
	return table, nil
}

// checkExpireTable refuses what DecodeExpireTable refuses, so a table is never written that
// the next decode would reject.
func checkExpireTable(table machine.ExpireTable) error {
	if len(table.Stores) > maxExpireStores {
		return malformed("expire table names %d stores, the cap is %d", len(table.Stores), maxExpireStores)
	}
	seen := make(map[uint16]bool, len(table.Stores))
	for _, store := range table.Stores {
		if len(store.Path) == 0 || len(store.Path) > 255 {
			return malformed("expire store %d has a %d-byte path, want 1 to 255", store.FieldID, len(store.Path))
		}
		if keys := strings.Count(store.Path, ".") + 1; keys > 3 {
			return malformed("expire store %d path %q has %d keys, the cap is 3", store.FieldID, store.Path, keys)
		}
		if seen[store.FieldID] {
			return malformed("expire store field %d is named twice", store.FieldID)
		}
		seen[store.FieldID] = true
	}
	return nil
}

// DecodeDeclaration reads the stored values EncodeDeclaration wrote. Fields 2 to 5 are
// required; field 6 is accepted only while the state is dropping; field 7 and any other
// id belong to another document family or to nothing and are refused.
//
// It DISCARDS field 6: Declaration has no member for the swept bitmap. A caller that
// reads a declaration in order to rewrite it must use DecodeDeclarationWithSwept and
// EncodeDeclarationWithSwept, or a dropping bucket loses its drop progress.
func DecodeDeclaration(stored map[uint16][]byte) (Declaration, error) {
	decl, _, err := DecodeDeclarationWithSwept(stored)
	return decl, err
}

// DecodeDeclarationWithSwept is DecodeDeclaration that also returns the field 6 swept
// bitmap, or nil when the document has none.
func DecodeDeclarationWithSwept(stored map[uint16][]byte) (Declaration, []byte, error) {
	var decl Declaration
	for fieldID := range stored {
		switch fieldID {
		case declFieldBucketID, declFieldVersion, declFieldState, declFieldFields, declFieldSwept, declFieldPending, declFieldIndexes, declFieldExpire, declFieldAggregates, declFieldReferences:
		default:
			return Declaration{}, nil, malformed("field %d is not a declaration field", fieldID)
		}
	}
	bucketID, err := requireBinary(stored, declFieldBucketID)
	if err != nil {
		return Declaration{}, nil, err
	}
	if len(bucketID) != 8 {
		return Declaration{}, nil, malformed("bucket_id is %d bytes, want 8", len(bucketID))
	}
	decl.BucketID = binary.BigEndian.Uint64(bucketID)
	if decl.Version, err = requireInt64(stored, declFieldVersion); err != nil {
		return Declaration{}, nil, err
	}
	if decl.State, err = requireString(stored, declFieldState); err != nil {
		return Declaration{}, nil, err
	}
	if decl.State != machine.StateActive && decl.State != machine.StateDropping {
		return Declaration{}, nil, malformed("state %q: want %q or %q", decl.State, machine.StateActive, machine.StateDropping)
	}
	packed, err := requireBinary(stored, declFieldFields)
	if err != nil {
		return Declaration{}, nil, err
	}
	if decl.Fields, err = DecodeFieldTable(packed); err != nil {
		return Declaration{}, nil, fmt.Errorf("field 5: %w", err)
	}
	swept, err := decodeSwept(stored, decl.State)
	if err != nil {
		return Declaration{}, nil, err
	}
	if _, ok := stored[declFieldPending]; ok {
		if decl.Pending, err = decodePending(stored); err != nil {
			return Declaration{}, nil, err
		}
	}
	if _, ok := stored[declFieldIndexes]; ok {
		if decl.Indexes, err = decodeIndexes(stored, decl.Fields); err != nil {
			return Declaration{}, nil, err
		}
	}
	if _, ok := stored[declFieldExpire]; ok {
		expire, err := requireBinary(stored, declFieldExpire)
		if err != nil {
			return Declaration{}, nil, err
		}
		if decl.Expire, err = DecodeExpireTable(expire); err != nil {
			return Declaration{}, nil, fmt.Errorf("field 11: %w", err)
		}
	}
	if _, ok := stored[declFieldAggregates]; ok {
		if decl.Aggregates, err = decodeAggregates(stored, decl.Fields); err != nil {
			return Declaration{}, nil, err
		}
	}
	if _, ok := stored[declFieldReferences]; ok {
		packed, err := requireBinary(stored, declFieldReferences)
		if err != nil {
			return Declaration{}, nil, err
		}
		if decl.References, err = DecodeReferences(packed); err != nil {
			return Declaration{}, nil, fmt.Errorf("field 13: %w", err)
		}
	}
	return decl, swept, nil
}

// EncodeReferences packs field 13: [n:1] ([field_id:2 BE][bucket_id:8 BE])×n. The caller
// checks the list first; EncodeDeclarationWithSwept does.
func EncodeReferences(references []machine.Reference) []byte {
	out := []byte{byte(len(references))}
	for _, reference := range references {
		out = binary.BigEndian.AppendUint16(out, reference.FieldID)
		out = binary.BigEndian.AppendUint64(out, reference.BucketID)
	}
	return out
}

// DecodeReferences is the inverse of EncodeReferences. It refuses an empty list, a field id
// or a bucket id of 0, ids out of ascending field order (so one list has one encoding), and
// bytes that are not exactly n entries.
func DecodeReferences(encoded []byte) ([]machine.Reference, error) {
	if len(encoded) < 1+referenceEntryBytes {
		return nil, malformed("references are %d bytes, one entry needs %d", len(encoded), 1+referenceEntryBytes)
	}
	count := int(encoded[0])
	if len(encoded) != 1+count*referenceEntryBytes {
		return nil, malformed("references say %d entries and are %d bytes, want %d", count, len(encoded), 1+count*referenceEntryBytes)
	}
	references := make([]machine.Reference, count)
	for i := range references {
		entry := encoded[1+i*referenceEntryBytes:]
		references[i] = machine.Reference{FieldID: binary.BigEndian.Uint16(entry), BucketID: binary.BigEndian.Uint64(entry[2:])}
	}
	if err := checkReferences(references); err != nil {
		return nil, err
	}
	return references, nil
}

// checkReferences refuses what DecodeReferences refuses.
func checkReferences(references []machine.Reference) error {
	if len(references) == 0 || len(references) > maxReferences {
		return malformed("references name %d fields, a list holds 1 to %d", len(references), maxReferences)
	}
	for i, reference := range references {
		switch {
		case reference.FieldID < 2:
			return malformed("reference %d names field id %d, which is _id_ or never allocated", i, reference.FieldID)
		case reference.BucketID == 0:
			return malformed("reference %d (field %d) names bucket id 0", i, reference.FieldID)
		case i > 0 && reference.FieldID <= references[i-1].FieldID:
			return malformed("reference %d (field %d) is not above field %d: references are in ascending field order", i, reference.FieldID, references[i-1].FieldID)
		}
	}
	return nil
}

func decodeSwept(stored map[uint16][]byte, state string) ([]byte, error) {
	if _, ok := stored[declFieldSwept]; !ok {
		return nil, nil
	}
	if state != machine.StateDropping {
		return nil, malformed("field 6 (swept) exists only while a bucket is dropping, and this one is %q", state)
	}
	swept, err := requireBinary(stored, declFieldSwept)
	if err != nil {
		return nil, err
	}
	if len(swept) != SweptBytes {
		return nil, malformed("swept is %d bytes, want %d", len(swept), SweptBytes)
	}
	return bytes.Clone(swept), nil
}

func decodePending(stored map[uint16][]byte) (*Pending, error) {
	packed, err := requireBinary(stored, declFieldPending)
	if err != nil {
		return nil, err
	}
	if len(packed) < pendingHeader {
		return nil, malformed("pending is %d bytes, it needs a %d-byte version first", len(packed), pendingHeader)
	}
	version := int64(binary.BigEndian.Uint64(packed))
	if version < 0 {
		return nil, malformed("pending version %d is negative", version)
	}
	rest := packed[pendingHeader:]
	if len(rest) > 0 && rest[0] == pendingExpireMarker {
		return decodePendingWithExpire(version, rest[1:])
	}
	table, err := DecodeFieldTable(rest)
	if err != nil {
		return nil, fmt.Errorf("field 8: %w", err)
	}
	return &Pending{Version: version, Table: table}, nil
}

// pendingExpireMarker follows the version in a pending that carries an expire section.
const pendingExpireMarker byte = 0xE1

func decodePendingWithExpire(version int64, rest []byte) (*Pending, error) {
	if len(rest) < 4 {
		return nil, malformed("pending expire section: %d bytes, it needs a 4-byte table length", len(rest))
	}
	tableLen := int(binary.BigEndian.Uint32(rest))
	if tableLen > len(rest)-4 {
		return nil, malformed("pending field table of %d bytes does not fit in %d", tableLen, len(rest)-4)
	}
	table, err := DecodeFieldTable(rest[4 : 4+tableLen])
	if err != nil {
		return nil, fmt.Errorf("field 8: %w", err)
	}
	expire, err := DecodeExpireTable(rest[4+tableLen:])
	if err != nil {
		return nil, fmt.Errorf("field 8 expire section: %w", err)
	}
	return &Pending{Version: version, Table: table, Expire: &expire}, nil
}

func storedString(text string) []byte {
	out := make([]byte, 0, 6+len(text))
	out = append(out, bsonString)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(text)+1))
	out = append(out, text...)
	return append(out, 0)
}

func storedBinary(payload []byte) []byte {
	out := make([]byte, 0, 6+len(payload))
	out = append(out, bsonBinary)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(payload)))
	out = append(out, 0)
	return append(out, payload...)
}

func storedInt64(number int64) []byte {
	out := make([]byte, 0, 9)
	out = append(out, bsonInt64)
	return binary.LittleEndian.AppendUint64(out, uint64(number))
}

func requireBinary(stored map[uint16][]byte, fieldID uint16) ([]byte, error) {
	raw, ok := stored[fieldID]
	if !ok {
		return nil, malformed("field %d is missing", fieldID)
	}
	if len(raw) < 6 || raw[0] != bsonBinary {
		return nil, malformed("field %d is not a BSON binary", fieldID)
	}
	size := binary.LittleEndian.Uint32(raw[1:5])
	if raw[5] != 0 || uint64(size) != uint64(len(raw)-6) {
		return nil, malformed("field %d: binary length or subtype is wrong", fieldID)
	}
	return raw[6:], nil
}

func requireString(stored map[uint16][]byte, fieldID uint16) (string, error) {
	raw, ok := stored[fieldID]
	if !ok {
		return "", malformed("field %d is missing", fieldID)
	}
	if len(raw) < 6 || raw[0] != bsonString {
		return "", malformed("field %d is not a BSON string", fieldID)
	}
	size := binary.LittleEndian.Uint32(raw[1:5])
	if size < 1 || uint64(size) != uint64(len(raw)-5) || raw[len(raw)-1] != 0 {
		return "", malformed("field %d: string length or terminator is wrong", fieldID)
	}
	return string(raw[5 : len(raw)-1]), nil
}

func requireInt64(stored map[uint16][]byte, fieldID uint16) (int64, error) {
	raw, ok := stored[fieldID]
	if !ok {
		return 0, malformed("field %d is missing", fieldID)
	}
	if len(raw) != 9 || raw[0] != bsonInt64 {
		return 0, malformed("field %d is not a BSON int64", fieldID)
	}
	number := int64(binary.LittleEndian.Uint64(raw[1:]))
	if number < 0 {
		return 0, malformed("field %d: version %d is negative", fieldID, number)
	}
	return number, nil
}

// EncodeIndexTable packs an ordered-index table as field 10 lays it out (ordered-index
// spec §2.2): [v:1][count:1][entry × count][next_index_id:1], each entry
// [index_id:1][nameLen:1][name][ncomp:1][(field_id:2 BE ‖ dir:1) × ncomp][state:1].
// The caller has checked the table with checkIndexTable; DecodeIndexTable checks the bytes.
func EncodeIndexTable(table machine.IndexTable) []byte {
	out := []byte{indexTableFormat, byte(len(table.Entries))}
	for _, entry := range table.Entries {
		out = append(out, entry.ID, byte(len(entry.Name)))
		out = append(out, entry.Name...)
		out = append(out, byte(len(entry.Components)))
		for _, component := range entry.Components {
			out = binary.BigEndian.AppendUint16(out, component.FieldID)
			dir := byte(0)
			if component.Desc {
				dir = 1
			}
			out = append(out, dir)
		}
		out = append(out, entry.State)
	}
	return append(out, table.NextIndexID)
}

// DecodeIndexTable reads a table EncodeIndexTable wrote. It refuses a format other than 1,
// an id outside 1 to MaxOrderedIndexes, an empty name, a component count outside
// 1 to MaxIndexComponents, a field id repeated within one entry, a direction other than
// 0 or 1, a state above IndexStateConflicted, a duplicate id or name, and trailing bytes.
// The schema checks (fields exist, types are indexable) are checkIndexTable's.
func DecodeIndexTable(encoded []byte) (machine.IndexTable, error) {
	var table machine.IndexTable
	if len(encoded) < 3 {
		return table, malformed("an index table is at least 3 bytes, got %d", len(encoded))
	}
	if encoded[0] != indexTableFormat {
		return table, malformed("index table format %d, want %d", encoded[0], indexTableFormat)
	}
	count := int(encoded[1])
	if count > machine.MaxOrderedIndexes {
		return table, malformed("%d ordered indexes, the cap is %d", count, machine.MaxOrderedIndexes)
	}
	rest := encoded[2:]
	if count > 0 {
		table.Entries = make([]machine.IndexEntry, 0, count)
	}
	ids := make(map[uint8]bool, count)
	names := make(map[string]bool, count)
	for position := 0; position < count; position++ {
		var entry machine.IndexEntry
		var err error
		entry, rest, err = decodeIndexEntry(rest)
		if err != nil {
			return machine.IndexTable{}, fmt.Errorf("index entry %d: %w", position, err)
		}
		if ids[entry.ID] {
			return machine.IndexTable{}, malformed("index id %d appears twice", entry.ID)
		}
		if names[entry.Name] {
			return machine.IndexTable{}, malformed("index name %q appears twice", entry.Name)
		}
		ids[entry.ID], names[entry.Name] = true, true
		table.Entries = append(table.Entries, entry)
	}
	if len(rest) != 1 {
		return machine.IndexTable{}, malformed("%d bytes after %d index entries, want exactly the next id", len(rest), count)
	}
	table.NextIndexID = rest[0]
	return table, nil
}

func decodeIndexEntry(rest []byte) (machine.IndexEntry, []byte, error) {
	var entry machine.IndexEntry
	if len(rest) < 2 {
		return entry, nil, malformed("truncated index entry")
	}
	entry.ID = rest[0]
	if entry.ID == 0 || entry.ID > machine.MaxOrderedIndexes {
		return entry, nil, malformed("index id %d, want 1 to %d", entry.ID, machine.MaxOrderedIndexes)
	}
	nameLen := int(rest[1])
	rest = rest[2:]
	if nameLen == 0 {
		return entry, nil, malformed("index %d has an empty name", entry.ID)
	}
	if len(rest) < nameLen+1 {
		return entry, nil, malformed("truncated index name")
	}
	entry.Name = string(rest[:nameLen])
	ncomp := int(rest[nameLen])
	rest = rest[nameLen+1:]
	if ncomp < 1 || ncomp > machine.MaxIndexComponents {
		return entry, nil, malformed("index %q has %d components, want 1 to %d", entry.Name, ncomp, machine.MaxIndexComponents)
	}
	if len(rest) < 3*ncomp+1 {
		return entry, nil, malformed("truncated components of index %q", entry.Name)
	}
	entry.Components = make([]machine.IndexComponent, ncomp)
	for position := range entry.Components {
		fieldID := binary.BigEndian.Uint16(rest)
		dir := rest[2]
		if dir > 1 {
			return entry, nil, malformed("index %q: direction %d, want 0 or 1", entry.Name, dir)
		}
		for earlier := range position {
			if entry.Components[earlier].FieldID == fieldID {
				return entry, nil, malformed("index %q names field %d twice", entry.Name, fieldID)
			}
		}
		entry.Components[position] = machine.IndexComponent{FieldID: fieldID, Desc: dir == 1}
		rest = rest[3:]
	}
	entry.State = rest[0]
	if entry.State > machine.IndexStateConflicted {
		return entry, nil, malformed("index %q: state %d, want 0 to %d", entry.Name, entry.State, machine.IndexStateConflicted)
	}
	return entry, rest[1:], nil
}

// indexableWords are the declared type words an ordered index may key (index spec §1.2):
// the scalar words whose `_id_` tag has an encoding, and any. decimal, bsontimestamp,
// object, array, regex and the six store words are refused, because no total order the
// scan agrees with exists for them, or because a store holds elements, not one value.
var indexableWords = map[string]bool{
	"null": true, "bool": true, "int32": true, "int": true, "double": true, "number": true,
	"datetime": true, "string": true, "binary": true, "uuid": true, "objectid": true, "any": true,
}

// checkIndexTable refuses an index table that names a field the bucket does not declare, a
// component that is the _id_ field or a non-indexable type, a component count outside
// 1 to MaxIndexComponents, or an index whose name equals a field name. The byte-level
// checks are DecodeIndexTable's.
func checkIndexTable(table machine.IndexTable, fields FieldTable) error {
	byID := make(map[uint16]machine.FieldEntry, len(fields.Entries))
	fieldNames := make(map[string]bool, len(fields.Entries))
	for _, field := range fields.Entries {
		byID[field.ID] = field
		fieldNames[field.Name] = true
	}
	for _, entry := range table.Entries {
		if fieldNames[entry.Name] {
			return fmt.Errorf("index %q has the name of a field: an index and a field of one bucket need different names", entry.Name)
		}
		if n := len(entry.Components); n < 1 || n > machine.MaxIndexComponents {
			return fmt.Errorf("index %q has %d components, the cap is %d", entry.Name, n, machine.MaxIndexComponents)
		}
		for _, component := range entry.Components {
			field, ok := byID[component.FieldID]
			if !ok {
				return fmt.Errorf("index %q names field %d, which the bucket does not declare", entry.Name, component.FieldID)
			}
			if field.Name == "_id_" {
				return fmt.Errorf("index %q names _id_, which is the primary key and already has its own index", entry.Name)
			}
			if !indexableWords[field.Type] {
				return fmt.Errorf("index %q: field %q has type %q, which an index cannot key", entry.Name, field.Name, field.Type)
			}
		}
	}
	return nil
}

// decodeIndexes reads field 10. It is checked against the declaration's own fields, which
// is why it runs after field 5 is decoded.
func decodeIndexes(stored map[uint16][]byte, fields FieldTable) (machine.IndexTable, error) {
	packed, err := requireBinary(stored, declFieldIndexes)
	if err != nil {
		return machine.IndexTable{}, err
	}
	table, err := DecodeIndexTable(packed)
	if err != nil {
		return machine.IndexTable{}, fmt.Errorf("field 10: %w", err)
	}
	if err := checkIndexTable(table, fields); err != nil {
		return machine.IndexTable{}, malformed("field 10: %v", err)
	}
	return table, nil
}
