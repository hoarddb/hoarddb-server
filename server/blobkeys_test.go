package server

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/sealed"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/storage/kvfault"
)

// blobRegistryTestPayloadName is a payload file name of the shape
// store.PayloadFileName writes.
const blobRegistryTestPayloadName = "2a7-0123456789abcdef-01890a5d-ac96-774b-bcce-b302099a8057"

// blobRegistryTestKV wraps a node store so a test can see when the node record
// is committed and fail its write or its read.
type blobRegistryTestKV struct {
	storage.KV
	events    *[]string
	commitErr error
	getErr    error
}

func (kv *blobRegistryTestKV) NewBatch() storage.KVBatch {
	return &blobRegistryTestBatch{KVBatch: kv.KV.NewBatch(), kv: kv}
}

func (kv *blobRegistryTestKV) NewSnapshot() storage.KVSnapshot {
	return &blobRegistryTestSnapshot{KVSnapshot: kv.KV.NewSnapshot(), kv: kv}
}

type blobRegistryTestBatch struct {
	storage.KVBatch
	kv *blobRegistryTestKV
}

func (batch *blobRegistryTestBatch) Commit(sync bool) error {
	if batch.kv.commitErr != nil {
		return batch.kv.commitErr
	}
	if batch.kv.events != nil {
		*batch.kv.events = append(*batch.kv.events, fmt.Sprintf("node record committed, sync=%t", sync))
	}
	return batch.KVBatch.Commit(sync)
}

type blobRegistryTestSnapshot struct {
	storage.KVSnapshot
	kv *blobRegistryTestKV
}

func (snap *blobRegistryTestSnapshot) Get(key []byte) ([]byte, error) {
	if snap.kv.getErr != nil {
		return nil, snap.kv.getErr
	}
	return snap.KVSnapshot.Get(key)
}

// No failure message in this file prints a key source: formatting one with %v
// would print its epoch keys.

// blobRegistryTestNode is a sealed node's data directory, master key and node
// store, with the loader's log captured.
type blobRegistryTestNode struct {
	cfg    *config.Config
	nm     *NodeMaster
	kv     *blobRegistryTestKV
	logs   *bytes.Buffer
	logger *slog.Logger
}

func newBlobRegistryTestNode(t *testing.T) *blobRegistryTestNode {
	t.Helper()
	logs := &bytes.Buffer{}
	return &blobRegistryTestNode{
		cfg:    encryptionTestConfig(t.TempDir(), "on", ""),
		nm:     encryptionTestNodeMaster(),
		kv:     &blobRegistryTestKV{KV: kvfault.New()},
		logs:   logs,
		logger: slog.New(slog.NewTextHandler(logs, nil)),
	}
}

func (node *blobRegistryTestNode) load() (sealed.BlobKeySource, error) {
	return LoadOrCreateBlobKeySource(node.cfg, node.nm, node.kv, node.logger)
}

func (node *blobRegistryTestNode) mustLoad(t *testing.T) sealed.BlobKeySource {
	t.Helper()
	source, err := node.load()
	if err != nil {
		t.Fatalf("LoadOrCreateBlobKeySource: %v", err)
	}
	if source == nil {
		t.Fatal("LoadOrCreateBlobKeySource returned no key source on a sealed node")
	}
	return source
}

func (node *blobRegistryTestNode) keysDir() string {
	return filepath.Join(node.cfg.Storage.DataDir, "keys")
}

func (node *blobRegistryTestNode) registryPath() string {
	return filepath.Join(node.keysDir(), "blobs.wrapped")
}

// registryBytes returns the registry file's bytes, or nil when it is absent.
func (node *blobRegistryTestNode) registryBytes(t *testing.T) []byte {
	t.Helper()
	wrapped, err := os.ReadFile(node.registryPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("reading %s: %v", node.registryPath(), err)
	}
	return wrapped
}

// recordBytes returns the node record's raw value, or nil when it is absent.
func (node *blobRegistryTestNode) recordBytes(t *testing.T) []byte {
	t.Helper()
	snap := node.kv.KV.NewSnapshot()
	defer snap.Discard()
	encoded, err := snap.Get(blobRegistryRecordKey())
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		t.Fatalf("reading the node record: %v", err)
	}
	return encoded
}

// epochKey opens the registry file and returns its current epoch's key, for
// comparing derivations and for searching log text. It is never printed.
func (node *blobRegistryTestNode) epochKey(t *testing.T) []byte {
	t.Helper()
	registry, err := openBlobEpochRegistry(node.nm.Bytes(), node.registryBytes(t))
	if err != nil {
		t.Fatalf("opening the registry the loader wrote: %v", err)
	}
	for _, entry := range registry.Epochs {
		if entry.ID == registry.Current {
			return entry.Key
		}
	}
	t.Fatalf("the registry's current epoch %d names no entry", registry.Current)
	return nil
}

// keysDirNames lists what the keys directory holds; nil when it does not exist.
func (node *blobRegistryTestNode) keysDirNames(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(node.keysDir())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("listing %s: %v", node.keysDir(), err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// watchBlobRegistrySteps records the rename and the directory sync of a mint
// into events, in the order they happen, and restores the seams afterwards.
func watchBlobRegistrySteps(t *testing.T, events *[]string) {
	t.Helper()
	realRename, realFsyncer := blobRegistryRename, blobRegistryDirFsyncer
	t.Cleanup(func() { blobRegistryRename, blobRegistryDirFsyncer = realRename, realFsyncer })
	blobRegistryRename = func(oldPath, newPath string) error {
		*events = append(*events, "registry renamed to "+filepath.Base(newPath))
		return realRename(oldPath, newPath)
	}
	blobRegistryDirFsyncer = func(dirPath string) error {
		*events = append(*events, "directory "+filepath.Base(dirPath)+" synced")
		return realFsyncer(dirPath)
	}
}

// TestBlobRegistryMintedOnAFreshSealedNode is the first start of a sealed
// node: no registry and no record. The registry is minted with the key
// directory's modes, and the node record is committed — synced — only after
// the registry's rename has itself been made durable.
func TestBlobRegistryMintedOnAFreshSealedNode(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	var events []string
	node.kv.events = &events
	watchBlobRegistrySteps(t, &events)

	source := node.mustLoad(t)
	if source.CurrentEpoch() != 1 {
		t.Errorf("the minted registry's current epoch is %d, want 1", source.CurrentEpoch())
	}

	wantEvents := []string{"registry renamed to blobs.wrapped", "directory keys synced", "node record committed, sync=true"}
	if !slices.Equal(events, wantEvents) {
		t.Fatalf("the mint's steps ran as %q, want %q: the record must follow the registry's durability", events, wantEvents)
	}
	registryInfo, err := os.Stat(node.registryPath())
	if err != nil {
		t.Fatalf("the registry file: %v", err)
	}
	if registryInfo.Mode().Perm() != 0o600 {
		t.Errorf("keys/blobs.wrapped has mode %o, want 600", registryInfo.Mode().Perm())
	}
	keysInfo, err := os.Stat(node.keysDir())
	if err != nil {
		t.Fatalf("the keys directory: %v", err)
	}
	if keysInfo.Mode().Perm() != 0o700 {
		t.Errorf("keys/ has mode %o, want 700", keysInfo.Mode().Perm())
	}
	if names := node.keysDirNames(t); !slices.Equal(names, []string{"blobs.wrapped"}) {
		t.Errorf("keys/ holds %q, want only blobs.wrapped: the temporary must not survive", names)
	}

	encodedRecord := node.recordBytes(t)
	if encodedRecord == nil {
		t.Fatal("no node record was written")
	}
	var record struct {
		FormatVersion int32     `bson:"format_version"`
		CreatedAt     time.Time `bson:"created_at"`
	}
	if err := bson.Unmarshal(encodedRecord, &record); err != nil {
		t.Fatalf("decoding the node record: %v", err)
	}
	if record.FormatVersion != 1 || time.Since(record.CreatedAt) > time.Hour || record.CreatedAt.IsZero() {
		t.Errorf("the node record is {format_version: %d, created_at: %s}; want version 1 and now", record.FormatVersion, record.CreatedAt)
	}
	var recordFields bson.M
	if err := bson.Unmarshal(encodedRecord, &recordFields); err != nil {
		t.Fatalf("decoding the node record's fields: %v", err)
	}
	if len(recordFields) != 2 {
		t.Errorf("the node record has %d fields, want exactly format_version and created_at: no kid, no key", len(recordFields))
	}
	epochKey := node.epochKey(t)
	if bytes.Contains(encodedRecord, epochKey) || bytes.Contains(encodedRecord, []byte(sealed.Kid(epochKey))) {
		t.Error("the node record holds the epoch key or its kid")
	}
}

// TestBlobRegistryDurableBeforeFirstUse injects the two crash points of a
// mint. Before the rename: nothing remains and the next start mints. After
// the rename, before the record: the registry remains, and the next start
// loads THAT registry and records it — it does not mint a second one.
func TestBlobRegistryDurableBeforeFirstUse(t *testing.T) {
	t.Run("a failure at the registry's rename", func(t *testing.T) {
		node := newBlobRegistryTestNode(t)
		realRename := blobRegistryRename
		t.Cleanup(func() { blobRegistryRename = realRename })
		blobRegistryRename = func(string, string) error { return errors.New("injected: the node stopped before the rename") }

		source, err := node.load()
		if err == nil || source != nil {
			t.Fatalf("a mint whose rename failed must return an error and no key source; got a source: %t, err: %v", source != nil, err)
		}
		if names := node.keysDirNames(t); len(names) != 0 {
			t.Fatalf("keys/ holds %q after a failed rename; neither the registry nor its temporary may remain", names)
		}
		if node.recordBytes(t) != nil {
			t.Fatal("the node record was written although no registry became durable: the next start would refuse forever")
		}

		blobRegistryRename = realRename
		node.mustLoad(t)
		if node.registryBytes(t) == nil || node.recordBytes(t) == nil {
			t.Fatal("the next start did not mint the registry and record it")
		}
	})

	t.Run("a failure at the node record's write", func(t *testing.T) {
		node := newBlobRegistryTestNode(t)
		node.kv.commitErr = errors.New("injected: the node stopped before the record committed")

		source, err := node.load()
		if err == nil || source != nil {
			t.Fatalf("a mint whose record did not commit must return an error and no key source; got a source: %t, err: %v", source != nil, err)
		}
		mintedRegistry := node.registryBytes(t)
		if mintedRegistry == nil {
			t.Fatal("the registry is absent; this leg needs it durable with the record unwritten")
		}
		if node.recordBytes(t) != nil {
			t.Fatal("the node record is present although its commit failed")
		}
		mintedKid := sealed.Kid(node.epochKey(t))

		node.kv.commitErr = nil
		restarted := node.mustLoad(t)
		if !bytes.Equal(node.registryBytes(t), mintedRegistry) {
			t.Fatal("the next start rewrote keys/blobs.wrapped; it must load the registry that is there")
		}
		if kid := sealed.Kid(node.epochKey(t)); kid != mintedKid {
			t.Fatalf("the next start serves epoch key kid %s, want the minted %s", kid, mintedKid)
		}
		if node.recordBytes(t) == nil {
			t.Fatal("the next start did not write the node record")
		}
		wantFileKey, err := sealed.FileKey(node.epochKey(t), sealed.PurposeBlobChunk, blobRegistryTestPayloadName)
		if err != nil {
			t.Fatalf("deriving the expected file key: %v", err)
		}
		fileKey, err := restarted.FileKey(1, blobRegistryTestPayloadName)
		if err != nil || !bytes.Equal(fileKey, wantFileKey) {
			t.Fatalf("the reloaded source does not derive from the minted epoch key (err: %v)", err)
		}
	})
}

// missingRegistryRefusal asserts err is the refusal for a recorded registry
// that is gone, worded as spec §4.2.1 g requires.
func missingRegistryRefusal(t *testing.T, node *blobRegistryTestNode, source sealed.BlobKeySource, err error) {
	t.Helper()
	if source != nil {
		t.Fatal("a refused start returned a key source")
	}
	var missing *blobRegistryMissingError
	if !errors.As(err, &missing) {
		t.Fatalf("want the missing-registry refusal, got %T: %v", err, err)
	}
	for _, want := range []string{
		"refusing to start",
		"keys/blobs.wrapped is missing",
		fmt.Sprintf("%q", node.cfg.Storage.DataDir),
		"Restore keys/blobs.wrapped from a backup",
		"escrow (-export-key) does not cover them",
		"nothing was written",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not contain %q:\n%v", want, err)
		}
	}
}

// TestMissingBlobRegistryWithTheNodeRecordIsRefusedNotMinted is the case the
// node record exists for: the registry is gone from a node that had one. A
// new registry's epoch 1 would be another key, so none is created.
func TestMissingBlobRegistryWithTheNodeRecordIsRefusedNotMinted(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	node.mustLoad(t)
	recordBefore := node.recordBytes(t)
	var record blobRegistryRecord
	if err := bson.Unmarshal(recordBefore, &record); err != nil {
		t.Fatalf("decoding the node record: %v", err)
	}
	if err := os.Remove(node.registryPath()); err != nil {
		t.Fatalf("removing the registry: %v", err)
	}

	source, err := node.load()
	missingRegistryRefusal(t, node, source, err)
	if createdAt := record.CreatedAt.UTC().Format(time.RFC3339); !strings.Contains(err.Error(), createdAt) {
		t.Errorf("the refusal does not name the record's created_at %s:\n%v", createdAt, err)
	}
	if names := node.keysDirNames(t); len(names) != 0 {
		t.Fatalf("keys/ holds %q after the refusal; nothing may be created", names)
	}
	if !bytes.Equal(node.recordBytes(t), recordBefore) {
		t.Fatal("the refusal changed the node record")
	}
}

// TestMissingBlobRegistryWithoutTheNodeRecordIsMinted is the upgrade path: a
// node sealed before blobs were holds a store with buckets in it and neither a
// registry nor a record, exactly once.
func TestMissingBlobRegistryWithoutTheNodeRecordIsMinted(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	if _, err := seedBucketIDAllocator(node.kv, node.cfg.Storage.DataDir); err != nil {
		t.Fatalf("seeding the allocator: %v", err)
	}
	batch := node.kv.KV.NewBatch()
	if err := batch.Set(catalogKey("app", "users"), []byte("an existing bucket's catalog entry")); err != nil {
		t.Fatalf("writing a catalog entry: %v", err)
	}
	if err := batch.Commit(true); err != nil {
		t.Fatalf("committing a catalog entry: %v", err)
	}

	node.mustLoad(t)
	if node.registryBytes(t) == nil || node.recordBytes(t) == nil {
		t.Fatal("a node with no registry and no record must mint one and record it")
	}
}

// TestBlobRegistryDecisionNeverReadsTheBlobRoot runs both decisions with
// storage.blob_dir naming a path that does not exist — a volume that is not
// mounted — and with it naming an empty tree. The decision must be the same:
// an absent tree is exactly what a lost registry beside unreachable payloads
// looks like, so a loader that took it for "no blobs here" would mint.
func TestBlobRegistryDecisionNeverReadsTheBlobRoot(t *testing.T) {
	blobRoots := map[string]func(t *testing.T) string{
		"blob_dir is not mounted":                      func(t *testing.T) string { return filepath.Join(t.TempDir(), "not-mounted") },
		"blob_dir is an empty tree":                    func(t *testing.T) string { return t.TempDir() },
		"blob_dir is unset and <data>/blobs is absent": func(*testing.T) string { return "" },
	}
	for name, blobRoot := range blobRoots {
		t.Run(name, func(t *testing.T) {
			node := newBlobRegistryTestNode(t)
			node.cfg.Storage.BlobDir = blobRoot(t)

			node.mustLoad(t)
			if node.registryBytes(t) == nil {
				t.Fatal("neither registry nor record: the loader must mint whatever the blob root looks like")
			}
			if err := os.Remove(node.registryPath()); err != nil {
				t.Fatalf("removing the registry: %v", err)
			}
			source, err := node.load()
			missingRegistryRefusal(t, node, source, err)
			if node.registryBytes(t) != nil {
				t.Fatal("record present, registry gone: the loader minted a new registry")
			}
			if name == "blob_dir is not mounted" {
				if _, statErr := os.Stat(node.cfg.Storage.BlobDir); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("the loader created the blob root (stat: %v)", statErr)
				}
			}
		})
	}
}

// TestWALRegistryPresentedAsTheBlobRegistryIsRefused copies wal/keys.wrapped
// to keys/blobs.wrapped. Both are purpose 0x08 under the same node master; the
// identity keeps them apart. The file is refused by name and never replaced,
// whether or not the node record exists.
func TestWALRegistryPresentedAsTheBlobRegistryIsRefused(t *testing.T) {
	for _, recorded := range []bool{false, true} {
		t.Run(fmt.Sprintf("node record present: %t", recorded), func(t *testing.T) {
			node := newBlobRegistryTestNode(t)
			if recorded {
				node.mustLoad(t)
				if err := os.Remove(node.registryPath()); err != nil {
					t.Fatalf("removing the registry: %v", err)
				}
			}
			if _, err := LoadOrCreateWALKeySource(node.cfg.Storage.DataDir, node.nm); err != nil {
				t.Fatalf("creating the WAL registry: %v", err)
			}
			walRegistry, err := os.ReadFile(filepath.Join(node.cfg.Storage.DataDir, "wal", WALEpochRegistryFileName))
			if err != nil {
				t.Fatalf("reading the WAL registry: %v", err)
			}
			if err := os.MkdirAll(node.keysDir(), 0o700); err != nil {
				t.Fatalf("creating keys/: %v", err)
			}
			if err := os.WriteFile(node.registryPath(), walRegistry, 0o600); err != nil {
				t.Fatalf("presenting the WAL registry as the blob registry: %v", err)
			}
			recordBefore := node.recordBytes(t)

			source, err := node.load()
			if err == nil || source != nil {
				t.Fatalf("the WAL's registry was accepted as the blob registry (got a source: %t, err: %v)", source != nil, err)
			}
			var unusable *blobRegistryUnusableError
			if !errors.As(err, &unusable) {
				t.Fatalf("want the unusable-registry refusal, got %T: %v", err, err)
			}
			if !strings.Contains(err.Error(), "keys/blobs.wrapped") || !strings.Contains(err.Error(), "refusing to start") {
				t.Errorf("the refusal must name keys/blobs.wrapped, got: %v", err)
			}
			if !bytes.Equal(node.registryBytes(t), walRegistry) {
				t.Fatal("the presented file was replaced; a present registry is never minted over")
			}
			if names := node.keysDirNames(t); !slices.Equal(names, []string{"blobs.wrapped"}) {
				t.Fatalf("keys/ holds %q after the refusal, want only the presented file", names)
			}
			if !bytes.Equal(node.recordBytes(t), recordBefore) {
				t.Fatal("the refusal changed the node record")
			}
		})
	}
}

// TestBlobRegistryOnAPlaintextNodeIsANoOp: no node master, no key source, and
// nothing written to the data directory or the store.
func TestBlobRegistryOnAPlaintextNodeIsANoOp(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	source, err := LoadOrCreateBlobKeySource(node.cfg, nil, node.kv, node.logger)
	if err != nil {
		t.Fatalf("LoadOrCreateBlobKeySource on a plaintext node: %v", err)
	}
	if source != nil {
		t.Fatalf("a plaintext node was given a blob key source (%T); the store would then seal", source)
	}
	entries, err := os.ReadDir(node.cfg.Storage.DataDir)
	if err != nil {
		t.Fatalf("listing the data directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("a plaintext node's data directory gained %d entries", len(entries))
	}
	if node.recordBytes(t) != nil {
		t.Fatal("a plaintext node's store gained the blob registry record")
	}
}

// TestBlobKeySourceDerivesFromTheEpochKeyAndPayloadName pins the file key as
// HKDF(epoch key, 0x06, payload name), apart from the WAL's purpose, and runs
// the source through the codec it exists for.
func TestBlobKeySourceDerivesFromTheEpochKeyAndPayloadName(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	source := node.mustLoad(t)
	epochKey := node.epochKey(t)

	fileKey, err := source.FileKey(source.CurrentEpoch(), blobRegistryTestPayloadName)
	if err != nil {
		t.Fatalf("FileKey: %v", err)
	}
	wantFileKey, err := sealed.FileKey(epochKey, sealed.PurposeBlobChunk, blobRegistryTestPayloadName)
	if err != nil {
		t.Fatalf("deriving the expected file key: %v", err)
	}
	if !bytes.Equal(fileKey, wantFileKey) {
		t.Fatal("the file key is not HKDF(epoch key, 0x06, payload name)")
	}
	walStyleKey, err := sealed.FileKey(epochKey, sealed.PurposeWALEntry, blobRegistryTestPayloadName)
	if err != nil {
		t.Fatalf("deriving the WAL-purpose key: %v", err)
	}
	if bytes.Equal(fileKey, walStyleKey) {
		t.Fatal("the blob file key equals the WAL's derivation for the same identity")
	}
	otherName, err := source.FileKey(source.CurrentEpoch(), blobRegistryTestPayloadName+"0")
	if err != nil || bytes.Equal(fileKey, otherName) {
		t.Fatalf("another payload name must derive another key (err: %v)", err)
	}

	if _, err := source.FileKey(9, blobRegistryTestPayloadName); !errors.Is(err, sealed.ErrBlobKeyMissing) {
		t.Fatalf("an epoch the registry does not hold: want sealed.ErrBlobKeyMissing, got: %v", err)
	}

	content := bytes.Repeat([]byte("sealed through the node's registry "), 4000)
	var payload bytes.Buffer
	if err := sealed.SealBlobFile(&payload, sealed.SealBlobRequest{Keys: source, PayloadName: blobRegistryTestPayloadName, Content: content}); err != nil {
		t.Fatalf("sealing through the registry's source: %v", err)
	}
	reloaded := node.mustLoad(t)
	opened, err := sealed.OpenBlobFile(bytes.NewReader(payload.Bytes()), sealed.OpenBlobRequest{
		Keys: reloaded, PayloadName: blobRegistryTestPayloadName, Size: int64(len(content)), FileLen: int64(payload.Len()),
	})
	if err != nil || !bytes.Equal(opened, content) {
		t.Fatalf("a payload sealed before a restart did not open after it: %v", err)
	}
}

// TestBlobRegistryLogsCarryNoKeyBytes captures what a mint logs and what each
// refusal says, and searches all of it for the node master and the epoch key
// in the encodings a log line could carry them in.
func TestBlobRegistryLogsCarryNoKeyBytes(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	node.mustLoad(t)
	if !strings.Contains(node.logs.String(), "created the blob key registry") {
		t.Fatalf("the mint logged nothing this test can search; the log was: %q", node.logs.String())
	}
	epochKey := node.epochKey(t)
	texts := map[string]string{"the mint's log": node.logs.String()}

	mintedRegistry := node.registryBytes(t)
	if err := os.Remove(node.registryPath()); err != nil {
		t.Fatalf("removing the registry: %v", err)
	}
	_, err := node.load()
	texts["the missing-registry refusal"] = fmt.Sprint(err)

	damaged := append([]byte(nil), mintedRegistry...)
	damaged[len(damaged)-1] ^= 0x01
	if err := os.WriteFile(node.registryPath(), damaged, 0o600); err != nil {
		t.Fatalf("writing a damaged registry: %v", err)
	}
	_, err = node.load()
	texts["the damaged-registry refusal"] = fmt.Sprint(err)

	otherMaster := &NodeMaster{bytes: bytes.Repeat([]byte{0x5a}, 32), kid: "othernm1"}
	if err := os.WriteFile(node.registryPath(), mintedRegistry, 0o600); err != nil {
		t.Fatalf("restoring the registry: %v", err)
	}
	_, err = LoadOrCreateBlobKeySource(node.cfg, otherMaster, node.kv, node.logger)
	texts["the wrong-node-master refusal"] = fmt.Sprint(err)

	if err := os.Remove(node.registryPath()); err != nil {
		t.Fatalf("removing the registry: %v", err)
	}
	node.kv.getErr = errors.New("injected: the store could not be read")
	_, err = node.load()
	texts["the unreadable-record refusal"] = fmt.Sprint(err)
	texts["the whole log"] = node.logs.String()

	secrets := map[string][]byte{"the node master": node.nm.Bytes(), "the epoch key": epochKey, "another node's master": otherMaster.Bytes()}
	for where, text := range texts {
		if text == "<nil>" {
			t.Errorf("%s: the load did not fail", where)
		}
		for what, secret := range secrets {
			for encoding, encoded := range map[string]string{
				"raw":    string(secret),
				"hex":    hex.EncodeToString(secret),
				"base64": base64.StdEncoding.EncodeToString(secret),
			} {
				if strings.Contains(text, encoded) {
					t.Errorf("%s carries %s (%s)", where, what, encoding)
				}
			}
		}
	}
}

// TestBlobRegistryReadErrorIsNotTreatedAsAbsent: a node store that cannot
// answer whether the record exists has not said it is absent. The loader
// errors and mints nothing; with the registry present it still errors, because
// it cannot confirm the record, and leaves the file alone.
func TestBlobRegistryReadErrorIsNotTreatedAsAbsent(t *testing.T) {
	storeReadErr := errors.New("injected: the store could not be read")

	t.Run("the registry is absent", func(t *testing.T) {
		node := newBlobRegistryTestNode(t)
		node.kv.getErr = storeReadErr
		source, err := node.load()
		if source != nil || !errors.Is(err, storeReadErr) {
			t.Fatalf("want the store's read error and no key source; got a source: %t, err: %v", source != nil, err)
		}
		if names := node.keysDirNames(t); len(names) != 0 {
			t.Fatalf("keys/ holds %q: a registry was minted on a read error", names)
		}
	})

	t.Run("the registry is present", func(t *testing.T) {
		node := newBlobRegistryTestNode(t)
		node.mustLoad(t)
		mintedRegistry := node.registryBytes(t)
		node.kv.getErr = storeReadErr
		source, err := node.load()
		if source != nil || !errors.Is(err, storeReadErr) {
			t.Fatalf("want the store's read error and no key source; got a source: %t, err: %v", source != nil, err)
		}
		if !bytes.Equal(node.registryBytes(t), mintedRegistry) {
			t.Fatal("the registry file changed on a read error")
		}
	})

	t.Run("the record is present and does not decode", func(t *testing.T) {
		node := newBlobRegistryTestNode(t)
		batch := node.kv.KV.NewBatch()
		if err := batch.Set(blobRegistryRecordKey(), []byte("not a record")); err != nil {
			t.Fatalf("writing a damaged record: %v", err)
		}
		if err := batch.Commit(true); err != nil {
			t.Fatalf("committing a damaged record: %v", err)
		}
		source, err := node.load()
		if source != nil || err == nil {
			t.Fatalf("a damaged record must be an error and no key source; got a source: %t, err: %v", source != nil, err)
		}
		if names := node.keysDirNames(t); len(names) != 0 {
			t.Fatalf("keys/ holds %q: a registry was minted over a damaged record", names)
		}
	})
}

// TestBlobRegistryRecordLiesInNoOtherNodeRecordRange keeps the record out of
// every range another part of the server walks: the catalog load, the
// reclaimer's records and the materialise records. A record swept by one of
// them would be decoded as something it is not.
func TestBlobRegistryRecordLiesInNoOtherNodeRecordRange(t *testing.T) {
	recordKey := blobRegistryRecordKey()
	catalogLower, catalogUpper := catalogPrefix()
	reclaimLower := storage.NodeKey(storage.NSReclaim)
	_, dropsUpper := databaseDropRange()
	materializeLower, materializeUpper := materializeRecordsRange()
	ranges := map[string][2][]byte{
		"catalog":     {catalogLower, catalogUpper},
		"reclaim":     {reclaimLower, dropsUpper},
		"materialise": {materializeLower, materializeUpper},
		"allocator":   {allocatorKey(), append(allocatorKey(), 0xFF)},
	}
	for name, bounds := range ranges {
		if bytes.Compare(recordKey, bounds[0]) >= 0 && bytes.Compare(recordKey, bounds[1]) < 0 {
			t.Errorf("the blob registry record's key %x lies in the %s range [%x, %x)", recordKey, name, bounds[0], bounds[1])
		}
	}
	if slot := recordKey[:2]; !bytes.Equal(slot, []byte{0xFF, 0xFF}) {
		t.Errorf("the record's key begins %x, want the node record slot: a slot's range scan must not ship it", slot)
	}
}

// TestBlobRegistryRecordMustFollowTheAllocatorSeed pins an ordering the
// plumbing that calls the loader has to keep. The allocator's seed treats a
// store that holds any key but no allocator record as damaged
// (seedBucketIDAllocator), so on a brand-new store the blob registry record
// must be written after the seed, never before.
func TestBlobRegistryRecordMustFollowTheAllocatorSeed(t *testing.T) {
	t.Run("the loader before the seed is refused by the seed", func(t *testing.T) {
		node := newBlobRegistryTestNode(t)
		node.mustLoad(t)
		var damaged *allocatorRecordError
		if _, err := seedBucketIDAllocator(node.kv, node.cfg.Storage.DataDir); !errors.As(err, &damaged) {
			t.Fatalf("the seed accepted a new store that already held the registry record (err: %v); if the seed's rule changed, this ordering constraint is gone", err)
		}
	})
	t.Run("the loader after the seed starts, and starts again", func(t *testing.T) {
		node := newBlobRegistryTestNode(t)
		for start := range 2 {
			if _, err := seedBucketIDAllocator(node.kv, node.cfg.Storage.DataDir); err != nil {
				t.Fatalf("start %d: seeding the allocator: %v", start, err)
			}
			node.mustLoad(t)
		}
	})
}

// TestBlobKeySourceFormatsWithoutItsKeys guards the accident the guard breaks
// of this file's own tests ran into: a key source handed to a formatting verb
// or a logger prints its epoch keys unless it says how it is printed.
func TestBlobKeySourceFormatsWithoutItsKeys(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	source := node.mustLoad(t)
	epochKey := node.epochKey(t)

	var logged bytes.Buffer
	slog.New(slog.NewTextHandler(&logged, nil)).Info("loaded", "keys", source)
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("loaded", "keys", source)
	formatted := fmt.Sprintf("%v|%+v|%s|%#v|%d|%x", source, source, source, source, source, source) + logged.String()

	keyAsDecimals := strings.Trim(fmt.Sprintf("%d", epochKey), "[]")
	for encoding, encoded := range map[string]string{
		"raw":            string(epochKey),
		"decimal bytes":  keyAsDecimals,
		"hex":            hex.EncodeToString(epochKey),
		"base64":         base64.StdEncoding.EncodeToString(epochKey),
		"Go byte syntax": strings.Trim(fmt.Sprintf("%#v", epochKey), "[]byte{}"),
	} {
		if strings.Contains(formatted, encoded) {
			t.Errorf("a formatted key source carries the epoch key (%s)", encoding)
		}
	}
	if !strings.Contains(formatted, "epoch 1") {
		t.Errorf("a formatted key source should still say which epoch is current")
	}
}

// TestBlobRegistryAndRecordShapesArePinned pins what is on disk by field name
// and BSON type, read without the loader's own structs: a renamed field or a
// changed type round-trips through this package and strands every node that
// wrote the old one. The registry is the WAL registry's shape, sealed as
// purpose 0x08 under identity "blobs/1".
func TestBlobRegistryAndRecordShapesArePinned(t *testing.T) {
	node := newBlobRegistryTestNode(t)
	node.mustLoad(t)

	plaintext, gen, err := sealed.Open(sealed.PurposeEpochRegistry, node.nm.Bytes(), func(gen uint64) string {
		return fmt.Sprintf("blobs/%d", gen)
	}, node.registryBytes(t))
	if err != nil || gen != 1 {
		t.Fatalf("opening keys/blobs.wrapped as purpose 0x08, identity blobs/<gen>: gen %d, %v", gen, err)
	}
	var registry bson.M
	if err := bson.Unmarshal(plaintext, &registry); err != nil {
		t.Fatalf("decoding the registry: %v", err)
	}
	if len(registry) != 3 || registry["format_version"] != int32(1) || registry["current"] != int64(1) {
		t.Fatalf("the registry's fields are not {format_version: int32 1, current: int64 1, epochs}: %d fields, format_version %T, current %T",
			len(registry), registry["format_version"], registry["current"])
	}
	epochs, isArray := registry["epochs"].(primitive.A)
	if !isArray || len(epochs) != 1 {
		t.Fatalf("epochs is a %T of %d entries, want an array of one", registry["epochs"], len(epochs))
	}
	epoch, isDocument := epochs[0].(bson.M)
	if !isDocument || len(epoch) != 4 {
		t.Fatalf("an epoch is a %T with %d fields, want a document of id, kid, key, created_at", epochs[0], len(epoch))
	}
	key, keyIsBinary := epoch["key"].(primitive.Binary)
	kid, kidIsString := epoch["kid"].(string)
	_, createdIsInt64 := epoch["created_at"].(int64)
	if epoch["id"] != int64(1) || !keyIsBinary || len(key.Data) != 32 || !kidIsString || kid != sealed.Kid(key.Data) || !createdIsInt64 {
		t.Fatalf("an epoch is not {id: int64 1, kid: string, key: 32-byte binary, created_at: int64}: id %T, kid %T, key %T, created_at %T",
			epoch["id"], epoch["kid"], epoch["key"], epoch["created_at"])
	}

	if recordKey := blobRegistryRecordKey(); !bytes.Equal(recordKey, []byte{0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x0B}) {
		t.Fatalf("the node record's key is %x, want [0xFFFF][bucket id 0][0x0B]", recordKey)
	}
	var record bson.M
	if err := bson.Unmarshal(node.recordBytes(t), &record); err != nil {
		t.Fatalf("decoding the node record: %v", err)
	}
	_, createdIsDateTime := record["created_at"].(primitive.DateTime)
	if len(record) != 2 || record["format_version"] != int32(1) || !createdIsDateTime {
		t.Fatalf("the node record is not {format_version: int32 1, created_at: datetime}: %d fields, format_version %T, created_at %T",
			len(record), record["format_version"], record["created_at"])
	}
}
