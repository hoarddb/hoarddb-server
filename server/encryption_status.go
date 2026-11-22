package server

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
)

// EncryptionStatus is what an operator is told about a node's encryption at
// rest when it starts: the mode, the keystore, the key ids, and where the
// sealed store and its wrapped keys live. It never carries key bytes, so it is
// safe to print and to log (spec §2.6, §3 step 10).
type EncryptionStatus struct {
	// Mode is encryption.mode: off, on or require.
	Mode string
	// Keystore is encryption.keystore: off, keyfile, command or systemd-creds.
	Keystore string
	// KEKKid and NodeMasterKid are the key ids (the first 8 hex characters of
	// each key's SHA-256), empty when the keystore is off.
	KEKKid        string
	NodeMasterKid string
	// FirstStart reports that this start generated the node master: the one
	// start on which the key-loss warning is printed in full.
	FirstStart bool
	// StoreDir is the node store, the one Badger directory every bucket lives in.
	StoreDir string
	// NodeMasterKeyPath is the wrapped node master, empty when the keystore is
	// off. StoreKeyPath is the wrapped store data key, empty when the store is
	// not sealed.
	NodeMasterKeyPath string
	StoreKeyPath      string
	// StoreSealed is the structural check's result, computed once after the
	// store opened; StoreProblem says why it is false for a node that was
	// configured to seal and could not be confirmed.
	StoreSealed  bool
	StoreProblem string
}

// Sealed reports whether the node's store is sealed, by the structural check
// of its files and not by the mode: a store that merely claims to be sealed in
// the config is not reported as sealed (spec §5.3).
func (st EncryptionStatus) Sealed() bool {
	return st.StoreSealed
}

// storeSealCheck is the result of the structural check of the node's store,
// taken once after the store opens.
type storeSealCheck struct {
	// sealed is true only when the check found nothing wrong.
	sealed bool
	// problem is the first thing the check found wrong in a store that the
	// mode says is sealed, and empty otherwise. A plaintext node has no
	// problem: it was never meant to be sealed.
	problem string
}

// checkStoreSeal runs the structural check on the Badger store in storeDir, for
// a node started in mode. It is called once per store, right after the store
// opens, because the check reads the store's files beside the running Badger.
func checkStoreSeal(storeDir, mode string) storeSealCheck {
	problems := storeStructureProblems(storeDir)
	if len(problems) == 0 {
		return storeSealCheck{sealed: true}
	}
	if !modeSealsStore(mode) {
		return storeSealCheck{}
	}
	return storeSealCheck{problem: problems[0]}
}

// errRequireRingStoreNotSealed is the refusal of a require node whose ring
// store the structural check, run before the store opens, finds not sealed. A
// cluster member is not told to move its ring aside (wire.ClusterMemberRingAdvice).
func errRequireRingStoreNotSealed(storeDir, problem string, member bool) error {
	ringDir := filepath.Dir(storeDir)
	recovery := fmt.Sprintf("Move %s aside (keep it) and restore the ring directory from a backup", ringDir)
	if member {
		recovery = wire.ClusterMemberRingAdvice(ringDir)
	}
	return fmt.Errorf("encryption.mode is require but the ring store at %s is not sealed: %s. Nothing was opened or "+
		"written. %s; or, to start anyway while you investigate, set encryption.mode to on (HOARDDB_ENCRYPTION_MODE=on), "+
		"which reports the finding in /health and warns instead of refusing", storeDir, problem, recovery)
}

// errRequireStoreNotSealed is the refusal of a require node whose store the
// structural check could not confirm as sealed. The fix is named for both
// causes: a damaged store is restored, and a node that may serve while the
// operator investigates is started in mode on.
func errRequireStoreNotSealed(problem string) error {
	return fmt.Errorf("encryption.mode is require but the node store is not sealed: %s. "+
		"Restore the node store from a backup, or to start anyway while you investigate set encryption.mode to on (HOARDDB_ENCRYPTION_MODE=on), "+
		"which warns instead of refusing", problem)
}

// refuseUnsealedRequireStore is the require node's gate on a store that
// already exists. A first start has no MANIFEST yet and nothing to refuse; the
// check after the store opens covers the rest.
func refuseUnsealedRequireStore(cfg *config.Config) error {
	if cfg.Encryption.Mode != "require" {
		return nil
	}
	manifestPath := filepath.Join(cfg.Storage.DataDir, nodeStoreDirName, "MANIFEST")
	if _, err := os.Stat(manifestPath); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("checking the node store before opening it: %w", err)
	}
	if problems := storeStructureProblems(filepath.Join(cfg.Storage.DataDir, nodeStoreDirName)); len(problems) > 0 {
		return errRequireStoreNotSealed(problems[0])
	}
	return nil
}

// notSealedByDesign is what a node writes in the clear whatever the mode
// (ring encryption at rest spec §3.2), as the encryption verb lists it. Each
// entry names a thing and why, never a value.
var notSealedByDesign = []string{
	"other nodes of the cluster: each node seals only what it stores, and only with its own encryption on; the cluster does not check that every node uses the same setting",
	"ring/raft: cluster metadata (database names, member ids and addresses, the cluster id, settings, seed bitmaps and bucket ids)",
	"server logs: they may contain document ids, bucket names and store-field keys",
	"dumps and the node-master escrow export: written in plaintext whatever the mode",
	"memory and swap, including decrypted table indexes in the index cache",
	"freed disk blocks of rewritten or removed files",
	"the ring store's MANIFEST, DISCARD, memtable-size and the entry headers in its logs: table levels, lengths and expiry times",
	"the ring log's segment headers and each entry's length, checksum, generation and sequence",
	"ring/FORMAT, ring/incarnation and the cluster-version marker: no names, keys or values",
	"keys/server.crt: the node's TLS certificate, which is public",
	"root.password: the first-run credential, while it exists, mode 0600",
	"the audit log: user names, source addresses and targets, mode 0600",
}

// healthEncryption is the node's encryption as the encryption verb reports it.
// Once the ring node has opened it reports the ring store's structural check,
// with the earlier store's finding appended after "node store: " while that
// store shares the data directory (ring encryption at rest spec §8); before,
// the earlier store's check alone.
func (s *Server) healthEncryption() *HealthEncryption {
	seal := s.storeSeal
	if ringSeal := s.ringSeal.Load(); ringSeal != nil {
		seal = *ringSeal
		if s.storeSeal.problem != "" {
			seal.problem = strings.TrimPrefix(seal.problem+"; node store: "+s.storeSeal.problem, "; ")
		}
	}
	report := &HealthEncryption{
		Mode:      s.config.Encryption.Mode,
		Keystore:  s.config.Encryption.Keystore,
		Sealed:    seal.sealed,
		Problem:   seal.problem,
		NotSealed: notSealedByDesign,
	}
	if s.keystore != nil {
		report.KEKKid = s.keystore.Kid()
	}
	if s.nodeMaster != nil {
		report.NodeMasterKid = s.nodeMaster.Kid()
	}
	return report
}

// auditKeyUnwrap records that this start unwrapped the node master. It names
// the key id and writes nothing when the node has no keystore. A failed unwrap
// never reaches this method; prepare records it separately through
// AuditFailedKeyUnwrap, because the service and its audit logger do not exist
// yet then (BUG-0315).
func (s *Server) auditKeyUnwrap() {
	if s.nodeMaster == nil {
		return
	}
	s.audit.Log(AuditEntry{
		Time:   s.keyUnwrapAt,
		Event:  EventKeyUnwrap,
		Result: "ok",
		Target: "node-master " + s.nodeMaster.Kid(),
	})
}

// encryptedLabel is the node's state as describe and show buckets print it.
func (s *Server) encryptedLabel() string {
	if s.storeSeal.sealed {
		return "yes"
	}
	return "no"
}

// EncryptionStatus reports the node's encryption state, resolved once by
// NewService.
func (svc *Service) EncryptionStatus() EncryptionStatus {
	core := svc.core
	dataDir := core.config.Storage.DataDir
	status := EncryptionStatus{
		Mode:         core.config.Encryption.Mode,
		Keystore:     core.config.Encryption.Keystore,
		StoreDir:     filepath.Join(dataDir, nodeStoreDirName),
		StoreSealed:  core.storeSeal.sealed,
		StoreProblem: core.storeSeal.problem,
	}
	if core.keystore != nil {
		status.KEKKid = core.keystore.Kid()
	}
	if core.nodeMaster != nil {
		status.NodeMasterKid = core.nodeMaster.Kid()
		status.FirstStart = core.nodeMaster.Created()
		status.NodeMasterKeyPath = filepath.Join(dataDir, "keys", NodeMasterFileName)
	}
	if status.Sealed() {
		status.StoreKeyPath = filepath.Join(dataDir, "keys", nodeStoreKeyFileName)
	}
	return status
}
