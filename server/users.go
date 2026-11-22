package server

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/hoarddb/hoarddb-server/authrules"
	"github.com/hoarddb/hoarddb-server/config"
)

// UsersFileName is the on-disk auth store, held by every node (§5.1).
const UsersFileName = "users.json"

// usersFormatVersion is the only accepted on-disk layout. It is frozen once a
// users.json has shipped: there is no unpeppered record anywhere, so there is
// no migration path to invent (spec 3.2).
const usersFormatVersion = 1

// maxUsers is the supported ceiling (§9). A single document held in memory on
// every node and rewritten whole per mutation breaks on write amplification
// long before it breaks on the read path, so the boundary is stated and
// refused rather than discovered.
const maxUsers = authrules.MaxUsers

// ErrNoUsersFile marks a first start: no users.json has been written yet. It
// is not a failure; the bootstrap path in cmd/server seeds the file. Any
// other read error is.
var ErrNoUsersFile = errors.New("no users file")

// dirFsyncer is an injectable hook for testing the order of directory fsyncs
// relative to .prev deletion. It is called during Save to fsync the directory
// after the rename and before any .prev delete. In production, it performs the
// actual fsync; in tests, it can be replaced to track call order.
var dirFsyncer = func(dirPath string) error {
	dirFile, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer func() { _ = dirFile.Close() }()
	return dirFile.Sync()
}

// usersFileRenamer is an injectable hook for testing the order of operations
// during users.json save (rename, fsync, delete .prev). In production, it
// performs the actual rename; in tests, it can be replaced to track call order.
var usersFileRenamer = func(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

// prevFileDeleter is an injectable hook for testing the order of operations
// during users.json save (rename, fsync, delete .prev). In production, it
// performs the actual delete; in tests, it can be replaced to track call order.
var prevFileDeleter = func(path string) error {
	return os.Remove(path)
}

// usersSealing carries the encryption configuration for users.json saves,
// and the running work factor the saved records are validated against.
type usersSealing struct {
	Keyring    *Keyring
	NodeMaster *NodeMaster
	Mode       string
	// Argon2 is the server's configured work factor. The clamp's upper bounds
	// are relative to it, so a save judged against the package default would
	// refuse a server's own credentials once it is tuned above that default.
	// Zero fields mean the package default.
	Argon2 Argon2Params
}

// usersConfig is the whole {dataDir}/users.json document (§5.1): the fence
// (term, version), the master that owns it, and the records.
type usersConfig struct {
	FormatVersion int          `json:"format_version"`
	Term          uint64       `json:"term"`    // membership epoch that elected the master
	Version       uint64       `json:"version"` // increments per accepted change within a term
	Master        string       `json:"master"`
	Users         []UserRecord `json:"users"`
}

// UsersFilePath returns the location of the user store inside dataDir. One
// name, one place, so no caller invents a different path.
func UsersFilePath(dataDir string) string {
	return filepath.Join(dataDir, UsersFileName)
}

// LoadUsersConfig reads and parses the user store. A missing file returns
// ErrNoUsersFile so the caller can distinguish first start from failure. A
// corrupt file is an error naming the path: silently starting empty would
// turn every password reset into an open database.
//
// A file carrying the encrypted magic header is decrypted with the keyring
// and/or node master first; a file without it is the legacy plaintext JSON
// and is parsed as-is. That is the upgrade path — no flag, no migration
// (spec encryption §3). The dispatch (plaintext, legacy, or new sealed format)
// uses decryptConfigDispatch.
func LoadUsersConfig(path string, keyring *Keyring, nodeMaster *NodeMaster) (*usersConfig, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNoUsersFile
	}
	if err != nil {
		return nil, fmt.Errorf("read users file %s: %w", path, err)
	}
	if isEncryptedUsersFile(data) {
		plaintext, err := decryptConfigDispatch(data, keyring, nodeMaster)
		if err != nil {
			return nil, fmt.Errorf("decrypt users file %s: %w", path, err)
		}
		data = plaintext
	}
	var cfg usersConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse users file %s: %w", path, err)
	}
	return &cfg, nil
}

// NewerThan reports whether this config is newer than the given (term,
// version). Term dominates: a higher term always wins, and within a term only
// a higher version does. A config that is not newer is ignored (§5.1).
func (c *usersConfig) NewerThan(term, version uint64) bool {
	if c.Term != term {
		return c.Term > term
	}
	return c.Version > version
}

// Save writes the store atomically at 0600 using the five-step order of §4 step 5:
//  1. Write the new users.json to a temp file
//  2. Fsync the temp file
//  3. Rename the temp file onto users.json
//  4. Fsync the directory
//  5. Delete .prev only if the sealing kid is about to change, including
//     no key -> a key (the first sealed save over a plaintext file)
//
// At mode=on or mode=require, a nil or empty node master is a refusal, not a
// plaintext write.
func (c *usersConfig) Save(path string, sealing usersSealing) error {
	if err := c.validateWith(sealing.Argon2.withDefaults()); err != nil {
		return fmt.Errorf("refusing to save invalid users file %s: %w", path, err)
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal users file: %w", err)
	}

	// Determine the sealing kid before and after
	var oldKid string
	var newKid string
	var sealedData []byte

	// At mode=on or mode=require, a nil or empty node master is a refusal
	if (sealing.Mode == "on" || sealing.Mode == "require") && sealing.NodeMaster == nil {
		return fmt.Errorf("cannot save users.json: mode=%s requires a node master", sealing.Mode)
	}

	// The file being replaced gives the old kid and the next generation.
	existing, readErr := os.ReadFile(path)
	if readErr != nil && !errors.Is(readErr, fs.ErrNotExist) {
		return fmt.Errorf("read existing users file %s: %w", path, readErr)
	}
	hadExistingFile := readErr == nil
	if isEncryptedUsersFile(existing) && len(existing) >= 14 {
		oldKid = string(existing[6:14])
	}

	// Encrypt if we have a node master and mode allows it
	if sealing.NodeMaster != nil && (sealing.Mode == "on" || sealing.Mode == "require") {
		gen, err := usersFileGen(existing)
		if err != nil {
			return fmt.Errorf("refusing to save users file %s: %w", path, err)
		}
		sealed, err := sealUsersWithNodeMaster(data, sealing.NodeMaster, gen)
		if err != nil {
			return fmt.Errorf("seal users file %s: %w", path, err)
		}
		sealedData = sealed
		newKid = sealing.NodeMaster.Kid()
	} else if sealing.Keyring != nil {
		// For backward compat: use legacy encryption with auth keyring if mode=off
		if key := sealing.Keyring.currentNoPrune(); len(key.Bytes) > 0 {
			encrypted, err := encryptConfig(data, key.Bytes, key.ID)
			if err != nil {
				return fmt.Errorf("encrypt users file %s: %w", path, err)
			}
			sealedData = encrypted
			newKid = key.ID
		} else {
			sealedData = data
		}
	} else {
		// Plaintext
		sealedData = data
	}

	// Step 1: Write to temp file
	tmp, err := os.CreateTemp(filepath.Dir(path), ".users-*.json")
	if err != nil {
		return fmt.Errorf("create temp users file: %w", err)
	}
	tmpName := tmp.Name()
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod temp users file: %w", err)
	}
	if _, err := tmp.Write(sealedData); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("write temp users file: %w", err)
	}

	// Step 2: Fsync temp file
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("sync temp users file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("close temp users file: %w", err)
	}

	// Keep the previous file before replacing it
	if prev, readErr := os.ReadFile(path); readErr == nil {
		if err := os.WriteFile(path+".prev", prev, 0600); err != nil {
			_ = os.Remove(tmpName)
			return fmt.Errorf("write previous users file %s.prev: %w", path, err)
		}
	} else if !errors.Is(readErr, fs.ErrNotExist) {
		_ = os.Remove(tmpName)
		return fmt.Errorf("read existing users file %s: %w", path, readErr)
	}

	// Step 3: Rename
	if err := usersFileRenamer(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("replace users file %s: %w", path, err)
	}

	// Step 4: Fsync directory
	if err := dirFsyncer(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync users directory: %w", err)
	}

	// Step 5: Delete .prev whenever the sealing kid is about to change over
	// an existing file, including no key -> a key: oldKid == ""
	// on the first sealed save over a plaintext users.json, and the
	// plaintext .prev written above must not survive that transition. The
	// hadExistingFile guard excludes the very first save ever (no file to
	// back up, so no .prev was written this call).
	kidChanged := hadExistingFile && ((oldKid == "" && newKid != "") || (oldKid != "" && newKid != "" && oldKid != newKid))
	if kidChanged {
		if delErr := prevFileDeleter(path + ".prev"); delErr != nil {
			return fmt.Errorf("removing previous credentials backup %s: %w", path+".prev", delErr)
		}
	}

	return nil
}

// sweepPlaintextPrev removes path+".prev" when path is sealed and the .prev is
// not. Save writes the pre-seal plaintext to .prev on the first sealing save
// and deletes it only after the rename and the directory fsync, so a crash in
// between, or a failure of either step, leaves the plaintext credential store
// beside the sealed one with nothing else to remove it (at-rest-encryption
// spec §4 step 5). A sealed .prev is rotation's fallback and is kept, as is
// any .prev beside a plaintext users.json. It reports whether it removed one.
func sweepPlaintextPrev(path string) (bool, error) {
	current, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("read users file %s: %w", path, err)
	}
	if !isEncryptedUsersFile(current) {
		return false, nil
	}
	prevPath := path + ".prev"
	prev, err := os.ReadFile(prevPath)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read previous users file %s: %w", prevPath, err)
	}
	if isEncryptedUsersFile(prev) {
		return false, nil
	}
	if err := os.Remove(prevPath); err != nil {
		return false, fmt.Errorf("%s holds the unencrypted credential store beside the encrypted %s and could not be removed: %w; "+
			"delete it by hand (it is not needed: %s is the current store), then start the server again",
			prevPath, path, err, path)
	}
	return true, nil
}

// Validate refuses a logically-wrong file, not only a corrupt one (§5.1). It
// runs before applying a push and before writing locally, so one bad config
// cannot brick every node's startup. Each refusal names the problem and the
// way out.
func (c *usersConfig) Validate() error {
	return c.validateWith(defaultArgon2Params())
}

// validateWith is Validate against a caller-supplied server default, so the
// upper bounds follow the running configuration rather than a fixed package
// literal. The handler uses it on receipt (§6.2).
func (c *usersConfig) validateWith(configuredDefault Argon2Params) error {
	if c.FormatVersion != usersFormatVersion {
		return fmt.Errorf("unsupported format_version %d; this server speaks %d — restore %s.prev or migrate the file explicitly",
			c.FormatVersion, usersFormatVersion, UsersFileName)
	}
	if len(c.Users) == 0 {
		return fmt.Errorf("no users: an empty user list is refused because starting with it would make every password reset an open database")
	}
	if len(c.Users) > maxUsers {
		return fmt.Errorf("holds %d users, over the supported limit of %d; move older accounts out before adding more",
			len(c.Users), maxUsers)
	}
	seenUsers := make(map[string]struct{}, len(c.Users))
	admin := false
	for i := range c.Users {
		record := &c.Users[i]
		if record.Username == "" {
			return fmt.Errorf("user %d has an empty name", i)
		}
		if _, dup := seenUsers[record.Username]; dup {
			return fmt.Errorf("user %q appears more than once", record.Username)
		}
		seenUsers[record.Username] = struct{}{}
		if err := validateCredentialClamp(record.Credentials, configuredDefault); err != nil {
			return fmt.Errorf("user %q: %w", record.Username, err)
		}
		if record.Enabled && record.hasAdminGrant() {
			admin = true
		}
	}
	if err := c.errRetiredRoles(); err != nil {
		return err
	}
	if !admin {
		return fmt.Errorf("no enabled user holds root or user-manager on every database, the accounts that can manage users; refusing a config that would lock everyone out. " +
			"A cluster-manager does not count, and --reset-password refuses this file for the same reason. " +
			"If users.json is plaintext JSON (a file from before encryption), give an enabled account root@* or user-manager@* in its roles and start the server again; an encrypted file cannot be edited by hand. " +
			"Otherwise stop the server, move the file aside and start it with HOARDDB_ROOT_PASSWORD set (auth_bootstrap_root on, the default): that seeds a new root user, and every other account in the old file is lost")
	}
	return nil
}

// validateCredentialMaterial judges credential material a client has just
// sent (USER_CREATE, USER_ALTER): the §6.2 clamp plus the salt encoding. The
// file-level validateWith runs only the clamp, because a salt defect makes one
// account unusable and must not stop a server booting, saving or applying a
// push; warnUnusableSalts reports such accounts instead.
func validateCredentialMaterial(material CredentialMaterial, configuredDefault Argon2Params) error {
	if err := validateCredentialClamp(material, configuredDefault); err != nil {
		return err
	}
	return validateCredentialSalt(material)
}

// validateCredentialSalt refuses a salt that VerifyUser could never decode,
// and one whose decoded length is not the declared salt_len, so salt_len is
// truthful and the 16-byte floor holds for the bytes actually hashed (BUG-0629).
func validateCredentialSalt(material CredentialMaterial) error {
	const saltFix = "credential salt must be standard base64 of salt_len (16+) bytes; the client derives it — check the driver's credential encoding"
	saltBytes, err := base64.StdEncoding.DecodeString(material.Salt)
	if err != nil {
		return fmt.Errorf("%s (got %q: %v)", saltFix, saltPrefix(material.Salt), err)
	}
	if len(saltBytes) != material.Params.SaltLen {
		return fmt.Errorf("%s (the salt decodes to %d bytes but params salt_len is %d)",
			saltFix, len(saltBytes), material.Params.SaltLen)
	}
	return nil
}

// saltPrefix shortens a rejected salt for an error message so a refusal never
// echoes more than a hint of what the client sent.
func saltPrefix(salt string) string {
	const shown = 12
	if len(salt) <= shown {
		return salt
	}
	return salt[:shown] + "..."
}

// warnUnusableSalts logs every account whose stored salt VerifyUser cannot
// use. It does not refuse: the account only fails to log in, and refusing
// would turn a stale record into a boot or replication outage.
func (c *usersConfig) warnUnusableSalts(logger *slog.Logger, source string) {
	for i := range c.Users {
		if err := validateCredentialSalt(c.Users[i].Credentials); err != nil {
			logger.Warn("a user's stored salt is unusable, so the account cannot log in; reset it with 'alter user <name> password' or --reset-password",
				"user", c.Users[i].Username, "source", source, "error", err)
		}
	}
}

// validateCredentialClamp enforces the §6.2 clamp. The client chooses the
// work factor, so every field lands on the Argon2 verification path and must
// be inside a window: too low is an account with no work factor, too high is a
// memory bomb planted through the legitimate creation path.
func validateCredentialClamp(material CredentialMaterial, configuredDefault Argon2Params) error {
	if material.Algo != "argon2id" {
		return fmt.Errorf("credentials algo %q is not supported; the only accepted value is \"argon2id\"", material.Algo)
	}
	if material.Salt == "" {
		return errors.New("credentials salt is empty")
	}
	if material.Hash == "" {
		return errors.New("credentials hash is empty")
	}
	params := material.Params
	maxMemory := config.Argon2WorkFactorScale * configuredDefault.Memory
	if params.Memory < config.MinArgon2Memory || params.Memory > maxMemory {
		return fmt.Errorf("credentials params memory %d is outside the permitted range [%d, %d]",
			params.Memory, config.MinArgon2Memory, maxMemory)
	}
	if params.Iterations < config.MinArgon2Iterations {
		return fmt.Errorf("credentials params iterations %d is below the minimum of %d",
			params.Iterations, config.MinArgon2Iterations)
	}
	maxIterations := config.Argon2WorkFactorScale * max(configuredDefault.Iterations, config.MinArgon2Iterations)
	if params.Iterations > maxIterations {
		return fmt.Errorf("credentials params iterations %d is outside the permitted range [%d, %d]; derive the credential again with iterations in that range (the server's configured default is %d), or raise auth.argon2_iterations if this is the server's own work factor",
			params.Iterations, config.MinArgon2Iterations, maxIterations, configuredDefault.Iterations)
	}
	if params.Parallelism < config.MinArgon2Parallelism || params.Parallelism > config.MaxArgon2Parallelism {
		return fmt.Errorf("credentials params parallelism %d is outside the permitted range [%d, %d]; derive the credential again with a parallelism in that range (the server default is %d), or set auth.argon2_parallelism inside it if this is the server's own work factor",
			params.Parallelism, config.MinArgon2Parallelism, config.MaxArgon2Parallelism, defaultArgon2Parallelism)
	}
	if params.SaltLen < defaultArgon2SaltLen {
		return fmt.Errorf("credentials params salt_len %d is below the minimum of %d",
			params.SaltLen, defaultArgon2SaltLen)
	}
	if params.HashLen != defaultArgon2HashLen {
		return fmt.Errorf("credentials params hash_len %d must be %d",
			params.HashLen, defaultArgon2HashLen)
	}
	return nil
}

// defaultArgon2Params is the package default work factor, used where no
// configured default is threaded through (tests, validation on load).
func defaultArgon2Params() Argon2Params {
	return Argon2Params{
		Memory:      defaultArgon2Memory,
		Iterations:  defaultArgon2Iterations,
		Parallelism: defaultArgon2Parallelism,
		SaltLen:     defaultArgon2SaltLen,
		HashLen:     defaultArgon2HashLen,
	}
}
