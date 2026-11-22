package server

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/hoarddb/hoarddb-server/sealed"
)

// usersFileMagic is the 4-byte marker that separates an encrypted users file
// from a legacy plaintext one, and usersFileVersion is the 2-byte layout
// version that follows it (spec §2). The detection rule is "first 4 bytes
// equal the magic"; anything else is treated as the plain JSON the pre-
// encryption builds wrote. Loading is therefore seamless: no flag, no
// migration command, no second file.
var (
	usersFileMagic   = []byte{0x00, 'H', 'D', 'B'}
	usersFileVersion = []byte{0x00, 0x01}
)

// The fixed header layout of an encrypted users file (spec §2):
//
//	usersFileMagic (4) | usersFileVersion (2) | kid (8) | nonce (12) | AEAD output
//
// The kid is the hex kid the keyring minted, stored as its first 8 bytes
// (kids are 8 hex characters today, so the field holds the whole kid). The
// nonce is the standard AES-GCM size. Everything after the 26-byte header is
// ciphertext with the 16-byte GCM tag appended.
const (
	usersFileMagicLen   = 4
	usersFileVersionLen = 2
	usersFileKIDLen     = 8
	usersFileNonceLen   = 12
	usersHeaderLen      = usersFileMagicLen + usersFileVersionLen + usersFileKIDLen + usersFileNonceLen
)

// encryptConfig seals plaintext with AES-256-GCM under key, prefixed by the
// magic, the version, the kid and a fresh random nonce. The signing key is 32
// bytes (authKeyLen), which is exactly one AES-256 key, so the bytes are used
// directly — no derivation, no format change to auth.token.key.
// It is the mode=off writer: with no node master, usersConfig.Save seals
// users.json this way under the auth signing key, exactly as before
// encryption at rest existed. Under mode=on or require the node master seals
// it instead, through sealUsersWithNodeMaster.
func encryptConfig(plaintext, key []byte, kid string) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("users file encryption: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("users file encryption: %w", err)
	}
	nonce := make([]byte, usersFileNonceLen)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("users file encryption nonce: %w", err)
	}

	sealed := gcm.Seal(nil, nonce, plaintext, nil)

	out := make([]byte, 0, usersHeaderLen+len(sealed))
	out = append(out, usersFileMagic...)
	out = append(out, usersFileVersion...)
	out = append(out, []byte(kid)[:usersFileKIDLen]...)
	out = append(out, nonce...)
	out = append(out, sealed...)
	return out, nil
}

// ErrUsersKeyMissing is a users.json sealed under a key the keyring does not
// hold. On a cluster member the keyring comes from the cluster token, so the
// caller says that token changed (NewService).
var ErrUsersKeyMissing = errors.New("users.json is sealed under a key this node does not hold")

// decryptConfig reverses encryptConfig: it checks the magic and the version,
// reads the kid and nonce from the header, looks the kid up in the keyring,
// and returns the plaintext. An unknown kid is the same class of failure as
// ErrPepperKeyMissing and names the way out, because the file cannot be
// decrypted without the key material it was written under.
func decryptConfig(ciphertext []byte, keyring *Keyring) ([]byte, error) {
	if len(ciphertext) < usersHeaderLen {
		return nil, fmt.Errorf("users file is too short to be an encrypted users file: %d bytes", len(ciphertext))
	}
	if !bytes.Equal(ciphertext[:len(usersFileMagic)], usersFileMagic) {
		return nil, fmt.Errorf("users file is not an encrypted users file (missing magic header)")
	}
	if !bytes.Equal(ciphertext[len(usersFileMagic):usersHeaderLen-usersFileKIDLen-usersFileNonceLen], usersFileVersion) {
		return nil, fmt.Errorf("users file uses an unsupported encryption version; this build reads version %x", usersFileVersion)
	}
	kid := string(ciphertext[len(usersFileMagic)+len(usersFileVersion) : len(usersFileMagic)+len(usersFileVersion)+usersFileKIDLen])
	nonce := ciphertext[usersHeaderLen-usersFileNonceLen : usersHeaderLen]
	sealed := ciphertext[usersHeaderLen:]

	if keyring == nil {
		return nil, fmt.Errorf("%w: users.json is encrypted with key %s, but no keyring was provided — restore the original auth.token.key or lose the users", ErrUsersKeyMissing, kid)
	}
	key, ok := keyring.KeyByID(kid)
	if !ok {
		return nil, fmt.Errorf("%w: users.json is encrypted with key %s, but that key is not in the keyring — restore the original auth.token.key or lose the users", ErrUsersKeyMissing, kid)
	}

	block, err := aes.NewCipher(key.Bytes)
	if err != nil {
		return nil, fmt.Errorf("users file decryption: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("users file decryption: %w", err)
	}
	plaintext, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return nil, fmt.Errorf("users file integrity check failed: %w", err)
	}
	return plaintext, nil
}

// isEncryptedUsersFile reports whether data carries the magic header that
// marks an encrypted users file ("first 4 bytes are \x00HDB", spec §2).
func isEncryptedUsersFile(data []byte) bool {
	return len(data) >= len(usersFileMagic) && bytes.Equal(data[:len(usersFileMagic)], usersFileMagic)
}

// usersFileGen is the envelope generation a save writes over existing (spec
// §2.3): the generation of the sealed file it replaces, plus one, so no two
// saves seal under the same derived key. A predecessor with no generation (no
// file, plaintext JSON, the legacy {0x00, 0x01} envelope) is replaced at 1.
// The header is read before any key is used, so an edited generation can at
// worst make one derived key seal a second message under a fresh nonce, the
// rollback case §2.3 accepts. It never wraps to 0.
func usersFileGen(existing []byte) (uint64, error) {
	if len(existing) < sealed.EnvelopeHeaderLen || !isEncryptedUsersFile(existing) || existing[4] != sealed.Version {
		return 1, nil
	}
	gen := binary.BigEndian.Uint64(existing[14:22])
	if gen == math.MaxUint64 {
		return 0, fmt.Errorf("its envelope generation is %d, the largest a header can hold, so the file was edited; restore it from a backup", gen)
	}
	return gen + 1, nil
}

// sealUsersWithNodeMaster seals plaintext users.json under the node master,
// using the sealed.Seal primitive with purpose 0x01 (users), deriving from
// the node master, and identity "users/" + gen.
func sealUsersWithNodeMaster(plaintext []byte, nodeMaster *NodeMaster, gen uint64) ([]byte, error) {
	if nodeMaster == nil {
		return nil, fmt.Errorf("cannot seal users.json: node master is nil")
	}
	sealed, err := sealed.Seal(sealed.PurposeUsers, nodeMaster.Bytes(), "users", gen, plaintext)
	if err != nil {
		return nil, fmt.Errorf("sealing users.json: %w", err)
	}
	return sealed, nil
}

// openUsersWithNodeMaster opens a users.json sealed under the node master,
// returning the plaintext and the generation that was read from the header.
func openUsersWithNodeMaster(ciphertext []byte, nodeMaster *NodeMaster) ([]byte, uint64, error) {
	if nodeMaster == nil {
		return nil, 0, fmt.Errorf("users.json is sealed under a node master key, but no keystore is configured for this command or start. " +
			"Run it with the encryption settings this data directory was created with: encryption.keystore and its key, and encryption.mode on or require " +
			"(HOARDDB_ENCRYPTION_KEYSTORE, HOARDDB_ENCRYPTION_KEY_FILE, HOARDDB_ENCRYPTION_MODE, or the same configuration file)")
	}
	plaintext, gen, err := sealed.Open(sealed.PurposeUsers, nodeMaster.Bytes(), func(g uint64) string {
		return fmt.Sprintf("users/%d", g)
	}, ciphertext)
	if err != nil {
		return nil, 0, fmt.Errorf("opening sealed users.json: %w", err)
	}
	return plaintext, gen, nil
}

// decryptConfigDispatch handles the three-way format detection and dispatch
// for loading users.json: plaintext, legacy sealed (v0x01), or new sealed (v0x02).
func decryptConfigDispatch(ciphertext []byte, keyring *Keyring, nodeMaster *NodeMaster) ([]byte, error) {
	// Check if plaintext
	if !isEncryptedUsersFile(ciphertext) {
		return ciphertext, nil
	}

	// Has magic; check version bytes
	if len(ciphertext) < 6 {
		return nil, fmt.Errorf("encrypted users file is too short")
	}

	versionBytes := ciphertext[4:6]

	// Legacy format: {0x00, 0x01}
	if bytes.Equal(versionBytes, usersFileVersion) {
		return decryptConfig(ciphertext, keyring)
	}

	// New format: 0x02 (version byte at offset 4)
	if ciphertext[4] == 0x02 {
		plaintext, _, err := openUsersWithNodeMaster(ciphertext, nodeMaster)
		return plaintext, err
	}

	// Unknown version
	return nil, fmt.Errorf("users file uses an unsupported encryption version %x; this build reads versions 0001, 02",
		versionBytes)
}
