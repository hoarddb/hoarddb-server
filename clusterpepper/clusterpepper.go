// Package clusterpepper is the cluster pepper of a ring cluster (ring user store spec 5): 32 random
// bytes generated once per cluster by the node that proposes USER_BOOTSTRAP, handed to every data
// member over the pinned control channel and kept in the sealed credential file
// auth.cluster.pepper. It never enters the Raft log, a snapshot or any replicated state.
//
// A stored password hash is HMAC-SHA256(key, client Argon2id output), where key is HKDF-SHA256 of
// the pepper bound to the cluster id, so a pepper copied into another cluster verifies nothing
// there. The kid names a pepper without revealing it.
//
// Neither Pepper nor Key ever prints its bytes: every fmt verb and slog print the kid.
package clusterpepper

import (
	"bytes"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Len is the length of a pepper and of a pepper key.
const Len = 32

// KIDLen is the length of a kid: lowercase hex characters (cluster.PepperKIDLen).
const KIDLen = 8

// FileName is the pepper's credential file in the data directory.
const FileName = "auth.cluster.pepper"

// FileMagic starts the plaintext form of the file, so a reader tells it from a damaged sealed
// envelope (whose magic is 0x00 'H' 'D' 'B') and says which (spec 17, B23).
const FileMagic = "HDBPEP1\n"

// The HKDF labels of spec 5.1. Changing any of them changes every stored hash.
const (
	hkdfSalt = "hoarddb-cluster-pepper-v1"
	keyInfo  = "cluster-pepper-v1:"
	kidInfo  = "cluster-pepper-kid-v1"
)

// Pepper is one cluster pepper. The zero value is no pepper.
//
// The bytes are reachable only through a function, never stored in the struct: fmt cannot call
// Format on a Pepper that is an unexported field of another value (reflect refuses Interface
// there) and then prints the field's contents. A byte array would print whole, and a pointer to one
// prints whole under a bad verb (%s, %q print the pointee at depth 0); a func prints as an address
// under every verb, and encoding/json skips the unexported field. A Pepper is never changed after
// it is made, so copies share the array safely.
type Pepper struct{ b func() *[Len]byte }

// Key is a pepper bound to one cluster id: what a stored hash is made and checked with. Its bytes
// are held the same way as Pepper's.
type Key struct{ b func() *[Len]byte }

// hold wraps a in the function a Pepper or Key keeps.
func hold(a *[Len]byte) func() *[Len]byte { return func() *[Len]byte { return a } }

// Generate draws a new pepper from crypto/rand. It takes no input: a pepper is not derived from
// any node's auth.token.key or anything else on the node.
func Generate() (Pepper, error) {
	drawn := new([Len]byte)
	if _, err := rand.Read(drawn[:]); err != nil {
		return Pepper{}, fmt.Errorf("drawing the cluster's password key from the system's random source: %w", err)
	}
	return Pepper{b: hold(drawn)}, nil
}

// FromBytes takes a pepper received from another member or read from the file.
func FromBytes(raw []byte) (Pepper, error) {
	if len(raw) != Len {
		return Pepper{}, fmt.Errorf("a cluster password key is %d bytes, and this one is %d", Len, len(raw))
	}
	held := new([Len]byte)
	copy(held[:], raw)
	return Pepper{b: hold(held)}, nil
}

// Bytes returns a copy of the pepper's bytes, for PEPPER_REPLY and the file only.
func (p Pepper) Bytes() []byte { return bytes.Clone(p.raw()) }

// raw is the pepper's bytes, all zero for no pepper.
func (p Pepper) raw() []byte {
	if p.b == nil {
		return make([]byte, Len)
	}
	return p.b()[:]
}

// Equal compares two peppers in constant time.
func (p Pepper) Equal(other Pepper) bool {
	return subtle.ConstantTimeCompare(p.raw(), other.raw()) == 1
}

// IsZero reports whether p is no pepper.
func (p Pepper) IsZero() bool { return p.Equal(Pepper{}) }

// bytes is the key's bytes, for this package's tests only.
func (k Key) bytes() []byte {
	if k.b == nil {
		return make([]byte, Len)
	}
	return bytes.Clone(k.b()[:])
}

func derive(secret []byte, info string) *[Len]byte {
	out, err := hkdf.Key(sha256.New, secret, []byte(hkdfSalt), info, Len)
	if err != nil {
		// hkdf.Key fails only for a length past 255 hash blocks or a FIPS-only refusal of a short
		// key; Len is one block and the secret is 32 bytes, so neither can happen.
		panic(fmt.Sprintf("clusterpepper: HKDF of %d bytes refused: %v", Len, err))
	}
	key := new([Len]byte)
	copy(key[:], out)
	return key
}

// KID names the pepper: the first 8 hex characters of SHA-256(HKDF(pepper, kid label)).
func (p Pepper) KID() string {
	derived := derive(p.raw(), kidInfo)
	sum := sha256.Sum256(derived[:])
	return hex.EncodeToString(sum[:])[:KIDLen]
}

// Key binds the pepper to clusterID (spec 5.1).
func (p Pepper) Key(clusterID [16]byte) Key {
	return Key{b: hold(derive(p.raw(), keyInfo+hex.EncodeToString(clusterID[:])))}
}

// Hash is the stored hash of a client-derived Argon2id output: HMAC-SHA256(key, clientHash), the
// construction of server/authkey.go:PepperHash.
func (k Key) Hash(clientHash []byte) []byte {
	mac := hmac.New(sha256.New, k.bytes())
	mac.Write(clientHash)
	return mac.Sum(nil)
}

// Verify reports, in constant time, whether stored is clientHash peppered under k.
func (k Key) Verify(clientHash, stored []byte) bool { return hmac.Equal(k.Hash(clientHash), stored) }

// String, GoString, Format, LogValue and MarshalJSON print the kid, never the bytes. They are
// cosmetic: the guarantee that no print shows the bytes is the func-valued field above, which holds
// also where fmt cannot call these methods (a Pepper in an unexported field of another value).
func (p Pepper) String() string { return "cluster password key " + p.KID() }

// GoString prints the kid.
func (p Pepper) GoString() string { return p.String() }

// Format prints the kid for every verb, %x and %d included.
func (p Pepper) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(p.String())) } //nolint:errcheck // fmt.State's Write reports nothing a Formatter can act on

// LogValue prints the kid.
func (p Pepper) LogValue() slog.Value { return slog.StringValue(p.String()) }

// MarshalJSON writes the kid, never the bytes.
func (p Pepper) MarshalJSON() ([]byte, error) { return json.Marshal(p.String()) }

// String prints that this is a key, never its bytes.
func (k Key) String() string { return "cluster password key (derived)" }

// GoString prints that this is a key.
func (k Key) GoString() string { return k.String() }

// Format prints that this is a key for every verb.
func (k Key) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte(k.String())) } //nolint:errcheck // as Pepper.Format

// LogValue prints that this is a key.
func (k Key) LogValue() slog.Value { return slog.StringValue(k.String()) }

// MarshalJSON writes that this is a key, never its bytes.
func (k Key) MarshalJSON() ([]byte, error) { return json.Marshal(k.String()) }

// SecretForms lists the spellings a secret could take in a log line or an error: raw, hex either
// case (also spaced, as % x prints it), standard and URL base64 with and without padding, fmt's
// decimal byte list and its %q form. Tests scan output for each.
func SecretForms(secret []byte) []string {
	lower := hex.EncodeToString(secret)
	digits := make([]string, len(secret))
	for i, b := range secret {
		digits[i] = strconv.Itoa(int(b))
	}
	decimal := strings.Join(digits, " ")
	quoted := strconv.Quote(string(secret))
	spaced := strings.TrimSpace(fmt.Sprintf("% x", secret))
	return []string{decimal, quoted[1 : len(quoted)-1], spaced, strings.ToUpper(spaced),
		string(secret), lower, upperHex(lower),
		base64.StdEncoding.EncodeToString(secret), base64.RawStdEncoding.EncodeToString(secret),
		base64.URLEncoding.EncodeToString(secret), base64.RawURLEncoding.EncodeToString(secret),
	}
}

func upperHex(lower string) string {
	out := []byte(lower)
	for i, c := range out {
		if c >= 'a' && c <= 'f' {
			out[i] = c - 'a' + 'A'
		}
	}
	return string(out)
}

// EncodeFile is the plaintext form of auth.cluster.pepper: FileMagic then the 32 bytes. A node at
// encryption.mode off writes it as it is (0600); a sealing node seals it under the node master.
func EncodeFile(p Pepper) []byte { return append([]byte(FileMagic), p.raw()...) }

// DecodeFile reads the plaintext form, refusing anything else by name.
func DecodeFile(contents []byte) (Pepper, error) {
	if !bytes.HasPrefix(contents, []byte(FileMagic)) {
		return Pepper{}, errors.New("it does not start with the cluster password key's header")
	}
	if len(contents) != len(FileMagic)+Len {
		return Pepper{}, fmt.Errorf("it is %d bytes, and a cluster password key file is %d", len(contents), len(FileMagic)+Len)
	}
	return FromBytes(contents[len(FileMagic):])
}

// File is the plaintext auth.cluster.pepper at Path, mode 0600: what a node at encryption.mode off
// keeps, and what the cluster harness uses. A sealing server keeps the same plaintext under its
// node master instead.
type File struct{ Path string }

// Load reads the pepper. A missing file is an error that matches fs.ErrNotExist.
func (f File) Load() (Pepper, error) {
	contents, err := os.ReadFile(f.Path)
	if err != nil {
		return Pepper{}, err
	}
	p, err := DecodeFile(contents)
	if err != nil {
		return Pepper{}, fmt.Errorf("%s is not a cluster password key file: %w. Move it aside and start the node: it asks another member for the cluster's key", f.Path, err)
	}
	return p, nil
}

// Store writes the pepper durably: a temporary file, fsync, rename, then a sync of the directory.
func (f File) Store(p Pepper) error { return WriteDurably(f.Path, EncodeFile(p)) }

// MoveAside renames the file to <path>.wrong-<kid> (or that name with a counter when it is taken),
// keeping it for the operator, and returns the new path. It never deletes a file.
func (f File) MoveAside(kid string) (string, error) { return MoveAside(f.Path, kid) }

// ErrSourceGone is what MoveAside answers when the file is no longer at its path: someone moved it
// already. Nothing was renamed and no name was picked.
var ErrSourceGone = errors.New("the cluster password key file is no longer at its path")

// ErrMovedNotSynced is what MoveAside answers when the rename succeeded and the directory sync after
// it failed: the file is moved, and a later write in the same directory syncs it again.
var ErrMovedNotSynced = errors.New("the file was moved aside but its directory could not be synced")

// MoveAside renames path to path.wrong-<kid>, adding .1, .2 ... when the name is taken, and syncs
// the directory. It never deletes a file. A file no longer at path is ErrSourceGone (nothing
// renamed); a rename whose directory sync failed returns the new path and ErrMovedNotSynced.
func MoveAside(path, kid string) (string, error) {
	if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("moving %s aside: %w", path, ErrSourceGone)
	} else if err != nil {
		return "", fmt.Errorf("checking %s before moving it aside: %w", path, err)
	}
	target := path + ".wrong-" + kid
	for i := 1; ; i++ {
		if _, err := os.Lstat(target); errors.Is(err, fs.ErrNotExist) {
			break
		} else if err != nil {
			return "", fmt.Errorf("checking %s before moving %s aside: %w", target, path, err)
		}
		target = fmt.Sprintf("%s.wrong-%s.%d", path, kid, i)
	}
	if err := os.Rename(path, target); err != nil {
		if _, statErr := os.Lstat(path); errors.Is(statErr, fs.ErrNotExist) {
			return "", fmt.Errorf("moving %s aside to %s: %w (%w)", path, target, ErrSourceGone, err)
		}
		return "", fmt.Errorf("moving %s aside to %s: %w", path, target, err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return target, fmt.Errorf("moved %s aside to %s: %w: %w", path, target, ErrMovedNotSynced, err)
	}
	return target, nil
}

// WriteDurably replaces path with contents at mode 0600: a temporary file beside it, fsync,
// rename, then a sync of the directory, so a crash leaves the old file or the new one whole.
func WriteDurably(path string, contents []byte) error {
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating %s: %w", tmp, err)
	}
	if _, err := file.Write(contents); err != nil {
		return errors.Join(fmt.Errorf("writing %s: %w", tmp, err), file.Close(), os.Remove(tmp))
	}
	if err := file.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing %s: %w", tmp, err), file.Close(), os.Remove(tmp))
	}
	if err := file.Close(); err != nil {
		return errors.Join(fmt.Errorf("closing %s: %w", tmp, err), os.Remove(tmp))
	}
	if err := os.Rename(tmp, path); err != nil {
		return errors.Join(fmt.Errorf("renaming %s to %s: %w", tmp, path, err), os.Remove(tmp))
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("opening %s to sync it: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		return errors.Join(fmt.Errorf("syncing %s: %w", dir, err), d.Close())
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", dir, err)
	}
	return nil
}
