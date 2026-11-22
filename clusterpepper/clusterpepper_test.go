package clusterpepper

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func vectorPepper(t *testing.T) Pepper {
	t.Helper()
	raw := make([]byte, Len)
	for i := range raw {
		raw[i] = byte(i)
	}
	p, err := FromBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func vectorClusterID() [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = 0xA0 + byte(i)
	}
	return id
}

// TestTheConstructionMatchesTheSpecVector pins spec 5.1 to numbers computed outside Go (Python's
// hmac and hashlib): key = HKDF-SHA256(pepper, "hoarddb-cluster-pepper-v1", "cluster-pepper-v1:" +
// hex(cluster id)), kid = first 8 hex of SHA-256(HKDF(pepper, same salt, "cluster-pepper-kid-v1")),
// stored hash = HMAC-SHA256(key, client hash).
func TestTheConstructionMatchesTheSpecVector(t *testing.T) {
	p := vectorPepper(t)
	if got := p.KID(); got != "647fb06d" {
		t.Fatalf("kid = %s, want 647fb06d", got)
	}
	key := p.Key(vectorClusterID())
	if got := hex.EncodeToString(key.bytes()); got != "626622c563b09f945d721ff91a56201fb58f0d016cb2088ca8afe6bed2601691" {
		t.Fatalf("key = %s", got)
	}
	if got := hex.EncodeToString(key.Hash(bytes.Repeat([]byte{0x55}, 32))); got != "dc5668d2e6f8cc4ce6de354561ef8a7ed5cfb621530e2412f72e047a41e2dc2f" {
		t.Fatalf("stored hash = %s", got)
	}
}

// TestThePepperKeyIsBoundToTheClusterID (spec 5.1): one pepper under two cluster ids gives two
// keys, so a hash peppered in one cluster verifies nothing in another. Break: leave the cluster id
// out of the HKDF info; red.
func TestThePepperKeyIsBoundToTheClusterID(t *testing.T) {
	p := vectorPepper(t)
	a, b := vectorClusterID(), vectorClusterID()
	b[15] ^= 1
	clientHash := bytes.Repeat([]byte{0x33}, 32)
	if bytes.Equal(p.Key(a).Hash(clientHash), p.Key(b).Hash(clientHash)) {
		t.Fatal("one pepper peppers a hash the same way in two clusters")
	}
	if !p.Key(a).Verify(clientHash, p.Key(a).Hash(clientHash)) {
		t.Fatal("a hash does not verify under the key that made it")
	}
	if p.Key(b).Verify(clientHash, p.Key(a).Hash(clientHash)) {
		t.Fatal("a hash made in one cluster verifies in another")
	}
}

func TestGenerateDrawsAFreshPepperEachTime(t *testing.T) {
	first, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	second, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if first.Equal(second) || first.KID() == second.KID() {
		t.Fatal("two generated peppers are equal")
	}
	if first.Equal(Pepper{}) {
		t.Fatal("a generated pepper is all zero")
	}
}

// TestAPepperNeverPrintsItsBytes: every fmt verb and slog print the kid, never the bytes, so a
// pepper passed to a log line or an error by mistake does not leak. Break: drop Format; red.
func TestAPepperNeverPrintsItsBytes(t *testing.T) {
	p := vectorPepper(t)
	key := p.Key(vectorClusterID())
	var logged bytes.Buffer
	slog.New(slog.NewTextHandler(&logged, nil)).Info("x", "pepper", p, "key", key)
	var out strings.Builder
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%T"} {
		fmt.Fprintf(&out, verb+" "+verb+"\n", p, key)
	}
	out.WriteString(logged.String())
	for _, secret := range [][]byte{p.Bytes(), key.bytes()} {
		for _, form := range SecretForms(secret) {
			if strings.Contains(out.String(), form) {
				t.Fatalf("a printed pepper or key holds %q:\n%s", form, out.String())
			}
		}
	}
	if !strings.Contains(out.String(), p.KID()) {
		t.Fatalf("a printed pepper does not name its kid:\n%s", out.String())
	}
}

func TestSecretFormsCoverHexBase64AndRaw(t *testing.T) {
	secret := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}
	forms := SecretForms(secret)
	for _, want := range []string{"deadbeef01", "DEADBEEF01", base64.StdEncoding.EncodeToString(secret), base64.RawURLEncoding.EncodeToString(secret), string(secret)} {
		found := false
		for _, form := range forms {
			found = found || form == want
		}
		if !found {
			t.Errorf("SecretForms misses %q", want)
		}
	}
}

func TestThePepperFileHasAMagicPrefix(t *testing.T) {
	p := vectorPepper(t)
	contents := EncodeFile(p)
	if !bytes.HasPrefix(contents, []byte(FileMagic)) || len(contents) != len(FileMagic)+Len {
		t.Fatalf("file is %q", contents)
	}
	back, err := DecodeFile(contents)
	if err != nil || !back.Equal(p) {
		t.Fatalf("decode: %v, %v", back, err)
	}
	for name, bad := range map[string][]byte{
		"raw 32 bytes":  p.Bytes(),
		"short":         contents[:len(contents)-1],
		"long":          append(append([]byte(nil), contents...), 0),
		"another magic": append([]byte("HDBPEP2\n"), p.Bytes()...),
	} {
		if _, err := DecodeFile(bad); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
}

func TestAPlainFileStoresLoadsAndMovesAside(t *testing.T) {
	dir := t.TempDir()
	file := File{Path: filepath.Join(dir, FileName)}
	if _, err := file.Load(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing file: %v, want fs.ErrNotExist", err)
	}
	p := vectorPepper(t)
	if err := file.Store(p); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(file.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v, want 0600", info.Mode().Perm())
	}
	got, err := file.Load()
	if err != nil || !got.Equal(p) {
		t.Fatalf("load: %v, %v", got, err)
	}
	aside, err := file.MoveAside(p.KID())
	if err != nil {
		t.Fatal(err)
	}
	if aside != file.Path+".wrong-"+p.KID() {
		t.Fatalf("moved to %s", aside)
	}
	if _, err := file.Load(); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("after the move: %v", err)
	}
	if kept, err := os.ReadFile(aside); err != nil || !bytes.Equal(kept, EncodeFile(p)) {
		t.Fatalf("the moved file is not kept whole: %v", err)
	}
}

// pepperHolder keeps a pepper and a key in unexported fields, where fmt cannot call their methods
// (reflect refuses Interface on an unexported field), and in exported ones.
type pepperHolder struct {
	p       Pepper
	k       Key
	pp      *Pepper
	Exposed Pepper
	Derived Key
}

// TestAPepperHeldInAnotherStructNeverPrintsItsBytes (review F4): a pepper or key that is a field
// of another value, unexported or exported, by value or behind a pointer, prints no byte form
// under any fmt verb, &holder, or slog's text and JSON handlers. Break: hold the bytes in the
// struct by value ([Len]byte); red.
func TestAPepperHeldInAnotherStructNeverPrintsItsBytes(t *testing.T) {
	p := vectorPepper(t)
	key := p.Key(vectorClusterID())
	holder := pepperHolder{p: p, k: key, pp: &p, Exposed: p, Derived: key}
	var out strings.Builder
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
		fmt.Fprintf(&out, verb+"\n", holder)
		fmt.Fprintf(&out, verb+"\n", &holder)
		fmt.Fprintf(&out, verb+"\n", []Pepper{p})
		fmt.Fprintf(&out, verb+"\n", map[string]Key{"k": key})
	}
	var text, js bytes.Buffer
	slog.New(slog.NewTextHandler(&text, nil)).Info("x", "holder", holder, "ptr", &holder)
	slog.New(slog.NewJSONHandler(&js, nil)).Info("x", "holder", holder, "ptr", &holder, "pepper", p)
	out.WriteString(text.String())
	out.WriteString(js.String())
	for _, secret := range [][]byte{p.Bytes(), key.bytes()} {
		for _, form := range SecretForms(secret) {
			if strings.Contains(out.String(), form) {
				t.Fatalf("a holder of a pepper printed %q:\n%s", form, out.String())
			}
		}
	}
}

func TestSecretFormsCoverQuotedAndSpacedHex(t *testing.T) {
	secret := []byte{0xde, 0xad, 0x22, 0x01}
	forms := SecretForms(secret)
	for _, want := range []string{fmt.Sprintf("%q", secret), fmt.Sprintf("% x", secret), fmt.Sprintf("%v", secret)} {
		found := false
		for _, form := range forms {
			found = found || strings.Contains(want, form) && len(form) >= len(want)-2
		}
		if !found {
			t.Errorf("SecretForms misses %q", want)
		}
	}
}

// TestMoveAsideOfAGoneFileSaysSo (review G1): moving aside a file that is no longer at its path
// (an operator moved it by hand) reports ErrSourceGone, not a plain failure, and picks no name.
func TestMoveAsideOfAGoneFileSaysSo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, FileName)
	if err := os.WriteFile(path+".wrong-0a1b2c3d", []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := MoveAside(path, "0a1b2c3d"); !errors.Is(err, ErrSourceGone) {
		t.Fatalf("moving aside a gone file: %v, want ErrSourceGone", err)
	}
	if _, err := os.Stat(path + ".wrong-0a1b2c3d.1"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a name was picked for a file that is gone: %v", err)
	}
}
