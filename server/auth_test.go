package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTokenManager(t *testing.T, keyring *Keyring, epochs *EpochStore) *AuthManager {
	t.Helper()
	if keyring == nil {
		var err error
		keyring, err = BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
		if err != nil {
			t.Fatalf("BuildKeyring: %v", err)
		}
	}
	return NewAuthManager(AuthTokenOptions{
		TTL:         15 * time.Minute,
		MaxLifetime: 12 * time.Hour,
		Skew:        60 * time.Second,
		Keyring:     keyring,
		Epochs:      epochs,
	}, discardLogger())
}

func seedUser(t *testing.T, am *AuthManager, username string) {
	t.Helper()
	if err := am.SeedRootUser(username, "correct-horse-battery"); err != nil {
		t.Fatalf("seed %s: %v", username, err)
	}
}

func mintToken(t *testing.T, am *AuthManager, username string) (string, TokenClaims) {
	t.Helper()
	claims, err := am.ClaimsFor(username)
	if err != nil {
		t.Fatalf("ClaimsFor(%s): %v", username, err)
	}
	token, err := am.GenerateToken(claims)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	return token, claims
}

// signRawClaims signs already-marshalled (possibly malformed) claims bytes
// with the current key, so a test can present a valid signature over invalid
// JSON.
func signRawClaims(t *testing.T, am *AuthManager, claimsJSON []byte) string {
	t.Helper()
	key := am.keyring.Current()
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)
	mac := hmac.New(sha256.New, key.Bytes)
	mac.Write([]byte(claimsB64))
	return claimsB64 + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestTokenRoundTripClaimsEqualRecord(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")

	token, minted := mintToken(t, am, "alice")
	got, err := am.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if got.Subject != "alice" {
		t.Fatalf("subject = %q, want alice", got.Subject)
	}
	if strings.Join(got.Roles, ",") != strings.Join(minted.Roles, ",") {
		t.Fatalf("roles = %v, want %v", got.Roles, minted.Roles)
	}
	if strings.Join(got.Databases, ",") != strings.Join(minted.Databases, ",") {
		t.Fatalf("databases = %v, want %v", got.Databases, minted.Databases)
	}
	// Acceptance criterion 4: claims for the root user equal its record.
	if strings.Join(got.Roles, ",") != "root" || strings.Join(got.Databases, ",") != "*" {
		t.Fatalf("claims %v/%v do not match the seeded record root/*", got.Roles, got.Databases)
	}
	if got.KeyID != am.keyring.Current().ID {
		t.Fatalf("kid = %q, want current %q", got.KeyID, am.keyring.Current().ID)
	}
	if err := am.VerifyClaims(got); err != nil {
		t.Fatalf("VerifyClaims: %v", err)
	}
}

func TestTokenTamperingIsInvalid(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	token, _ := mintToken(t, am, "alice")

	claimsB64, sigB64, _ := strings.Cut(token, ".")

	payloadTampered := flipFirstChar(claimsB64) + "." + sigB64
	if _, err := am.ValidateToken(payloadTampered); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("tampered payload error = %v, want ErrTokenInvalid", err)
	}

	sigTampered := claimsB64 + "." + flipFirstChar(sigB64)
	if _, err := am.ValidateToken(sigTampered); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("tampered signature error = %v, want ErrTokenInvalid", err)
	}
}

// flipFirstChar changes the first base64 character of a segment, which
// always changes the decoded bytes. Flipping the last character can leave the
// decoded bytes identical because RawURLEncoding ignores the low bits of the
// final character, so it is not a reliable tamper.
func flipFirstChar(s string) string {
	if s == "" {
		return "x"
	}
	first := s[0]
	if first == 'A' {
		first = 'B'
	} else {
		first = 'A'
	}
	return string(first) + s[1:]
}

func TestTokenExpiryAndSkew(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	now := time.Now()

	cases := []struct {
		name    string
		exp     int64
		iat     int64
		wantErr error
	}{
		{"exp within skew accepted", now.Add(-30 * time.Second).Unix(), now.Unix(), nil},
		{"exp past skew expired", now.Add(-90 * time.Second).Unix(), now.Unix(), ErrTokenExpired},
		{"iat within skew accepted", now.Add(time.Hour).Unix(), now.Add(30 * time.Second).Unix(), nil},
		{"iat past skew invalid", now.Add(time.Hour).Unix(), now.Add(90 * time.Second).Unix(), ErrTokenInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token, err := am.GenerateToken(TokenClaims{
				Subject:   "alice",
				Roles:     []string{"admin"},
				Databases: []string{"*"},
				IssuedAt:  tc.iat,
				ExpiresAt: tc.exp,
				Deadline:  now.Add(24 * time.Hour).Unix(),
			})
			if err != nil {
				t.Fatalf("GenerateToken: %v", err)
			}
			_, err = am.ValidateToken(token)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("ValidateToken = %v, want accepted", err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ValidateToken error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// validateTokenAsOf validates token at the instant at. ValidateToken reads the
// clock through tokenNowHook, so an expiry boundary is reachable in a test
// without waiting for it.
func validateTokenAsOf(t *testing.T, am *AuthManager, token string, at time.Time) error {
	t.Helper()
	tokenNowHook = func() time.Time { return at }
	defer func() { tokenNowHook = nil }()
	_, err := am.ValidateToken(token)
	return err
}

// TestATokenLivesItsFullTTLWhateverTheSubsecond pins exp to the ceiling of
// now+ttl. A mint at .9 s with a 1 s TTL truncates to the next whole second,
// 100 ms away, so the fraction of the second must be carried rather than
// dropped: the token is valid for the full TTL it was granted (BUG-0633).
func TestATokenLivesItsFullTTLWhateverTheSubsecond(t *testing.T) {
	keyring, err := BuildKeyring(t.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL:         time.Second,
		MaxLifetime: time.Hour,
		Skew:        0,
		Keyring:     keyring,
	}, discardLogger())
	seedUser(t, am, "alice")
	record, err := am.recordFor("alice")
	if err != nil {
		t.Fatalf("recordFor: %v", err)
	}

	for _, fraction := range []time.Duration{0, 100 * time.Millisecond, 900 * time.Millisecond, 999 * time.Millisecond} {
		mintedAt := time.Unix(1000, 0).Add(fraction)
		claims := am.MintClaims(record, nil, mintedAt)
		token, err := am.GenerateToken(claims)
		if err != nil {
			t.Fatalf("GenerateToken at %s: %v", mintedAt, err)
		}

		lastValid := mintedAt.Add(am.tokenExp - time.Millisecond)
		if err := validateTokenAsOf(t, am, token, lastValid); err != nil {
			t.Fatalf("a token minted at %s with a %s TTL was refused %s after the mint (exp %d, %d s of life): %v; want it to live at least its TTL",
				mintedAt, am.tokenExp, lastValid.Sub(mintedAt), claims.ExpiresAt, claims.ExpiresAt-mintedAt.Unix(), err)
		}

		afterTTL := mintedAt.Add(am.tokenExp + time.Second)
		if err := validateTokenAsOf(t, am, token, afterTTL); !errors.Is(err, ErrTokenExpired) {
			t.Fatalf("a token minted at %s with a %s TTL at %s (exp %d) = %v, want ErrTokenExpired",
				mintedAt, am.tokenExp, afterTTL.Sub(mintedAt), claims.ExpiresAt, err)
		}
	}
}

func TestTokenFromAnotherKeyIsInvalid(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	token, _ := mintToken(t, am, "alice")

	// A second manager derived from a different cluster token has a different
	// key; the token must not validate.
	otherKeyring, err := BuildKeyring(t.TempDir(), "a-different-cluster-token", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	other := newTokenManager(t, otherKeyring, nil)
	if _, err := other.ValidateToken(token); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("foreign-key token error = %v, want ErrTokenInvalid", err)
	}
}

func TestClusterDerivedKeysValidateAcrossNodes(t *testing.T) {
	keyA, err := BuildKeyring(t.TempDir(), "shared-cluster-token", "prod", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring A: %v", err)
	}
	keyB, err := BuildKeyring(t.TempDir(), "shared-cluster-token", "prod", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring B: %v", err)
	}
	amA := newTokenManager(t, keyA, nil)
	amB := newTokenManager(t, keyB, nil)
	seedUser(t, amA, "alice")

	token, _ := mintToken(t, amA, "alice")
	if _, err := amB.ValidateToken(token); err != nil {
		t.Fatalf("node B rejected node A's token: %v", err)
	}
}

func TestEpochBumpInvalidatesClaims(t *testing.T) {
	epochs, err := LoadEpochStore(t.TempDir())
	if err != nil {
		t.Fatalf("LoadEpochStore: %v", err)
	}
	am := newTokenManager(t, nil, epochs)
	seedUser(t, am, "alice")
	token, _ := mintToken(t, am, "alice")

	if err := am.InvalidateUserTokens("alice"); err != nil {
		t.Fatalf("InvalidateUserTokens: %v", err)
	}
	// The token is still cryptographically valid (pure validation).
	claims, err := am.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken after epoch bump: %v", err)
	}
	if err := am.VerifyClaims(claims); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("VerifyClaims after epoch bump = %v, want ErrTokenInvalid", err)
	}
}

func TestRefreshKeepsAbsoluteDeadline(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	_, presented := mintToken(t, am, "alice")

	record, err := am.recordFor("alice")
	if err != nil {
		t.Fatalf("recordFor: %v", err)
	}
	refreshed := am.MintClaims(record, &presented, time.Now().Add(time.Minute))
	if refreshed.Deadline != presented.Deadline {
		t.Fatalf("refresh changed abs: %d -> %d", presented.Deadline, refreshed.Deadline)
	}
	if refreshed.IssuedAt <= presented.IssuedAt {
		t.Fatalf("refresh did not advance iat: %d -> %d", presented.IssuedAt, refreshed.IssuedAt)
	}
}

func TestChainStartJittersDeadline(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	_, claims := mintToken(t, am, "alice")

	want := time.Now().Add(am.maxLifetime).Unix()
	jitter := int64(am.maxLifetime/time.Second) / 10
	if claims.Deadline < want-jitter || claims.Deadline > want+jitter+1 {
		t.Fatalf("fresh deadline %d outside ±10%% of %d", claims.Deadline, want)
	}
}

func TestSizeCapRejectsBeforeDecode(t *testing.T) {
	am := newTokenManager(t, nil, nil)

	var decodes int
	tokenDecodeHook = func() { decodes++ }
	defer func() { tokenDecodeHook = nil }()

	oversized := strings.Repeat("a", maxTokenBytes+1)
	if _, err := am.ValidateToken(oversized); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("oversized token error = %v, want ErrTokenInvalid", err)
	}
	if decodes != 0 {
		t.Fatalf("JSON decoder reached %d times for an oversized token, want 0", decodes)
	}

	// A three-segment token is also rejected before the decoder.
	if _, err := am.ValidateToken("aaaa.bbbb.cccc"); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("three-segment token error = %v, want ErrTokenInvalid", err)
	}
	if decodes != 0 {
		t.Fatalf("JSON decoder reached %d times for a three-segment token, want 0", decodes)
	}
}

func TestSegmentShapesAreInvalid(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")
	valid, _ := mintToken(t, am, "alice")
	_, sigB64, _ := strings.Cut(valid, ".")

	bad := []string{
		"a",
		"a.b.c",
		"a.",
		".b",
		"AAAA==." + sigB64, // padded standard base64 variant
	}
	for _, token := range bad {
		if _, err := am.ValidateToken(token); !errors.Is(err, ErrTokenInvalid) {
			t.Fatalf("token %q error = %v, want ErrTokenInvalid", token, err)
		}
	}
}

func TestMalformedClaimsWithValidSignatureIsInvalid(t *testing.T) {
	am := newTokenManager(t, nil, nil)

	var decodes int
	tokenDecodeHook = func() { decodes++ }
	defer func() { tokenDecodeHook = nil }()

	token := signRawClaims(t, am, []byte(`{"sub":"alice","roles":`))
	_, err := am.ValidateToken(token)
	if !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("malformed-claims error = %v, want ErrTokenInvalid", err)
	}
	// The JSON decoder must have run (kid lives in the claims), but no claim
	// content may leak into the error.
	if decodes != 1 {
		t.Fatalf("decoder runs = %d, want 1", decodes)
	}
	if strings.Contains(err.Error(), "alice") {
		t.Fatalf("error leaked claim content: %q", err)
	}
}

func TestKeyIDSelection(t *testing.T) {
	previous := newSigningKey([]byte("previous-key-material-32-bytes!"))
	current := newSigningKey([]byte("current-key-material-32-bytes!!"))
	now := time.Now()

	makeRing := func(window time.Duration) *Keyring {
		return &Keyring{
			window: window,
			entries: []keyEntry{
				{Key: previous, AddedAt: now},
				{Key: current, AddedAt: now},
			},
		}
	}

	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: time.Hour, Skew: time.Minute,
		Keyring: makeRing(time.Hour),
	}, discardLogger())
	seedUser(t, am, "alice")

	// Sign directly with the previous key: inside the overlap window it must
	// validate.
	claims := TokenClaims{
		Subject: "alice", Roles: []string{"admin"}, Databases: []string{"*"},
		IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
		Deadline: now.Add(2 * time.Hour).Unix(), KeyID: previous.ID,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	previousToken := signWithKey(t, previous, payload)
	if _, err := am.ValidateToken(previousToken); err != nil {
		t.Fatalf("previous kid inside window rejected: %v", err)
	}

	// Retire it: window 0 means no overlap.
	retired := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: time.Hour, Skew: time.Minute,
		Keyring: makeRing(0),
	}, discardLogger())
	if _, err := retired.ValidateToken(previousToken); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("previous kid after retirement = %v, want ErrTokenInvalid", err)
	}

	// Unknown kid, signed by a real ring key: must miss, not fall back.
	unknown := TokenClaims{
		Subject: "alice", Roles: []string{"admin"}, Databases: []string{"*"},
		IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
		Deadline: now.Add(2 * time.Hour).Unix(), KeyID: "deadbeef",
	}
	payload, _ = json.Marshal(unknown)
	unknownToken := signWithKey(t, current, payload)
	if _, err := am.ValidateToken(unknownToken); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("unknown kid = %v, want ErrTokenInvalid", err)
	}
}

// signWithKey produces the wire format with an explicit key, for previous-kid
// tests.
func signWithKey(t *testing.T, key SigningKey, claimsJSON []byte) string {
	t.Helper()
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)
	mac := hmac.New(sha256.New, key.Bytes)
	mac.Write([]byte(claimsB64))
	return claimsB64 + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestConcurrentValidateAndInvalidate(t *testing.T) {
	epochs, err := LoadEpochStore(t.TempDir())
	if err != nil {
		t.Fatalf("LoadEpochStore: %v", err)
	}
	am := newTokenManager(t, nil, epochs)
	seedUser(t, am, "alice")
	token, _ := mintToken(t, am, "alice")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				claims, err := am.ValidateToken(token)
				if err != nil {
					continue
				}
				// Either accepted (before the bump) or refused; both are
				// correct, and neither may race with the write.
				_ = am.VerifyClaims(claims)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			if err := am.InvalidateUserTokens("alice"); err != nil {
				t.Errorf("InvalidateUserTokens: %v", err)
				return
			}
		}
	}()
	wg.Wait()

	// After the dust settles the original epoch is certainly revoked.
	claims, err := am.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if err := am.VerifyClaims(claims); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("VerifyClaims after concurrent bumps = %v, want ErrTokenInvalid", err)
	}
}

func TestRefreshRevalidatesAgainstNarrowedRecord(t *testing.T) {
	am := newTokenManager(t, nil, nil)
	seedUser(t, am, "alice")

	// Seed a record with two roles, mint, then narrow the record.
	am.mu.Lock()
	am.users["alice"].Roles = []Grant{{Role: "admin", DB: "*"}, {Role: "user", DB: "app"}}
	am.mu.Unlock()
	_, presented := mintToken(t, am, "alice")

	// Narrow the record and bump its epoch, as a record mutation would.
	am.mu.Lock()
	am.users["alice"].Roles = []Grant{{Role: "user", DB: "app"}}
	am.users["alice"].Epoch++
	am.mu.Unlock()

	record, err := am.recordFor("alice")
	if err != nil {
		t.Fatalf("recordFor: %v", err)
	}
	refreshed := am.MintClaims(record, &presented, time.Now().Add(time.Minute))
	if strings.Join(refreshed.Roles, ",") != "user" {
		t.Fatalf("refreshed roles = %v, want the intersection [user]", refreshed.Roles)
	}
	if strings.Join(refreshed.Databases, ",") != "app" {
		t.Fatalf("refreshed databases = %v, want the intersection [app]", refreshed.Databases)
	}
	if err := am.VerifyClaims(presented); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("presented epoch after bump = %v, want ErrTokenInvalid", err)
	}
}

func TestEpochPersistenceAcrossManagerRestart(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, UsersFileName)
	keyring, err := BuildKeyring(dir, "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}

	options := AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: 12 * time.Hour, Skew: time.Minute,
		Keyring: keyring, UsersPath: userPath, Master: "node-a",
	}
	am1 := NewAuthManager(options, discardLogger())
	if err := am1.SeedRootUser("alice", "correct-horse-battery"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	token, _ := mintToken(t, am1, "alice")
	if err := am1.InvalidateUserTokens("alice"); err != nil {
		t.Fatalf("InvalidateUserTokens: %v", err)
	}

	// Restart: a fresh manager reloads users.json. The epoch bump must have
	// survived on disk, so the pre-bump token is refused.
	am2 := NewAuthManager(options, discardLogger())
	present, folded, err := am2.LoadUsers(userPath, nil, nil)
	if err != nil || !present {
		t.Fatalf("LoadUsers after restart = %v, %v, want present", present, err)
	}
	if folded != 0 {
		t.Fatalf("folded = %d with no side file, want 0", folded)
	}
	claims, err := am2.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken after restart: %v", err)
	}
	if err := am2.VerifyClaims(claims); !errors.Is(err, ErrTokenInvalid) {
		t.Fatalf("revoked token after restart = %v, want ErrTokenInvalid", err)
	}
}

// TestLoadUsersFoldsEpochsWithMax is the §6.4 fold: the record's epoch is
// max(record, side file), the fold is idempotent, and a counter for an absent
// user is dropped rather than carried.
func TestLoadUsersFoldsEpochsWithMax(t *testing.T) {
	dir := t.TempDir()
	userPath := filepath.Join(dir, UsersFileName)

	keyring, err := BuildKeyring(dir, "", "", time.Hour, discardLogger())
	if err != nil {
		t.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: 12 * time.Hour, Skew: time.Minute,
		Keyring: keyring, UsersPath: userPath, Master: "node-a",
	}, discardLogger())
	if err := am.SeedRootUser("alice", "correct-horse-battery"); err != nil {
		t.Fatalf("SeedRootUser: %v", err)
	}
	// A pre-user-store node wrote higher counters for alice and a user that
	// no longer exists.
	if err := os.WriteFile(filepath.Join(dir, epochsFile), []byte(`{"alice":9,"ghost":4}`), 0600); err != nil {
		t.Fatalf("write epochs: %v", err)
	}
	epochs, err := LoadEpochStore(dir)
	if err != nil {
		t.Fatalf("LoadEpochStore: %v", err)
	}

	present, folded, err := am.LoadUsers(userPath, nil, epochs)
	if err != nil || !present {
		t.Fatalf("LoadUsers = %v, %v, want present", present, err)
	}
	if folded != 1 {
		t.Fatalf("folded = %d, want 1 (ghost has no record and is dropped)", folded)
	}
	am.mu.RLock()
	got := am.users["alice"].Epoch
	_, ghost := am.users["ghost"]
	am.mu.RUnlock()
	if got != 9 {
		t.Fatalf("alice epoch after fold = %d, want 9", got)
	}
	if ghost {
		t.Fatal("a counter with no matching user must be dropped, not carried")
	}

	// A second load with a lower side counter must not lower the record.
	if err := os.WriteFile(filepath.Join(dir, epochsFile), []byte(`{"alice":3}`), 0600); err != nil {
		t.Fatalf("rewrite epochs: %v", err)
	}
	lower, err := LoadEpochStore(dir)
	if err != nil {
		t.Fatalf("LoadEpochStore lower: %v", err)
	}
	if _, _, err := am.LoadUsers(userPath, nil, lower); err != nil {
		t.Fatalf("second LoadUsers: %v", err)
	}
	// users.json still holds the seeded epoch, so the fold re-reads it; the
	// assertion is that the fold never lowers below the record.
	am.mu.RLock()
	got = am.users["alice"].Epoch
	am.mu.RUnlock()
	if got < 1 {
		t.Fatalf("alice epoch after lower side counter = %d, want the record's value or higher", got)
	}
}
