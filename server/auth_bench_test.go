package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/transport"
)

// benchAuthManager builds a manager with a real temp-file keyring and one
// seeded user.
func benchAuthManager(tb testing.TB) (*AuthManager, *Server) {
	tb.Helper()
	keyring, err := BuildKeyring(tb.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		tb.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: 15 * time.Minute, MaxLifetime: 12 * time.Hour, Skew: 60 * time.Second,
		Keyring: keyring,
	}, discardLogger())
	if err := am.SeedRootUser("admin", "bench-password"); err != nil {
		tb.Fatalf("SeedRootUser: %v", err)
	}
	return am, &Server{logger: discardLogger()}
}

// BenchmarkAuthPassword measures the Argon2id password path through
// handleAuth. Expect tens of milliseconds on a normal host (spec Appendix A).
func BenchmarkAuthPassword(b *testing.B) {
	am, s := benchAuthManager(b)
	payload, _ := bson.Marshal(&AuthRequest{Username: "admin", Password: "bench-password"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess := &transport.Session{}
		ctx := transport.WithSession(context.Background(), sess)
		resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, s, discardLogger())
		if err != nil || resp.Opcode != transport.OpAuthOK {
			b.Fatalf("password AUTH = %v err=%v", resp.Opcode, err)
		}
	}
}

// BenchmarkAuthToken measures the whole handleAuth token path — size check,
// segment split, HMAC, base64/JSON decode, epoch lookup and a fresh mint —
// not ValidateToken in isolation, because that is what runs once per socket.
func BenchmarkAuthToken(b *testing.B) {
	am, s := benchAuthManager(b)
	claims, err := am.ClaimsFor("admin")
	if err != nil {
		b.Fatalf("ClaimsFor: %v", err)
	}
	token, err := am.GenerateToken(claims)
	if err != nil {
		b.Fatalf("GenerateToken: %v", err)
	}
	payload, _ := bson.Marshal(&AuthRequest{Token: token})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess := &transport.Session{}
		ctx := transport.WithSession(context.Background(), sess)
		resp, err := handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, s, discardLogger())
		if err != nil || resp.Opcode != transport.OpAuthOK {
			b.Fatalf("token AUTH = %v err=%v", resp.Opcode, err)
		}
	}
}

// BenchmarkAuthDerivationGate runs auth_max_derivations+1 concurrent AUTHs per
// iteration and asserts the instrumented peak never exceeds the bound. Argon2
// cost is minimized so the gate, not the hash, dominates.
func BenchmarkAuthDerivationGate(b *testing.B) {
	const maxDerivations = 4
	keyring, err := BuildKeyring(b.TempDir(), "", "", time.Hour, discardLogger())
	if err != nil {
		b.Fatalf("BuildKeyring: %v", err)
	}
	am := NewAuthManager(AuthTokenOptions{
		TTL: time.Hour, MaxLifetime: time.Hour, Skew: time.Minute,
		Keyring: keyring, MaxDerivations: maxDerivations, DerivationTimeout: 5 * time.Second,
		Argon2: Argon2Params{Memory: 8, Iterations: 1, Parallelism: 1, SaltLen: 16, HashLen: 32},
	}, discardLogger())
	if err := am.SeedRootUser("admin", "bench-password"); err != nil {
		b.Fatalf("SeedRootUser: %v", err)
	}
	s := &Server{logger: discardLogger()}

	var active, peak atomic.Int64
	verifyUserHook = func() {
		v := active.Add(1)
		for {
			m := peak.Load()
			if v <= m || peak.CompareAndSwap(m, v) {
				break
			}
		}
		active.Add(-1)
	}
	defer func() { verifyUserHook = nil }()

	payload, _ := bson.Marshal(&AuthRequest{Username: "admin", Password: "bench-password"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < maxDerivations+1; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sess := &transport.Session{}
				ctx := transport.WithSession(context.Background(), sess)
				_, _ = handleAuth(ctx, transport.NewFrame(transport.OpAuth, payload), am, s, discardLogger())
			}()
		}
		wg.Wait()
	}
	b.StopTimer()
	if got := peak.Load(); got > maxDerivations {
		b.Fatalf("peak concurrent derivations = %d, want <= %d", got, maxDerivations)
	}
}
