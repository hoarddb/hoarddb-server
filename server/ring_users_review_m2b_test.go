package server

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/transport"
)

// Review fixes of M2b (2026-11-20): F-1 and F-3, a token refresh re-checks the epoch and the
// incarnation on the record it mints from, not only on the one it verified first; F-2, a cluster
// token is bound to the cluster it was minted in.

// refreshAcrossAGap logs in as admin on a one-member cluster, then refreshes that token while
// change runs between the refresh's two reads of the record (tokenRefreshGapHook), and returns
// the refresh's answer.
func refreshAcrossAGap(t *testing.T, change func(record *UserRecord)) *transport.Frame {
	t.Helper()
	qs, _ := clusterUsersServer(t)
	am := qs.core.auth
	_, auth, _ := dialTCPServer(t, qs.tcpListener.Addr().String()).authCall(&AuthRequest{Username: "admin", Password: "test123"})
	if auth.Token == "" {
		t.Fatal("the login minted no token")
	}
	tokenRefreshGapHook = func() {
		am.mu.Lock()
		defer am.mu.Unlock()
		changed := *am.users["admin"]
		change(&changed)
		am.users["admin"] = &changed
	}
	t.Cleanup(func() { tokenRefreshGapHook = nil })
	frame, _, _ := dialTCPServer(t, qs.tcpListener.Addr().String()).authCall(&AuthRequest{Token: auth.Token})
	return frame
}

// TestATokenRefreshRechecksTheEpochAfterItsSecondRead (review F-1): a revocation (the epoch moves)
// that lands between the refresh's check and its read of the record ends the refresh; the token is
// never re-minted at the new epoch. Break: re-check only the incarnation after the second read; red.
func TestATokenRefreshRechecksTheEpochAfterItsSecondRead(t *testing.T) {
	frame := refreshAcrossAGap(t, func(record *UserRecord) { record.Epoch++ })
	if frame.Opcode == transport.OpAuthOK {
		t.Fatal("a token revoked between the refresh's two reads was re-minted at the new epoch")
	}
}

// TestATokenRefreshRechecksTheIncarnationAfterItsSecondRead (review F-3): an account dropped and
// created again between the two reads ends the refresh. Break: drop the second incarnation check;
// red.
func TestATokenRefreshRechecksTheIncarnationAfterItsSecondRead(t *testing.T) {
	frame := refreshAcrossAGap(t, func(record *UserRecord) { record.createdSeq += 100 })
	if frame.Opcode == transport.OpAuthOK {
		t.Fatal("a token of a dropped account resumed on its re-creation between the refresh's two reads")
	}
}

// TestATokenRefreshRechecksTheAccountIsEnabledAfterItsSecondRead (review F-1): a disable between
// the two reads ends the refresh too. Break: drop the enabled check after the second read; red.
func TestATokenRefreshRechecksTheAccountIsEnabledAfterItsSecondRead(t *testing.T) {
	frame := refreshAcrossAGap(t, func(record *UserRecord) { record.Enabled = false })
	if frame.Opcode == transport.OpAuthOK {
		t.Fatal("a token of an account disabled between the refresh's two reads was re-minted")
	}
}

// TestATokenMintedInOneClusterIsRefusedInAnother (review F-2): a cluster rebuilt with the same
// token key and the same first administrator reaches the same created_seq and epoch; its tokens
// carry the cluster id, so a token of the old cluster is refused on the new one. Break: compare no
// cluster id; red.
func TestATokenMintedInOneClusterIsRefusedInAnother(t *testing.T) {
	am, table, follower, key, kid, _ := followedManager(t)
	table.set(cluster.UserView{Users: []cluster.UserRow{tableRow("ada", "ada-password-1", key, kid, 4, 4)}, PepperKID: kid, StateSeq: 4})
	awaitInstalled(t, follower, 4)
	clusterA := [16]byte{0xA1}
	am.SetClusterPasswordKey(fakeClusterKey{key: key, kid: kid, clusterID: clusterA})
	claims, err := am.ClaimsFor("ada")
	if err != nil {
		t.Fatal(err)
	}
	if claims.ClusterID != hex.EncodeToString(clusterA[:]) {
		t.Fatalf("a cluster token names cluster %q, want %x", claims.ClusterID, clusterA)
	}
	token, err := am.GenerateToken(claims)
	if err != nil {
		t.Fatal(err)
	}
	presented, err := am.ValidateToken(token)
	if err != nil || am.VerifyClaims(presented) != nil {
		t.Fatalf("the token does not resume in its own cluster: %v", err)
	}
	am.SetClusterPasswordKey(fakeClusterKey{key: key, kid: kid, clusterID: [16]byte{0xB2}})
	err = am.VerifyClaims(presented)
	if !errors.Is(err, ErrTokenInvalid) || !strings.Contains(err.Error(), "another cluster") {
		t.Fatalf("cluster A's token in cluster B answered %v, want refused naming the other cluster", err)
	}
}
