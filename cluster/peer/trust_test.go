package peer

import (
	"crypto/tls"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/transport"
)

// trustServer serves TLS on loopback with the key made in a fresh directory and returns its
// address and fingerprint. Its listener refuses nothing: the test is of the dialer.
func trustServer(t *testing.T) (addr, fingerprint string, serverCfg *tls.Config) {
	t.Helper()
	cfg, fp, err := transport.MakeServerTLSConfig(filepath.Join(t.TempDir(), "keys"))
	if err != nil {
		t.Fatal(err)
	}
	cfg = cfg.Clone()
	cfg.ClientAuth = tls.NoClientCert
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsLn := tls.NewListener(ln, cfg)
	t.Cleanup(func() { _ = tlsLn.Close() })
	go func() {
		for {
			conn, err := tlsLn.Accept()
			if err != nil {
				return
			}
			go func() {
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				_, _ = conn.Read(make([]byte, 1))
				_ = conn.Close()
			}()
		}
	}()
	return ln.Addr().String(), fp, cfg
}

func trustDial(t *testing.T, conf *tls.Config, addr string) error {
	t.Helper()
	raw, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	return tls.Client(raw, conf).Handshake()
}

// TestClientConfigForHoldsADialToTheMembersOwnKey: [GUARD a dial to member B accepts B's pinned
// key and refuses another member's pinned key presented at B's address; without BindIDs, or with
// no id, any pinned key is accepted as before].
func TestClientConfigForHoldsADialToTheMembersOwnKey(t *testing.T) {
	addrB, fpB, _ := trustServer(t)
	addrC, fpC, _ := trustServer(t)
	own, _, err := transport.MakeServerTLSConfig(filepath.Join(t.TempDir(), "keys"))
	if err != nil {
		t.Fatal(err)
	}
	trust, err := NewTrust(own, []string{fpB, fpC})
	if err != nil {
		t.Fatal(err)
	}
	pins := map[string]string{"B": fpB, "C": fpC}
	trust.BindIDs(func(id string) (string, bool) { fp, ok := pins[id]; return fp, ok })

	conf, err := trust.ClientConfigFor("B", addrB)
	if err != nil {
		t.Fatal(err)
	}
	if err := trustDial(t, conf, addrB); err != nil {
		t.Fatalf("a dial to B at B's address was refused: %v", err)
	}
	// C's pinned key answers at "B's" address: the redirect the pin map exists to stop.
	conf, err = trust.ClientConfigFor("B", addrC)
	if err != nil {
		t.Fatal(err)
	}
	if err := trustDial(t, conf, addrC); err == nil {
		t.Fatal("a dial to B accepted C's pinned key")
	}
	if _, err := trust.ClientConfigFor("D", addrB); err == nil {
		t.Fatal("a dial to a member with no pin was given a config")
	}
	// An unnamed dial is refused once ids are bound. [BREAK: fall through to
	// t.ClientConfig(addr) for an empty id].
	if _, err := trust.ClientConfigFor("", addrC); err == nil {
		t.Fatal("a dial with no id was given a config that accepts any pinned key")
	}
	unbound, err := NewTrust(own, []string{fpB, fpC})
	if err != nil {
		t.Fatal(err)
	}
	conf, err = unbound.ClientConfigFor("", addrC)
	if err != nil {
		t.Fatal(err)
	}
	if err := trustDial(t, conf, addrC); err != nil {
		t.Fatalf("without BindIDs a dial with no id, held to the pinned set, refused a pinned key: %v", err)
	}
}
