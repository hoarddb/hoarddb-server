package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// The root credential every capture server starts with. The vectors carry it
// (a replaying harness starts its servers with the same two variables), so it
// is a test fixture, never a secret.
const (
	rootUser     = "admin"
	rootPassword = "vectors-root-password"
)

// nodeKey is a node identity made from a fixed seed, so every capture's
// fingerprints are the same and need no normalising.
type nodeKey struct {
	private ed25519.PrivateKey
	public  ed25519.PublicKey
	certPEM []byte
	keyPEM  []byte
}

// zeroReader stands in for randomness where the standard library asks for it
// but an ed25519 certificate does not need it (the signature is
// deterministic), so a certificate made from a fixed seed is fixed bytes.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

// newNodeKey derives an ed25519 identity and a self-signed certificate from
// seedByte, in the shape the server writes under <data dir>/keys/
// (transport.GenerateEd25519Keys): PKCS#8 PEM and an X.509 PEM.
func newNodeKey(seedByte byte) (nodeKey, error) {
	seed := make([]byte, ed25519.SeedSize)
	for index := range seed {
		seed[index] = seedByte
	}
	private := ed25519.NewKeyFromSeed(seed)
	public := private.Public().(ed25519.PublicKey)
	notBefore := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(int64(seedByte) + 1),
		Subject:      pkix.Name{Organization: []string{"hoardDB"}, CommonName: "hoarddb-node"},
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(100 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(zeroReader{}, template, template, public, private)
	if err != nil {
		return nodeKey{}, fmt.Errorf("creating the certificate for seed %d: %w", seedByte, err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nodeKey{}, fmt.Errorf("encoding the key for seed %d: %w", seedByte, err)
	}
	return nodeKey{
		private: private,
		public:  public,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		keyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}),
	}, nil
}

// fingerprint is the server's own fingerprint of the key.
func (k nodeKey) fingerprint() string { return transport.Fingerprint(k.public) }

// install writes the identity where the server looks for it, before its
// first start, so the server loads it instead of minting a random one.
func (k nodeKey) install(dataDir string) error {
	keyDir := filepath.Join(dataDir, "keys")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		return fmt.Errorf("creating %s: %w", keyDir, err)
	}
	if err := os.WriteFile(filepath.Join(keyDir, "server.key"), k.keyPEM, 0o600); err != nil {
		return fmt.Errorf("writing the node key: %w", err)
	}
	if err := os.WriteFile(filepath.Join(keyDir, "server.crt"), k.certPEM, 0o644); err != nil {
		return fmt.Errorf("writing the node certificate: %w", err)
	}
	return nil
}

// freePort picks a loopback port for a server to bind later. It is drawn from
// below the kernel's ephemeral range: the OS hands ephemeral ports to every
// outgoing connection (the nodes dialling each other, other tests' Listen on
// port 0), so a port taken from that range and released can be reused as a
// source port before the server binds it, and node3's ports sit unbound for
// the whole of a stop and restart (BUG-0487). Ports outside the range are only
// ever taken by a process that binds them on purpose.
func freePort() (int, error) {
	return pickPort(1)
}

// freeRingPort finds a port whose successor is also free: the internode
// listener is always the ring port plus one (handbook testing.md 3.2).
func freeRingPort() (int, error) {
	return pickPort(2)
}

// pickedPorts holds every port this process has handed out, so two nodes of
// one capture never get the same one or a neighbour of one.
var pickedPorts = struct {
	sync.Mutex
	taken map[int]bool
}{taken: make(map[int]bool)}

const (
	lowestPickedPort = 10240
	pickAttempts     = 200
)

// ephemeralPortStart is the first port the kernel hands out to outgoing
// connections; 32768 is the Linux default when the setting cannot be read.
func ephemeralPortStart() int {
	contents, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 32768
	}
	fields := strings.Fields(string(contents))
	if len(fields) == 0 {
		return 32768
	}
	start, err := strconv.Atoi(fields[0])
	if err != nil {
		return 32768
	}
	return start
}

// pickPort returns the first of span consecutive ports that are free now, below
// the ephemeral range, and not handed out before.
func pickPort(span int) (int, error) {
	ceiling := ephemeralPortStart()
	if ceiling-span <= lowestPickedPort {
		return 0, fmt.Errorf("the ephemeral port range starts at %d, leaving no room below it for the capture servers", ceiling)
	}
	pickedPorts.Lock()
	defer pickedPorts.Unlock()
	for attempt := 0; attempt < pickAttempts; attempt++ {
		port := lowestPickedPort + rand.IntN(ceiling-span-lowestPickedPort)
		if portsFree(port, span) {
			for offset := 0; offset < span; offset++ {
				pickedPorts.taken[port+offset] = true
			}
			return port, nil
		}
	}
	return 0, fmt.Errorf("no run of %d free loopback ports in %d-%d after %d attempts", span, lowestPickedPort, ceiling-1, pickAttempts)
}

// portsFree reports whether span consecutive loopback ports from port can be
// bound now and were not handed out before.
func portsFree(port, span int) bool {
	for offset := 0; offset < span; offset++ {
		if pickedPorts.taken[port+offset] {
			return false
		}
		listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port+offset))
		if err != nil {
			return false
		}
		if err := listener.Close(); err != nil {
			return false
		}
	}
	return true
}

// endpoint is a server a capture talks to: where its driver listener is and
// the key it must present.
type endpoint struct {
	name        string
	driverAddr  string
	fingerprint string
}

// profile is an in-process standalone server's settings beyond the defaults.
// env is how a replaying harness reproduces them, and apply is the same
// setting made on the config directly.
type profile struct {
	name  string
	env   map[string]string
	apply func(*config.Config)
}

// inProcessServer is one standalone server.Service in this process.
type inProcessServer struct {
	endpoint
	service *server.Service
	dataDir string
	logFile *os.File
	logger  *slog.Logger
}

// startInProcess starts a standalone server on loopback ports in a fresh
// data directory, with the fixed root credential and a fixed identity.
func startInProcess(ctx context.Context, workDir string, p profile, key nodeKey, ports *portBook) (*inProcessServer, error) {
	dataDir, err := os.MkdirTemp(workDir, "single-"+p.name+"-")
	if err != nil {
		return nil, fmt.Errorf("creating the %s data directory: %w", p.name, err)
	}
	if err := key.install(dataDir); err != nil {
		return nil, err
	}
	logFile, err := os.Create(dataDir + ".log")
	if err != nil {
		return nil, fmt.Errorf("creating the %s log: %w", p.name, err)
	}
	driverPort, err := freePort()
	if err != nil {
		return nil, err
	}
	ringPort, err := freeRingPort()
	if err != nil {
		return nil, err
	}
	ports.record(driverPort, 7433)
	ports.record(ringPort, 4433)

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Server.NodeID = "vectors-node"
	cfg.Server.Listen = "127.0.0.1:" + strconv.Itoa(ringPort)
	cfg.TCP.Listen = "127.0.0.1:" + strconv.Itoa(driverPort)
	cfg.TCP.InternodeListen = "127.0.0.1:" + strconv.Itoa(ringPort+1)
	cfg.Metrics.Enabled = false
	cfg.Audit.Enabled = false
	cfg.Logging.Output = logFile.Name()
	if p.apply != nil {
		p.apply(cfg)
	}
	logger := slog.New(slog.NewJSONHandler(logFile, &slog.HandlerOptions{Level: slog.LevelInfo}))
	service, err := server.NewService(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("creating the %s server: %w", p.name, err)
	}
	if err := service.BootstrapUsers(rootUser, rootPassword, true); err != nil {
		return nil, fmt.Errorf("bootstrapping the %s root user: %w", p.name, err)
	}
	if err := service.Start(); err != nil {
		return nil, fmt.Errorf("starting the %s server: %w", p.name, err)
	}
	started := &inProcessServer{
		endpoint: endpoint{name: p.name, driverAddr: cfg.TCP.Listen, fingerprint: key.fingerprint()},
		service:  service,
		dataDir:  dataDir,
		logFile:  logFile,
		logger:   logger,
	}
	if err := waitForPing(ctx, started.endpoint); err != nil {
		started.stop()
		return nil, err
	}
	return started, nil
}

// stop stops the server and closes its log. The data directory goes with the
// generation's work directory.
func (s *inProcessServer) stop() {
	if err := s.service.Stop(); err != nil {
		s.logger.Error("stopping the capture server", "error", err)
	}
	if err := s.logFile.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "closing the %s server log: %v\n", s.name, err)
	}
}

// waitForPing dials until the server answers PING (served before AUTH).
func waitForPing(ctx context.Context, target endpoint) error {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := dial(ctx, target)
		if err == nil {
			_, err = conn.roundTrip(transport.OpPing, fixedRequestID(0), nil)
			conn.close()
			if err == nil {
				return nil
			}
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("server %s at %s did not answer PING within 30 s: %w", target.name, target.driverAddr, lastErr)
}

// portBook maps every port a run happened to get to a fixed one, so a
// message that names an address reads the same in every run.
type portBook struct {
	actual map[int]int
}

func (b *portBook) record(actual, canonical int) {
	if b.actual == nil {
		b.actual = make(map[int]int)
	}
	b.actual[actual] = canonical
}

// wireConn is one TLS connection to a server's driver listener, used in
// lockstep: write a frame, read a frame.
type wireConn struct {
	conn   *tls.Conn
	reader *bufio.Reader
}

// dial connects with the server's own client trust: the fingerprint pinned.
func dial(ctx context.Context, target endpoint) (*wireConn, error) {
	tlsCfg, err := transport.MakeClientTLSConfig("", "", transport.TrustPolicy{Expected: []string{target.fingerprint}}, target.driverAddr)
	if err != nil {
		return nil, fmt.Errorf("client TLS for %s: %w", target.name, err)
	}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: tlsCfg}
	raw, err := dialer.DialContext(ctx, "tcp", target.driverAddr)
	if err != nil {
		return nil, fmt.Errorf("dialling %s at %s: %w", target.name, target.driverAddr, err)
	}
	conn := raw.(*tls.Conn)
	return &wireConn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

// roundTrip writes one frame and reads the next one back.
func (c *wireConn) roundTrip(op transport.Opcode, requestID [transport.RequestIDLen]byte, payload []byte) (*transport.Frame, error) {
	if err := c.conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return nil, fmt.Errorf("setting the deadline: %w", err)
	}
	if _, err := c.conn.Write(transport.NewFrameWithID(op, requestID, payload).Encode()); err != nil {
		return nil, fmt.Errorf("writing %s: %w", op, err)
	}
	return c.read()
}

// read reads one frame.
func (c *wireConn) read() (*transport.Frame, error) {
	frame, err := transport.DecodeFrame(c.reader)
	if err != nil {
		return nil, fmt.Errorf("reading the answer: %w", err)
	}
	return frame, nil
}

func (c *wireConn) close() {
	_ = c.conn.Close()
}

// authenticate sends the root password AUTH and requires AUTH_OK.
func (c *wireConn) authenticate() error {
	return c.authenticateAs(rootUser, rootPassword)
}

// authenticateAs sends a password AUTH and requires AUTH_OK.
func (c *wireConn) authenticateAs(username, password string) error {
	payload, err := bson.Marshal(server.AuthRequest{
		Username: username, Password: password, ProtocolVersion: 1,
		Capabilities: []string{}, Client: "hoarddb-vectors/1",
	})
	if err != nil {
		return fmt.Errorf("encoding AUTH: %w", err)
	}
	answer, err := c.roundTrip(transport.OpAuth, fixedRequestID(0), payload)
	if err != nil {
		return err
	}
	if answer.Opcode != transport.OpAuthOK {
		return fmt.Errorf("AUTH as %s answered %s: %s", username, answer.Opcode, describePayload(answer.Payload))
	}
	return nil
}

// describePayload renders a payload for an error message.
func describePayload(payload []byte) string {
	if len(payload) == 0 {
		return "(empty payload)"
	}
	extended, err := bson.MarshalExtJSON(bson.Raw(payload), false, false)
	if err != nil {
		return fmt.Sprintf("(%d bytes that are not BSON: %x)", len(payload), payload)
	}
	return string(extended)
}
