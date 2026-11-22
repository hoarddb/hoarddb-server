package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/server"
)

// concurrentBuffer is a bytes.Buffer guarded by a sync.Mutex, safe for
// concurrent writes (from the server's capacity sampler) and reads (from
// tests). It implements io.Writer.
type concurrentBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p to the buffer under lock and returns len(p), nil.
func (cb *concurrentBuffer) Write(p []byte) (int, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.buf.Write(p)
}

// String returns a copy of the buffer's contents under lock.
func (cb *concurrentBuffer) String() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.buf.String()
}

// TestNoArgumentStartPathInTempDir checks that, with no
// arguments and no credential environment, startup must use a data directory
// relative to the working directory and bootstrap a root credential, so the
// CLI can authenticate afterwards.
func TestNoArgumentStartPathInTempDir(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("HOARDDB_DATA_DIR", "")
	t.Setenv("HOARDDB_ROOT_USER", "")
	t.Setenv("HOARDDB_ROOT_PASSWORD", "")
	t.Setenv("HOARDDB_CLUSTER_TOKEN", "")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if cfg.Storage.DataDir != "./data" {
		t.Fatalf("data_dir = %q, want ./data", cfg.Storage.DataDir)
	}
	if cfg.Server.RootUser != "admin" {
		t.Fatalf("root user = %q, want admin", cfg.Server.RootUser)
	}

	// No argument and no credential setting: the operator types the root
	// password at the terminal, which is the one thing an unattended first
	// start cannot do for itself.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	terminal := &scriptedTerminal{interactive: true, answers: []string{startRootPassword, startRootPassword}}
	srv, err := prepare(cfg, logger, io.Discard, terminal)
	if err != nil {
		t.Fatalf("prepare (no-argument start path): %v", err)
	}
	if srv == nil {
		t.Fatal("prepare returned a nil service")
	}
	// A prepared service owns a capacity sampler goroutine, so the test must
	// stop it rather than leave it logging into this test's handlers.
	defer func() { _ = srv.Stop() }()

	// The data directory was created relative to the working directory, not
	// at /data, and the root credential is there as a hash in users.json,
	// never as a plain-text file.
	if _, err := os.Stat(server.UsersFilePath("data")); err != nil {
		t.Fatalf("users.json was not created under ./data: %v", err)
	}
	if _, err := os.Stat(filepath.Join("data", "root.password")); !os.IsNotExist(err) {
		t.Fatalf("a first start wrote root.password under ./data (stat error: %v)", err)
	}
	if _, err := os.Stat(filepath.Join("data", "cluster.token")); !os.IsNotExist(err) {
		t.Fatalf("a first start wrote cluster.token under ./data (stat error: %v), and this release has no cluster token", err)
	}
}

// TestVersionFlagSpellingsMatch pins GAP-4: -v, -version and --version print
// one identical string, and it names every field.
func TestVersionFlagSpellingsMatch(t *testing.T) {
	for _, spelling := range []string{"-v", "-version", "--version"} {
		t.Run(spelling, func(t *testing.T) {
			got := versionLine("hoarddb-server")
			if !strings.Contains(got, "version=") || !strings.Contains(got, "commit=") {
				t.Fatalf("%s output = %q, missing version/commit fields", spelling, got)
			}
		})
	}
}

// TestVersionFlagHasNoSideEffects runs the real binary from an empty temp
// directory and asserts it creates no data directory: the flag must print and
// exit before config loading or the first-run bootstrap.
func TestVersionFlagHasNoSideEffects(t *testing.T) {
	dir := t.TempDir()

	// Build the binary from the module root (two levels up: cmd/server -> cmd -> module).
	out, err := exec.Command("go", "build", "-C", "../..", "-o", dir+"/hsrv", "./cmd/server").CombinedOutput()
	if err != nil {
		t.Fatalf("build server: %v\n%s", err, out)
	}

	for _, spelling := range []string{"-v", "-version", "--version"} {
		cmdOut, runErr := exec.Command(dir+"/hsrv", spelling).CombinedOutput()
		if runErr != nil {
			t.Fatalf("%s: %v\n%s", spelling, runErr, cmdOut)
		}
		if !strings.Contains(string(cmdOut), "version=") {
			t.Fatalf("%s = %q, want a version line", spelling, cmdOut)
		}
	}

	// Run -v with the empty temp dir as the working directory: a data
	// directory is the first-run bootstrap's side effect, and none must
	// appear.
	cmd := exec.Command(dir+"/hsrv", "-v")
	cmd.Dir = dir
	if _, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("-v from the temp dir: %v", err)
	}
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if entry.Name() != "hsrv" {
			t.Fatalf("-v created %q in the working directory; the flag must have no side effects", entry.Name())
		}
	}
}

// loadStartConfig is config.Load in a fresh working directory with no
// credential or encryption environment leaking in from the caller's shell.
func loadStartConfig(t *testing.T) *config.Config {
	t.Helper()
	t.Chdir(t.TempDir())
	for _, name := range []string{"HOARDDB_DATA_DIR", "HOARDDB_ROOT_USER", "HOARDDB_ROOT_PASSWORD", "HOARDDB_CLUSTER_TOKEN",
		"HOARDDB_ENCRYPTION_KEYSTORE", "HOARDDB_ENCRYPTION_KEY_FILE", "HOARDDB_ENCRYPTION_MODE"} {
		t.Setenv(name, "")
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	return cfg
}

// startRootPassword is the root password the tests that need a started
// server give a first start: with no terminal, a first start refuses without
// one.
const startRootPassword = "first-start-root-password"

// loadStartConfigWithRoot is loadStartConfig with the root password set, as
// an unattended first start needs it.
func loadStartConfigWithRoot(t *testing.T) *config.Config {
	t.Helper()
	cfg := loadStartConfig(t)
	cfg.Server.RootPassword = startRootPassword
	return cfg
}

// sealWithKeyFile points cfg at a fresh 32-byte KEK outside the data directory
// and turns encryption on.
func sealWithKeyFile(t *testing.T, cfg *config.Config) {
	t.Helper()
	kekPath := filepath.Join(t.TempDir(), "kek")
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = byte(0xA0 + i)
	}
	if err := os.WriteFile(kekPath, kek, 0o600); err != nil {
		t.Fatalf("writing the KEK: %v", err)
	}
	cfg.Encryption = config.EncryptionConfig{Keystore: "keyfile", KeyFile: kekPath, Mode: "on"}
}

// startedNodeMaster unwraps the node master prepare left on disk, so a test can
// look for its bytes and its kid in what was printed.
func startedNodeMaster(t *testing.T, cfg *config.Config) *server.NodeMaster {
	t.Helper()
	keystore, err := server.OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("opening the keystore: %v", err)
	}
	nodeMaster, err := server.LoadOrCreateNodeMaster(cfg.Storage.DataDir, keystore)
	if err != nil {
		t.Fatalf("loading the node master: %v", err)
	}
	return nodeMaster
}

// TestPrepareLoadsTheCatalogBeforeStart: main starts the server it prepared,
// so prepare must have loaded the catalog. The cluster's start-time sweep walks
// the catalog, and over an empty one it would mark the node settled at a
// membership change it never handed off (join-freeze build doc step 5, S-8).
func TestPrepareLoadsTheCatalogBeforeStart(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	srv, err := prepare(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard, noTerminal())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer func() { _ = srv.Stop() }()
	if !srv.CatalogLoaded() {
		t.Fatal("prepare returned a service whose catalog is not loaded; main starts it next")
	}
}

// TestEncryptionBannerOnFirstEncryptedStart: the first start that generates
// the node master prints one screen an operator cannot miss, naming what is
// sealed, where the keys are, and that losing the keystore loses the data
// (spec §3 step 10). No key byte appears in it or in the log.
func TestEncryptionBannerOnFirstEncryptedStart(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	sealWithKeyFile(t, cfg)
	var banners concurrentBuffer
	var logs concurrentBuffer
	srv, err := prepare(cfg, slog.New(slog.NewTextHandler(&logs, nil)), &banners, noTerminal())
	if err != nil {
		t.Fatalf("prepare (first encrypted start): %v", err)
	}
	defer func() { _ = srv.Stop() }()

	nodeMaster := startedNodeMaster(t, cfg)
	for _, want := range []string{
		"ENCRYPTION AT REST",
		"mode:            on",
		"keystore: keyfile",
		nodeMaster.Kid(),
		filepath.Join(cfg.Storage.DataDir, "store"),
		filepath.Join(cfg.Storage.DataDir, "keys", "store.wrapped"),
		filepath.Join(cfg.Storage.DataDir, "keys", "node-master.key.wrapped"),
		"unrecoverable data loss",
		"the configuration page of the user docs",
	} {
		if !strings.Contains(banners.String(), want) {
			t.Errorf("the first-start banner does not name %q\nbanner:\n%s", want, banners.String())
		}
	}
	printed := banners.String() + logs.String()
	if strings.Contains(printed, hex.EncodeToString(nodeMaster.Bytes())) || bytes.Contains([]byte(printed), nodeMaster.Bytes()) {
		t.Fatal("the node master's bytes were printed")
	}
}

// TestEncryptionBannerOnlyOnFirstStart: a later start prints no banner, and
// logs the mode and the kid on one line instead (spec §2.6).
func TestEncryptionBannerOnlyOnFirstStart(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	sealWithKeyFile(t, cfg)
	firstSrv, err := prepare(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard, noTerminal())
	if err != nil {
		t.Fatalf("prepare (first start): %v", err)
	}
	if err := firstSrv.Stop(); err != nil {
		t.Fatalf("stop first server: %v", err)
	}

	var banners, logs concurrentBuffer
	srv, err := prepare(cfg, slog.New(slog.NewTextHandler(&logs, nil)), &banners, noTerminal())
	if err != nil {
		t.Fatalf("prepare (restart): %v", err)
	}
	defer func() { _ = srv.Stop() }()
	if strings.Contains(banners.String(), "ENCRYPTION AT REST") {
		t.Fatalf("a restart printed the first-start banner again:\n%s", banners.String())
	}
	nodeMaster := startedNodeMaster(t, cfg)
	for _, want := range []string{`msg="encryption at rest"`, "mode=on", "keystore=keyfile", "node_master_kid=" + nodeMaster.Kid()} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("the restart's log does not carry %q\nlog: %s", want, logs.String())
		}
	}
}

// TestNoEncryptionBannerWhenModeOff: the zero-config start prints no
// encryption banner, says in the log that encryption is off, and writes no
// key file.
func TestNoEncryptionBannerWhenModeOff(t *testing.T) {
	cfg := loadStartConfigWithRoot(t)
	var banners, logs concurrentBuffer
	srv, err := prepare(cfg, slog.New(slog.NewTextHandler(&logs, nil)), &banners, noTerminal())
	if err != nil {
		t.Fatalf("prepare (zero config): %v", err)
	}
	defer func() { _ = srv.Stop() }()
	if strings.Contains(banners.String(), "ENCRYPTION AT REST") {
		t.Fatalf("mode=off printed the encryption banner:\n%s", banners.String())
	}
	if !strings.Contains(logs.String(), `msg="encryption at rest" mode=off`) {
		t.Errorf("mode=off is not stated in the start-up log\nlog: %s", logs.String())
	}
	if _, err := os.Stat(filepath.Join(cfg.Storage.DataDir, "keys", "node-master.key.wrapped")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("mode=off wrote a node master key file (stat: %v)", err)
	}
}

// writeTestKEK writes a 32-byte key-encryption key at 0600, distinct per
// seed byte so two calls in the same test never collide.
func writeTestKEK(t *testing.T, path string, seed byte) {
	t.Helper()
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = seed + byte(i)
	}
	if err := os.WriteFile(path, kek, 0o600); err != nil {
		t.Fatalf("writing KEK %s: %v", path, err)
	}
}

// TestExportAndImportFlagsAreWired is the CLI-level test: it builds
// the real binary and runs the escrow rehearsal (export -> destroy the
// keystore -> new KEK -> import) through the -export-key and -import-key
// flags, the pattern of TestVersionFlagHasNoSideEffects. On the pre-fix tree
// this fails at flag parsing ("flag provided but not defined: -export-key"),
// which is the bug. It also covers two more acceptance criteria: export
// refuses while a server holds the driver port, and import refuses over a
// live wrapped file without -force.
func TestExportAndImportFlagsAreWired(t *testing.T) {
	buildDir := t.TempDir()
	buildOut, err := exec.Command("go", "build", "-C", "../..", "-o", buildDir+"/hsrv", "./cmd/server").CombinedOutput()
	if err != nil {
		t.Fatalf("build server: %v\n%s", err, buildOut)
	}
	binary := buildDir + "/hsrv"

	dataDir := t.TempDir()
	kekPath1 := filepath.Join(t.TempDir(), "kek1")
	writeTestKEK(t, kekPath1, 0xA0)

	cfg := config.DefaultConfig()
	cfg.Storage.DataDir = dataDir
	cfg.Encryption.Keystore = "keyfile"
	cfg.Encryption.Mode = "on"
	cfg.Encryption.KeyFile = kekPath1

	ks1, err := server.OpenKeystore(cfg)
	if err != nil {
		t.Fatalf("OpenKeystore: %v", err)
	}
	nm1, err := server.LoadOrCreateNodeMaster(dataDir, ks1)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster (fixture): %v", err)
	}
	initialKid := nm1.Kid()

	// The simulated server owns the port from the moment it is chosen, so no
	// other process can take it before the refusal check below (BUG-0499).
	blocking, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("simulating a running server: %v", err)
	}
	tcpAddr := blocking.Addr().String()
	env1 := append(os.Environ(),
		"HOARDDB_DATA_DIR="+dataDir,
		"HOARDDB_ENCRYPTION_KEYSTORE=keyfile",
		"HOARDDB_ENCRYPTION_KEY_FILE="+kekPath1,
		"HOARDDB_ENCRYPTION_MODE=on",
		"HOARDDB_TCP_LISTEN="+tcpAddr,
	)

	// Acceptance criterion: export refuses while the server runs.
	refusedExportPath := filepath.Join(t.TempDir(), "should-not-exist.key")
	cmd := exec.Command(binary, "-export-key", refusedExportPath)
	cmd.Env = env1
	refuseOut, refuseErr := cmd.CombinedOutput()
	if refuseErr == nil {
		t.Fatalf("-export-key should refuse while the server is running; output:\n%s", refuseOut)
	}
	if !strings.Contains(string(refuseOut), tcpAddr) {
		t.Errorf("the refusal does not name the address it found bound (%s):\n%s", tcpAddr, refuseOut)
	}
	if _, statErr := os.Stat(refusedExportPath); !os.IsNotExist(statErr) {
		t.Errorf("export wrote %s despite refusing while the server ran", refusedExportPath)
	}
	if err := blocking.Close(); err != nil {
		t.Fatalf("closing the simulated server listener: %v", err)
	}

	// Export, through the -export-key flag.
	exportPath := filepath.Join(t.TempDir(), "nm.key")
	cmd = exec.Command(binary, "-export-key", exportPath)
	cmd.Env = env1
	exportOut, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("-export-key: %v\n%s", err, exportOut)
	}
	if !strings.Contains(string(exportOut), initialKid) {
		t.Fatalf("export output does not name the kid %q:\n%s", initialKid, exportOut)
	}
	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatalf("export did not write %s: %v", exportPath, err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("export file perm = %04o, want 0600", perm)
	}
	exportedBytes, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("reading the exported file: %v", err)
	}
	if !bytes.Equal(exportedBytes, nm1.Bytes()) {
		t.Fatal("exported bytes do not match the node master that was on disk")
	}

	// Destroy the keystore: point the config at a brand-new KEK, so the
	// wrapped file left on disk can no longer be opened.
	kekPath2 := filepath.Join(t.TempDir(), "kek2")
	writeTestKEK(t, kekPath2, 0xB0)
	env2 := append(os.Environ(),
		"HOARDDB_DATA_DIR="+dataDir,
		"HOARDDB_ENCRYPTION_KEYSTORE=keyfile",
		"HOARDDB_ENCRYPTION_KEY_FILE="+kekPath2,
		"HOARDDB_ENCRYPTION_MODE=on",
		"HOARDDB_TCP_LISTEN="+tcpAddr,
	)

	// Acceptance criterion: import refuses over a live wrapped file without
	// -force.
	cmd = exec.Command(binary, "-import-key", exportPath)
	cmd.Env = env2
	noForceOut, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("-import-key without -force should refuse over the live wrapped file; output:\n%s", noForceOut)
	}
	if !strings.Contains(string(noForceOut), "-force") {
		t.Errorf("the refusal does not mention -force:\n%s", noForceOut)
	}

	// Import, through the -import-key and -force flags, under the new KEK.
	cmd = exec.Command(binary, "-import-key", exportPath, "-force")
	cmd.Env = env2
	importOut, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("-import-key -force: %v\n%s", err, importOut)
	}
	if !strings.Contains(string(importOut), initialKid) {
		t.Fatalf("import output does not name the kid %q:\n%s", initialKid, importOut)
	}

	// The new keystore now unwraps the same bytes: the rehearsal closed the
	// loop.
	cfg2 := config.DefaultConfig()
	cfg2.Storage.DataDir = dataDir
	cfg2.Encryption.Keystore = "keyfile"
	cfg2.Encryption.Mode = "on"
	cfg2.Encryption.KeyFile = kekPath2

	ks2, err := server.OpenKeystore(cfg2)
	if err != nil {
		t.Fatalf("OpenKeystore (new KEK): %v", err)
	}
	nm2, err := server.LoadOrCreateNodeMaster(dataDir, ks2)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster (after import): %v", err)
	}
	if !bytes.Equal(nm1.Bytes(), nm2.Bytes()) {
		t.Fatal("node master bytes changed across the escrow rehearsal")
	}
	if nm2.Kid() != initialKid {
		t.Errorf("node master kid changed: got %q, want %q", nm2.Kid(), initialKid)
	}
}

// buildServerBinary compiles the real server so a test drives it through its
// flags and environment, which is the only path a config-file bug could break.
func buildServerBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "hsrv")
	buildOut, err := exec.Command("go", "build", "-C", "../..", "-o", binary, "./cmd/server").CombinedOutput()
	if err != nil {
		t.Fatalf("build server: %v\n%s", err, buildOut)
	}
	return binary
}

// serverEnvironment is the process environment without any HOARDDB_ variable
// the developer's shell happens to export, plus extra NAME=value pairs.
func serverEnvironment(extra ...string) []string {
	var environment []string
	for _, pair := range os.Environ() {
		if !strings.HasPrefix(pair, "HOARDDB_") {
			environment = append(environment, pair)
		}
	}
	// Every test server is given its root password; a
	// test of the no-password first start overrides it with an empty value.
	environment = append(environment, "HOARDDB_ROOT_USER=admin", "HOARDDB_ROOT_PASSWORD="+testRootPassword)
	return append(environment, extra...)
}

// testRootPassword is the root password of every server a test starts.
const testRootPassword = "cmd-server-test-root-password"

// runningServer is a server process a test started and must stop.
type runningServer struct {
	command *exec.Cmd
	// output is the server's stdout+stderr sink. It is a concurrentBuffer
	// because the live child process writes to it and a test may read it
	// before stop returns.
	output *concurrentBuffer
	exited chan error
}

func startServerBinary(t *testing.T, binary, workDir string, environment []string, args ...string) *runningServer {
	t.Helper()
	started := &runningServer{
		command: exec.Command(binary, args...),
		output:  &concurrentBuffer{},
		exited:  make(chan error, 1),
	}
	started.command.Dir = workDir
	started.command.Env = environment
	started.command.Stdout = started.output
	started.command.Stderr = started.output
	if err := started.command.Start(); err != nil {
		t.Fatalf("starting the server: %v", err)
	}
	go func() { started.exited <- started.command.Wait() }()
	t.Cleanup(func() { started.stop() })
	return started
}

// stop ends the server this test started, and only it.
func (started *runningServer) stop() {
	_ = started.command.Process.Signal(syscall.SIGTERM)
	select {
	case <-started.exited:
	case <-time.After(20 * time.Second):
		_ = started.command.Process.Kill()
		<-started.exited
	}
}

// waitForFile fails the test if the server exits, or the file does not appear.
func (started *runningServer) waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.After(60 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case err := <-started.exited:
			started.exited <- err
			t.Fatalf("the server exited (%v) before creating %s", err, path)
		case <-deadline:
			t.Fatalf("%s did not appear within 60s", path)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// runServerToExit runs the binary and returns its combined output and exit error.
func runServerToExit(t *testing.T, binary, workDir string, environment []string, args ...string) (string, error) {
	t.Helper()
	// A server that wrongly starts would run forever; the deadline turns that
	// into a failing test instead of a hang.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = workDir
	command.Env = environment
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("the server was still running after 30s (it should have exited):\n%s", output)
	}
	return string(output), err
}

// TestHelpShowsTheConfigFlagWithAnExample: rule 8, --help shows a real
// example of the flag and the precedence.
func TestHelpShowsTheConfigFlagWithAnExample(t *testing.T) {
	binary := buildServerBinary(t)
	output, _ := runServerToExit(t, binary, t.TempDir(), serverEnvironment(), "--help")
	for _, want := range []string{"--config", "HOARDDB_CONFIG", "hoarddb-server --config /etc/hoarddb/config.yaml"} {
		if !strings.Contains(output, want) {
			t.Errorf("--help does not contain %q:\n%s", want, output)
		}
	}
}

// listenEnvironment gives a test server its own ports, so tests never
// contend for 4433 or 7433. Port 0 lets the server's own bind pick them: a
// port named here and released before the server binds can be taken in
// between (BUG-0499).
func listenEnvironment(t *testing.T) []string {
	t.Helper()
	return []string{
		"HOARDDB_LISTEN=127.0.0.1:0",
		"HOARDDB_TCP_LISTEN=127.0.0.1:0",
	}
}

func writeServerConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hoarddb.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing the config file: %v", err)
	}
	return path
}

// TestServerReadsTheConfigFileNamedByTheFlag checks that a YAML
// file names storage.data_dir, the server is started with --config, and the
// server's first-run files land in that directory and not in ./data. On the
// pre-fix tree the flag does not exist and the process exits at flag parsing.
func TestServerReadsTheConfigFileNamedByTheFlag(t *testing.T) {
	binary := buildServerBinary(t)
	workDir := t.TempDir()
	yamlDataDir := filepath.Join(t.TempDir(), "yaml-data")
	configPath := writeServerConfig(t, "storage:\n  data_dir: "+yamlDataDir+"\n")

	started := startServerBinary(t, binary, workDir, serverEnvironment(listenEnvironment(t)...), "--config", configPath)
	started.waitForFile(t, server.UsersFilePath(yamlDataDir))

	if _, err := os.Stat(filepath.Join(workDir, "data")); !os.IsNotExist(err) {
		t.Errorf("the server also created ./data (stat error: %v); the file's data_dir was not the only one used", err)
	}
}

// TestServerReadsTheConfigFileNamedByTheEnvironment: HOARDDB_CONFIG is the
// equivalent of --config.
func TestServerReadsTheConfigFileNamedByTheEnvironment(t *testing.T) {
	binary := buildServerBinary(t)
	yamlDataDir := filepath.Join(t.TempDir(), "yaml-data")
	configPath := writeServerConfig(t, "storage:\n  data_dir: "+yamlDataDir+"\n")

	environment := serverEnvironment(append(listenEnvironment(t), "HOARDDB_CONFIG="+configPath)...)
	started := startServerBinary(t, binary, t.TempDir(), environment)
	started.waitForFile(t, server.UsersFilePath(yamlDataDir))
}

// TestConfigFlagBeatsTheConfigEnvironment: when both name a file, the flag wins.
func TestConfigFlagBeatsTheConfigEnvironment(t *testing.T) {
	binary := buildServerBinary(t)
	flagDataDir := filepath.Join(t.TempDir(), "flag-data")
	envDataDir := filepath.Join(t.TempDir(), "env-data")
	flagConfig := writeServerConfig(t, "storage:\n  data_dir: "+flagDataDir+"\n")
	envConfig := writeServerConfig(t, "storage:\n  data_dir: "+envDataDir+"\n")

	environment := serverEnvironment(append(listenEnvironment(t), "HOARDDB_CONFIG="+envConfig)...)
	started := startServerBinary(t, binary, t.TempDir(), environment, "--config", flagConfig)
	started.waitForFile(t, server.UsersFilePath(flagDataDir))
	if _, err := os.Stat(envDataDir); !os.IsNotExist(err) {
		t.Errorf("the HOARDDB_CONFIG file was used although --config was given (stat error: %v)", err)
	}
}

// TestEnvironmentOverridesTheConfigFile: the precedence is defaults, then the
// file, then HOARDDB_* variables.
func TestEnvironmentOverridesTheConfigFile(t *testing.T) {
	binary := buildServerBinary(t)
	fileDataDir := filepath.Join(t.TempDir(), "file-data")
	envDataDir := filepath.Join(t.TempDir(), "env-data")
	configPath := writeServerConfig(t, "storage:\n  data_dir: "+fileDataDir+"\n")

	environment := serverEnvironment(append(listenEnvironment(t), "HOARDDB_DATA_DIR="+envDataDir)...)
	started := startServerBinary(t, binary, t.TempDir(), environment, "--config", configPath)
	started.waitForFile(t, server.UsersFilePath(envDataDir))
	if _, err := os.Stat(fileDataDir); !os.IsNotExist(err) {
		t.Errorf("the file's data_dir was used although HOARDDB_DATA_DIR was set (stat error: %v)", err)
	}
}

// TestNoConfigFileStillStarts: a start with no configuration file must keep
// working (AGENTS.md rule 2). With no terminal the root password is the one
// setting a first start needs.
func TestNoConfigFileStillStarts(t *testing.T) {
	binary := buildServerBinary(t)
	workDir := t.TempDir()

	environment := serverEnvironment(append(listenEnvironment(t), "HOARDDB_ROOT_PASSWORD="+startRootPassword)...)
	started := startServerBinary(t, binary, workDir, environment)
	started.waitForFile(t, server.UsersFilePath(filepath.Join(workDir, "data")))
}

// TestUnknownConfigKeyStopsTheServer: a key the server does not define stops
// the start with a message that names the key, its line and the fix, and no
// data directory is created.
func TestUnknownConfigKeyStopsTheServer(t *testing.T) {
	binary := buildServerBinary(t)
	workDir := t.TempDir()
	configPath := writeServerConfig(t, "storage:\n  data_dr: /tmp/never\n")

	output, err := runServerToExit(t, binary, workDir, serverEnvironment(listenEnvironment(t)...), "--config", configPath)
	if err == nil {
		t.Fatalf("the server started with an unknown config key:\n%s", output)
	}
	for _, want := range []string{`"data_dr"`, "line 2", `did you mean "data_dir"`, configPath} {
		if !strings.Contains(output, want) {
			t.Errorf("the refusal does not contain %q:\n%s", want, output)
		}
	}
	if _, statErr := os.Stat(filepath.Join(workDir, "data")); !os.IsNotExist(statErr) {
		t.Errorf("the server created ./data although it refused to start (stat error: %v)", statErr)
	}
}

// TestLogReadWhileCapacitySamplerLogs reproduces a race: a test that reads
// its log buffer while the server runs races the capacity sampler, which logs
// the memory limit from its own goroutine on its first sample. The gate holds
// that sample until prepare has returned, so the sampler's write always lands
// while the read loop below runs, with nothing ordering the two.
func TestLogReadWhileCapacitySamplerLogs(t *testing.T) {
	gateOpen := make(chan struct{})
	releaseSampler := sync.OnceFunc(func() { close(gateOpen) })
	gate := func() { <-gateOpen }
	server.CapacityFirstSampleGate.Store(&gate)
	t.Cleanup(func() { server.CapacityFirstSampleGate.Store(nil) })

	cfg := loadStartConfigWithRoot(t)
	var logs concurrentBuffer
	srv, err := prepare(cfg, slog.New(slog.NewTextHandler(&logs, nil)), io.Discard, noTerminal())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	// Stop waits for the sampler, which waits for the gate: release first.
	defer func() {
		releaseSampler()
		_ = srv.Stop()
	}()

	releaseSampler()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(logs.String(), "limit_bytes=") {
		if time.Now().After(deadline) {
			t.Fatal("the capacity sampler never logged its memory limit")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestMissingConfigFileStopsTheServer: a path that was asked for and cannot be
// read stops the start, naming the path and the way out.
func TestMissingConfigFileStopsTheServer(t *testing.T) {
	binary := buildServerBinary(t)
	missing := filepath.Join(t.TempDir(), "absent.yaml")

	cases := []struct {
		name        string
		args        []string
		environment []string
	}{
		{name: "flag", args: []string{"--config", missing}},
		{name: "environment", environment: []string{"HOARDDB_CONFIG=" + missing}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workDir := t.TempDir()
			output, err := runServerToExit(t, binary, workDir, serverEnvironment(tc.environment...), tc.args...)
			if err == nil {
				t.Fatalf("the server started although %s does not exist:\n%s", missing, output)
			}
			for _, want := range []string{missing, "HOARDDB_CONFIG"} {
				if !strings.Contains(output, want) {
					t.Errorf("the refusal does not contain %q:\n%s", want, output)
				}
			}
			if _, statErr := os.Stat(filepath.Join(workDir, "data")); !os.IsNotExist(statErr) {
				t.Errorf("the server created ./data although it refused to start (stat error: %v)", statErr)
			}
		})
	}
}

// TestKeyCommandIsReadFromTheConfigFile: key_command has no environment form,
// so before the config file was read a binary-started node could not use keystore=command. The
// fixture wraps a node master under a KEK a script prints; -export-key,
// started with only the config file, must unwrap it.
func TestKeyCommandIsReadFromTheConfigFile(t *testing.T) {
	binary := buildServerBinary(t)
	dataDir := t.TempDir()
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = byte(0xC0 + i)
	}
	scriptPath := filepath.Join(t.TempDir(), "getkey.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nprintf '"+hex.EncodeToString(kek)+"'\n"), 0o755); err != nil {
		t.Fatalf("writing the key script: %v", err)
	}

	fixtureConfig := config.DefaultConfig()
	fixtureConfig.Storage.DataDir = dataDir
	fixtureConfig.Encryption = config.EncryptionConfig{Keystore: "command", Mode: "on", KeyCommand: []string{scriptPath}}
	keystore, err := server.OpenKeystore(fixtureConfig)
	if err != nil {
		t.Fatalf("OpenKeystore (fixture): %v", err)
	}
	nodeMaster, err := server.LoadOrCreateNodeMaster(dataDir, keystore)
	if err != nil {
		t.Fatalf("LoadOrCreateNodeMaster (fixture): %v", err)
	}

	configPath := writeServerConfig(t, "storage:\n  data_dir: "+dataDir+"\n"+
		"encryption:\n  keystore: command\n  mode: \"on\"\n  key_command: [\""+scriptPath+"\"]\n")
	exportPath := filepath.Join(t.TempDir(), "nm.key")
	output, err := runServerToExit(t, binary, t.TempDir(), serverEnvironment(listenEnvironment(t)...),
		"--config", configPath, "-export-key", exportPath)
	if err != nil {
		t.Fatalf("-export-key with key_command from the file: %v\n%s", err, output)
	}
	exported, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatalf("reading the exported key: %v", err)
	}
	if !bytes.Equal(exported, nodeMaster.Bytes()) {
		t.Error("the exported node master does not match the one wrapped under the key_command KEK")
	}
}

// TestRefusedLegacyStartWritesNothing: a data directory in the per-bucket
// layout is refused before the cluster token, the keys or anything else is
// written beside it, because the refusal says nothing in the directory was
// changed.
func TestRefusedLegacyStartWritesNothing(t *testing.T) {
	t.Setenv("HOARDDB_CLUSTER_TOKEN", "")
	t.Setenv("HOARDDB_CLUSTER_TOKEN_FILE", "")
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, "catalog.json"), []byte(`{"version": 1}`), 0o644); err != nil {
		t.Fatalf("writing the legacy catalog: %v", err)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	cfg.Storage.DataDir = dataDir

	_, err = prepare(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), io.Discard, noTerminal())
	if err == nil || !strings.Contains(err.Error(), "per-bucket storage layout") {
		t.Fatalf("prepare on a legacy data directory = %v, want the layout refusal", err)
	}
	entries, readErr := os.ReadDir(dataDir)
	if readErr != nil {
		t.Fatalf("reading the data directory: %v", readErr)
	}
	if len(entries) != 1 || entries[0].Name() != "catalog.json" {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Errorf("the refused start left %v in the data directory, want only catalog.json", names)
	}
}

func TestVersionLineNamesTheStateLevelAndRange(t *testing.T) {
	line := versionLine("hoarddb-server")
	want := fmt.Sprintf("state_level=%d state_range=%d-%d", cluster.StateLevel, cluster.StateLevel, cluster.StateLevel)
	if !strings.Contains(line, want) {
		t.Fatalf("-v prints %q, want it to contain %q so an operator can compare two builds", line, want)
	}
}
