package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/hoarddb/hoarddb-server/cluster"
	"github.com/hoarddb/hoarddb-server/cluster/wire"
	"github.com/hoarddb/hoarddb-server/config"
	"github.com/hoarddb/hoarddb-server/logging"
	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/storage"
	"github.com/hoarddb/hoarddb-server/termio"
	"github.com/hoarddb/hoarddb-server/transport"
	"github.com/hoarddb/hoarddb-server/version"
)

func main() {
	configFlag := flag.String("config", "", "path to a YAML config file; "+config.PathEnv+" is the same setting and the flag wins")
	resetUser := flag.String("reset-password", "", "reset a user's password offline; the server must be stopped (prompts twice, no echo)")
	exportKeyPath := flag.String("export-key", "", "export the node master key to <file> for escrow, then exit; the server must be stopped and <file> must be outside data_dir")
	importKeyPath := flag.String("import-key", "", "import a node master key from <file> and rewrap it under the configured keystore, then exit; the server must be stopped")
	force := flag.Bool("force", false, "allow -reset-password on a node that is not the recorded cluster master, or -import-key to overwrite an existing wrapped node master")
	versionFlag := flag.Bool("v", false, "print the version and exit")
	// Go's flag package treats --version as the same flag as -version, so a
	// single registration covers -v, -version and --version.
	flag.BoolVar(versionFlag, "version", false, "print the version and exit")
	flag.Usage = printUsage
	flag.Parse()

	// The version flag prints and exits before any side effect: no data
	// directory, no TLS, no listener (GAP-4; a --help that
	// binds a port is the same trap).
	if *versionFlag {
		printVersion("hoarddb-server")
		return
	}

	configPath := config.ResolvePath(*configFlag)
	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.Output)

	// Before anything is created: the offline key verbs below write key files
	// too. The logger exists first so a change is visible.
	if umask := server.RestrictUmask(); umask.Applied != umask.Previous {
		logger.Info("umask restricted: new files are owner-only",
			"previous", fmt.Sprintf("%04o", umask.Previous), "applied", fmt.Sprintf("%04o", umask.Applied))
	}

	if *resetUser != "" {
		if err := runResetPassword(cfg, *resetUser, *force, logger); err != nil {
			logger.Error("password reset failed", "error", err)
			os.Exit(1)
		}
		return
	}

	if *exportKeyPath != "" {
		if err := runExportNodeMaster(cfg, *exportKeyPath); err != nil {
			logger.Error("export node master failed", "error", err)
			os.Exit(1)
		}
		return
	}

	if *importKeyPath != "" {
		if err := runImportNodeMaster(cfg, *importKeyPath, *force); err != nil {
			logger.Error("import node master failed", "error", err)
			os.Exit(1)
		}
		return
	}

	if configPath != "" {
		logger.Info("config file read", "path", configPath)
	}
	logger.Info("hoardDB server starting",
		"node_id", cfg.Server.NodeID,
		"listen", cfg.Server.Listen,
		"version", version.String(),
		"go_memory_limit", server.SetGoMemoryLimit(),
	)
	logListenAddresses(logger, cfg)

	srv, err := prepare(cfg, logger, os.Stderr, stdinTerminal{})
	if err != nil {
		logger.Error("failed to prepare server", "error", err)
		os.Exit(1)
	}

	registerSignals := func(ch chan<- os.Signal) { signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM) }
	if code := runService(srv, registerSignals, logger); code != 0 {
		os.Exit(code)
	}
}

// ringService is what runService needs of the server: Start, the abort of its boot wait, and the
// wait for a signal or a divergence.
type ringService interface {
	Start() error
	AbortBootWait()
	WaitForExit(signals <-chan os.Signal) int
}

// runService registers the shutdown signals, starts the server, waits for a signal or a
// divergence and returns the process exit status. The registration comes before Start, which
// can wait up to 30 s for a node restarted with its peers down: a signal in that wait must not
// kill the process mid-start (the default action, with no Notify), and it must not sit buffered
// until Start returns either, so the first signal aborts the boot wait and Start returns at once
// for Stop to run; the signal itself is then handed to WaitForExit.
func runService(srv ringService, register func(chan<- os.Signal), logger *slog.Logger) int {
	received := make(chan os.Signal, 1)
	register(received)
	forwarded := make(chan os.Signal, 1)
	finished := make(chan struct{})
	defer close(finished)
	go func() {
		select {
		case sig := <-received:
			srv.AbortBootWait()
			forwarded <- sig
		case <-finished:
		}
	}()

	// Start the server (non-blocking once the log is applied or the boot wait ends).
	if err := srv.Start(); err != nil {
		logger.Error("failed to start server", "error", err)
		return startFailureExitCode(err)
	}
	// A signal, or the ring node reporting that it disagrees with the cluster (exit 70).
	return srv.WaitForExit(forwarded)
}

// startFailureExitCode is the exit status of a start that failed: cluster.DivergedExitCode when
// the node's newest snapshot was written by a build this one cannot read, or its data directory is
// at a cluster version this build cannot run (the same "wrong build, do not restart" meaning as a
// divergence found in the log), or its CLUSTER_VERSION marker is not a line this build writes (a
// restart cannot fix the file), or its ring directory was written in another ring format (every
// directory of an earlier build, which a restart cannot change either), or its ring store's sync
// marker was written by another build (the same case, one file down), 1 otherwise.
func startFailureExitCode(err error) int {
	var foreign *cluster.ForeignSnapshotError
	var unrunnable *cluster.ClusterVersionError
	var unreadable *cluster.ClusterVersionMarkerError
	var otherFormat *wire.RingFormatRefusal
	var otherMarker *storage.VouchedMarkerFormatError
	if errors.As(err, &foreign) || errors.As(err, &unrunnable) || errors.As(err, &unreadable) || errors.As(err, &otherFormat) ||
		errors.As(err, &otherMarker) {
		return cluster.DivergedExitCode
	}
	return 1
}

// versionLine renders the -v output for a binary: name, the version package's
// full line, and the protocol version, so the flag and the startup log cannot
// drift. state_level and state_range are what the build declares in its handshake: a node joins
// a cluster only when its range contains the cluster's version.
func versionLine(name string) string {
	level := cluster.BuildStateRange()
	return fmt.Sprintf("%s %s protocol=%d state_level=%d state_range=%d-%d", name, version.String(), transport.ProtocolMajor, cluster.StateLevel, level.Min, level.Max)
}

// printVersion writes the version line to stdout. Split from main so the
// side-effect-free contract is testable without building a binary.
func printVersion(name string) {
	fmt.Printf("%s\n", versionLine(name))
}

// rootPasswordSetting names the setting a configured root password came from.
// The file wins when both are set (config.applySecretFileOverrides), so its
// name is the one a refusal must point at.
func rootPasswordSetting() string {
	if os.Getenv("HOARDDB_ROOT_PASSWORD_FILE") != "" {
		return "HOARDDB_ROOT_PASSWORD_FILE"
	}
	return "HOARDDB_ROOT_PASSWORD"
}

// prepare performs the first-run bootstrap and builds the server: the state
// directory and TLS keys, the catalog, and the root credentials. It is split out of main so the no-argument start path can be
// tested in a temp working directory without binding sockets. The one-screen
// banners an operator must see (the root credentials, the
// encryption key-loss warning) go to operatorOut, which main sets to stderr.
func prepare(cfg *config.Config, logger *slog.Logger, operatorOut io.Writer, terminal firstStartTerminal) (*server.Service, error) {
	// Before anything is written: the old replicated mode is no longer supported.
	if err := cfg.RefuseOldReplicatedMode(); err != nil {
		return nil, err
	}

	// First, before anything is written beside data this server cannot read.
	if err := server.RefuseLegacyLayout(cfg); err != nil {
		return nil, err
	}

	// Choose the root password before anything is written either, so a
	// refusal or a mistyped prompt leaves the data directory as it was.
	rootUser := cfg.Server.RootUser
	if rootUser == "" {
		rootUser = "admin"
	}
	root, err := chooseRootPassword(cfg, rootUser, terminal)
	if err != nil {
		return nil, err
	}

	// Open the keystore and the node master before any credential file is written: with
	// encryption at rest on, each is sealed under the node master from its first write.
	keys, err := server.OpenNodeKeys(cfg)
	if err != nil {
		server.AuditFailedKeyUnwrap(cfg, logger, err)
		return nil, fmt.Errorf("create server: %w", err)
	}

	// The data directory is created here, owner-only, whatever the process umask: the files
	// written below hold credentials.
	if err := os.MkdirAll(cfg.Storage.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory %s: %w", cfg.Storage.DataDir, err)
	}

	// This release has no cluster token: members trust each other by pinned keys and nothing uses
	// the token in a cluster.token, so a first start writes none. An operator who still sets the
	// variable is told once that it does nothing. A cluster.token an earlier build left in the
	// data directory is left as it is without encryption at rest; with it, the start seals a
	// token file in place like every credential file, and refuses one that is neither sealed nor
	// a token file, naming the fix (move it aside).
	for _, name := range []string{"HOARDDB_CLUSTER_TOKEN", "HOARDDB_CLUSTER_TOKEN_FILE"} {
		if os.Getenv(name) != "" {
			logger.Warn(name + " is not used by this release: a node joins a cluster on its pinned key " +
				"(HOARDDB_SEED_FINGERPRINTS), not on a token. Unset it")
			break
		}
	}

	// Create the server.
	srv, err := server.NewServiceWithKeys(cfg, logger, keys)
	if err != nil {
		server.AuditFailedKeyUnwrap(cfg, logger, err)
		return nil, fmt.Errorf("create server: %w", err)
	}
	// What a cluster member did with a root password is told here as well as in the log.
	srv.SetOperatorOutput(operatorOut)
	reportEncryption(operatorOut, logger, srv.EncryptionStatus())

	// Re-attach the databases and buckets already on disk. Without this the
	// server comes up reporting an empty data directory while the data sits
	// there unreachable.
	if err := srv.LoadCatalog(); err != nil {
		return nil, fmt.Errorf("load catalog: %w", err)
	}

	// Bootstrap the root credential. The user store was already loaded by
	// NewService; BootstrapUsers applies the spec 7.1 precedence (seed only a
	// missing file, never overwrite a stored record). Only the password's hash
	// is kept, in users.json: the plaintext is never written. A
	// server with no way in is the defect this closes: the CLI refuses every
	// command until AUTH succeeds.
	if root.source == rootFromSetting {
		// Wipe the credential from the environment and config after use.
		_ = os.Unsetenv("HOARDDB_ROOT_PASSWORD")
		cfg.Server.RootPassword = ""
	}
	if err := srv.BootstrapUsersFrom(rootUser, root.password, cfg.Auth.AuthBootstrapRoot, rootPasswordSetting()); err != nil {
		return nil, fmt.Errorf("bootstrap root user %q: %w", rootUser, err)
	}
	usersPath := server.UsersFilePath(cfg.Storage.DataDir)
	if root.source == rootPrompted && cfg.Cluster.Enabled {
		if _, err := fmt.Fprintf(operatorOut, "\nThe cluster's first administrator %q is created through the cluster once this node has started. Only the password's hash is kept, in the cluster's state.\nConnect with: hoarddb -insecure -user %s -password\n\n",
			rootUser, rootUser); err != nil {
			logger.Warn("could not print the root user note", "error", err)
		}
	} else if root.source == rootPrompted {
		if _, err := fmt.Fprintf(operatorOut, "\nRoot user %q created. Only the password's hash is stored, in %s.\nConnect with: hoarddb -insecure -user %s -password\n\n",
			rootUser, usersPath, rootUser); err != nil {
			logger.Warn("could not print the root user note", "error", err)
		}
	}
	convertRootPasswordFile(cfg.Storage.DataDir, usersPath, logger)

	return srv, nil
}

// runResetPassword performs the offline password reset: prove the server is
// stopped, prompt twice with no echo, apply the reset, and print exactly what
// was written so the operator can check it (spec 7.1 rule 3).
func runResetPassword(cfg *config.Config, username string, force bool, logger *slog.Logger) error {
	// Before the prompt and before anything is read or written (ring user store spec 6.7).
	if err := server.RefuseResetPasswordInCluster(cfg); err != nil {
		return err
	}
	if err := server.CheckServerStopped(cfg); err != nil {
		return err
	}
	password, err := termio.ReadPasswordTwice(fmt.Sprintf("New password for %q: ", username), "Repeat new password: ")
	if err != nil {
		return err
	}
	term, version, err := server.ResetPassword(cfg, username, password, force, logger)
	if err != nil {
		return err
	}
	usersPath := server.UsersFilePath(cfg.Storage.DataDir)
	fmt.Fprintf(os.Stderr, `
=== PASSWORD RESET WRITTEN ===
file:    %s
user:    %s
term:    %d   (bumped so the reset wins the cluster fence on rejoin)
version: %d
After the server restarts, verify:
  1. the user authenticates with the new password
  2. if clustered, this config reaches every node
==============================
`, usersPath, username, term, version)
	return nil
}

// runExportNodeMaster performs the offline escrow export: prove the server is
// stopped, then delegate to server.ExportNodeMaster, which refuses a
// destination inside data_dir and refuses when no node master exists rather
// than minting one.
func runExportNodeMaster(cfg *config.Config, path string) error {
	if err := server.CheckServerStopped(cfg); err != nil {
		return err
	}
	return server.ExportNodeMaster(cfg, path)
}

// runImportNodeMaster performs the offline escrow import: prove the server is
// stopped, then delegate to server.ImportNodeMaster, which refuses to
// overwrite a live wrapped node master unless force is set.
func runImportNodeMaster(cfg *config.Config, path string, force bool) error {
	if err := server.CheckServerStopped(cfg); err != nil {
		return err
	}
	return server.ImportNodeMaster(cfg, path, force)
}

// reportEncryption states the node's encryption at rest on every start: one
// Info line with the mode and the key ids (spec §2.6), and, on the start that
// generated the node master, the one-screen banner.
func reportEncryption(operatorOut io.Writer, logger *slog.Logger, status server.EncryptionStatus) {
	if !status.Sealed() {
		logger.Info("encryption at rest", "mode", status.Mode)
		return
	}
	logger.Info("encryption at rest",
		"mode", status.Mode,
		"keystore", status.Keystore,
		"kek_kid", status.KEKKid,
		"node_master_kid", status.NodeMasterKid,
		"store", status.StoreDir,
		"store_key", status.StoreKeyPath,
	)
	if status.FirstStart {
		if err := printEncryptionBanner(operatorOut, status); err != nil {
			logger.Warn("could not print the encryption banner", "error", err, "node_master_kid", status.NodeMasterKid)
		}
	}
}

// printEncryptionBanner is printed once per data directory, on the start that
// generated the node master: after that the
// key hierarchy exists and the one thing an operator can still get wrong is
// losing the keystore. It names key ids and paths, never key bytes.
func printEncryptionBanner(operatorOut io.Writer, status server.EncryptionStatus) error {
	_, err := fmt.Fprintf(operatorOut, `
=== ENCRYPTION AT REST (shown only on first start) ===
mode:            %s (keystore: %s)
KEK kid:         %s
node master kid: %s
sealed store:    %s
key files:       %s (the node master, wrapped under the KEK)
                 %s (the store key, wrapped under the node master)
Losing the keystore is unrecoverable data loss: without the KEK, no bucket,
user or log entry on this node can be read again. Turning the keystore off
does not return the node to plaintext; it loses the data. Keep an escrow copy
of the node master away from this host -- see "Encryption at rest" in
the configuration page of the user docs.
======================================================
`, status.Mode, status.Keystore, status.KEKKid, status.NodeMasterKid,
		status.StoreDir, status.NodeMasterKeyPath, status.StoreKeyPath)
	return err
}
