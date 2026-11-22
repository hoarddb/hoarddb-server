// Package cli: the helpers `hoarddb ingest` and the other client verbs share.
//
// The ingest verb itself is RunRingIngestCommand (ringingest_run.go). This file
// keeps the connection flags the verbs declare once and two small helpers.
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// connectionFlags are the flags every verb that dials a server takes, declared
// once here so ingest does not carry a fourth pasted copy of RunDumpCommand's
// block. dump and restore are not moved onto it: GAP-24's RunMain consolidates
// them (build doc S-6).
type connectionFlags struct {
	address   *string
	config    *string
	authToken *string
	user      *string
	password  *string
	insecure  *bool
	// fingerprints is --fingerprint; connectSession refuses it until Part 9.
	fingerprints *fingerprintFlag
}

// spec is the connection half of a ConnectSpec; the caller adds the streams.
func (f *connectionFlags) spec(explicitAddress bool) ConnectSpec {
	return ConnectSpec{
		Address: *f.address, ExplicitAddress: explicitAddress, Config: *f.config,
		AuthToken: *f.authToken, User: *f.user, Password: *f.password,
		Insecure: *f.insecure, Fingerprints: *f.fingerprints, NoPrompt: true,
		Getenv: os.Getenv,
	}
}

// registerConnectionFlags declares --address --config --auth-token
// --user --password --insecure --fingerprint on fs. Dump, restore and ingest
// share it, so the three verbs cannot drift apart.
func registerConnectionFlags(fs *flag.FlagSet) *connectionFlags {
	fingerprints := &fingerprintFlag{}
	fs.Var(fingerprints, "fingerprint", "pin the server's certificate fingerprint, SHA256:<64 hex>, which the server prints when it starts")
	return &connectionFlags{
		fingerprints: fingerprints,
		address:      fs.String("address", "localhost:7433", addressFlagUsage),
		config:       fs.String("config", "", "path to YAML configuration file"),
		authToken:    fs.String("auth-token", "", "AUTH resumption token (env: HOARDDB_AUTH_TOKEN)"),
		user:         fs.String("user", "", "root username (env: HOARDDB_ROOT_USER)"),
		password:     fs.String("password", "", "root password (env: HOARDDB_ROOT_PASSWORD)"),
		insecure:     fs.Bool("insecure", false, "skip TLS certificate verification (local development only)"),
	}
}

// firstLine picks the flag package's own complaint out of its usage dump.
func firstLine(usage string, err error) string {
	if line, _, _ := strings.Cut(strings.TrimSpace(usage), "\n"); line != "" {
		return line
	}
	return err.Error()
}

func isTerminalFile(file *os.File) bool {
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

// describeFSError names the reason for a filesystem failure without the path.
func describeFSError(err error) string {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err.Error()
	}
	return err.Error()
}

// readHead returns the first bytes of a file, for format detection.
func readHead(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, &ingestUsageError{Message: fmt.Sprintf("cannot open %s: %v. Check the path, or pass '-' to read standard input.", path, describeFSError(err))}
	}
	defer func() { _ = file.Close() }()
	head := make([]byte, 4096)
	count, err := io.ReadFull(file, head)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("reading the start of %s: %w", path, err)
	}
	return head[:count], nil
}
