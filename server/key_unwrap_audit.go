package server

import (
	"errors"
	"log/slog"
	"time"

	"github.com/hoarddb/hoarddb-server/config"
)

// AuditFailedKeyUnwrap appends one key.unwrap result=failed entry when the
// service could not resolve the keystore or unwrap the node master, before the
// service and its audit logger exist (BUG-0315). It is best-effort: a failure
// to write the entry is a Warn and never changes the refusal already in hand.
// The entry reuses the success event's shape, so one reader parses both; it
// names the key source and the error class, never key material.
func AuditFailedKeyUnwrap(cfg *config.Config, logger *slog.Logger, prepareErr error) {
	if !cfg.Audit.Enabled {
		return
	}
	var failure *KeyUnwrapFailure
	if !errors.As(prepareErr, &failure) {
		return
	}
	al, err := NewAuditLogger(cfg.Audit.Output)
	if err != nil {
		logger.Warn("could not record the failed key unwrap in the audit log",
			"error", err, "output", cfg.Audit.Output)
		return
	}
	al.Log(AuditEntry{
		Time:   time.Now().UTC(),
		Event:  EventKeyUnwrap,
		Result: "failed",
		Target: failure.Class + " " + keySourceLabel(cfg),
	})
	al.Close()
}

// keySourceLabel names where the keystore's key comes from, never its bytes:
// the key file path, the command's executable (its arguments may carry
// credentials, so they are left out), or the systemd credential's name.
func keySourceLabel(cfg *config.Config) string {
	switch cfg.Encryption.Keystore {
	case "keyfile":
		return "keyfile " + cfg.Encryption.KeyFile
	case "command":
		if len(cfg.Encryption.KeyCommand) > 0 {
			return "command " + cfg.Encryption.KeyCommand[0]
		}
		return "command"
	case "systemd-creds":
		return "systemd-creds hoarddb-dek"
	default:
		return "keystore " + cfg.Encryption.Keystore
	}
}
