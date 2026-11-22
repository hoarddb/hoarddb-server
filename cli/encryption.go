package cli

import (
	"fmt"

	"github.com/hoarddb/hoarddb-server/server"
	"github.com/hoarddb/hoarddb-server/transport"
)

// handleEncryption prints the node's encryption at rest. It rides on the
// health response, so it needs no opcode of its own.
func (h *CommandHandler) handleEncryption() error {
	if h.conn == nil {
		h.printf("Not connected to server.\n")
		return nil
	}
	var resp server.HealthResponse
	if err := h.send(transport.OpHealthCheck, &server.HealthRequest{}, &resp); err != nil {
		return fmt.Errorf("reading the node's encryption state: %w", err)
	}
	h.printEncryption(resp.Encryption)
	return nil
}

// printEncryption renders the report. It names modes, key ids and file names;
// the server sends no key bytes and this prints none. A nil report is an older
// server, which is not called plaintext because its store may well be sealed.
func (h *CommandHandler) printEncryption(report *server.HealthEncryption) {
	if report == nil {
		h.printf("This server does not report its encryption state: it is older than the 'encryption' command.\n")
		h.printf("Upgrade it, or read the \"encryption at rest\" line in its start-up log.\n")
		return
	}
	h.printf("ENCRYPTION AT REST\n")
	h.printf("---\n")
	switch {
	case report.Sealed:
		h.printf("Sealed:       yes (the store's files show it; checked when the node started)\n")
	case report.Problem != "":
		h.printf("Sealed:       NO (configured to be sealed, but the store's files do not show it)\n")
	default:
		h.printf("Sealed:       no\n")
	}
	h.printf("Mode:         %s\n", terminalSafe(report.Mode))
	h.printf("Keystore:     %s\n", terminalSafe(report.Keystore))
	if report.KEKKid != "" {
		h.printf("KEK kid:      %s\n", terminalSafe(report.KEKKid))
	}
	if report.NodeMasterKid != "" {
		h.printf("Node master:  %s\n", terminalSafe(report.NodeMasterKid))
	}
	switch {
	case report.Problem != "":
		h.printf("Problem:      %s\n", terminalSafeText(report.Problem))
		h.printf("\nDo not rely on this node's encryption. A plaintext store under encryption.mode %s is refused at start,\n", terminalSafe(report.Mode))
		h.printf("so either the store changed after it opened, its files were altered before this start, or the check\n")
		h.printf("could not read it. Restart the node; if this repeats, take a 'hoarddb dump' and report it\n")
		h.printf("(SECURITY.md).\n")
	case !report.Sealed:
		h.printf("\nThis node's store is plaintext on disk. Encryption is fixed when a data directory is first\n")
		h.printf("started: to encrypt, back up with 'hoarddb dump', start a fresh data directory with\n")
		h.printf("encryption.mode: require and a keystore, then restore. See \"Encryption at rest\" in\n")
		h.printf("the configuration page of the user docs.\n")
	default:
		h.printf("\nNot sealed, whatever the mode:\n")
		for _, entry := range report.NotSealed {
			h.printf("  - %s\n", terminalSafeText(entry))
		}
	}
}
