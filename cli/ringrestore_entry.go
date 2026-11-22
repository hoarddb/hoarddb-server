package cli

import "os"

// restoreEntry is the verb entry of `hoarddb restore`. A --verify among the arguments runs
// the file check, which needs no server; anything else is the ring restore. The legacy
// RunRestoreCommand stays for the legacy engine and is not reached from here.
func restoreEntry(args []string) int {
	if wantsRestoreVerify(args) {
		return runRestoreVerify(args, os.Stdout, os.Stderr)
	}
	return RunRingRestoreCommand(args)
}

// wantsRestoreVerify reports whether args ask for the file check: a -verify or --verify
// before any "--". After the terminator the flag package reads every word as a positional
// one, so a file that happens to be named --verify is a path, not the flag.
func wantsRestoreVerify(args []string) bool {
	for _, arg := range args {
		if arg == "--" {
			return false
		}
		if arg == "-verify" || arg == "--verify" {
			return true
		}
	}
	return false
}
