package server

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-server/transport"
)

// restoreSentinelCodes is the wire class each restore sentinel answers, with
// the retry rule that class carries in clients/handbook/errors.md section 6.
var restoreSentinelCodes = map[string]struct {
	err  error
	want transport.ErrorCode
}{
	// The caller changes the request: the same request fails the same way.
	"ErrRestoreFormatVersion":     {ErrRestoreFormatVersion, transport.ErrInvalidArgument},
	"ErrRestorePathEscapesRoot":   {ErrRestorePathEscapesRoot, transport.ErrInvalidArgument},
	"ErrRestoreStoreTypeMismatch": {ErrRestoreStoreTypeMismatch, transport.ErrInvalidArgument},
	"ErrRestoreDropBlobDataLoss":  {ErrRestoreDropBlobDataLoss, transport.ErrInvalidArgument},
	"ErrRestoreNoActiveSession":   {ErrRestoreNoActiveSession, transport.ErrInvalidArgument},
	"ErrRestoreReservedName":      {ErrRestoreReservedName, transport.ErrInvalidArgument},
	// Transient: the dump or the other restore ends, the disk frees, and the
	// start was refused before anything was written, so a backoff retry is safe.
	"ErrRestoreBucketBeingDumped":     {ErrRestoreBucketBeingDumped, transport.ErrUnavailable},
	"ErrRestoreBucketConflict":        {ErrRestoreBucketConflict, transport.ErrUnavailable},
	"ErrRestoreInsufficientDiskSpace": {ErrRestoreInsufficientDiskSpace, transport.ErrUnavailable},
}

// TestEveryRestoreSentinelAnswersItsOwnClass is BUG-0678: codeFor had no case
// for seven of the restore sentinels, so a driver got INTERNAL ("report a
// server bug, never retry") for a refusal it could act on.
func TestEveryRestoreSentinelAnswersItsOwnClass(t *testing.T) {
	for name, row := range restoreSentinelCodes {
		t.Run(name, func(t *testing.T) {
			wrapped := fmt.Errorf("restoring bucket d/b: %w: detail", row.err)
			if got := codeFor(wrapped); got != row.want {
				t.Errorf("%s answers code %d, want %d", name, got, row.want)
			}
		})
	}
}

// TestRestoreSentinelTableCoversRestoreGo fails when restore.go declares an
// Err sentinel the table above does not list, so a new one cannot ship
// unclassified.
func TestRestoreSentinelTableCoversRestoreGo(t *testing.T) {
	parsed, err := parser.ParseFile(token.NewFileSet(), "restore.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing restore.go: %v", err)
	}
	declared := map[string]bool{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		if spec, ok := node.(*ast.ValueSpec); ok {
			for _, ident := range spec.Names {
				if strings.HasPrefix(ident.Name, "ErrRestore") {
					declared[ident.Name] = true
				}
			}
		}
		return true
	})
	for name := range declared {
		if _, listed := restoreSentinelCodes[name]; !listed {
			t.Errorf("restore.go declares %s, which restoreSentinelCodes does not classify", name)
		}
	}
	if len(declared) != len(restoreSentinelCodes) {
		t.Errorf("restore.go declares %d sentinels, the table lists %d", len(declared), len(restoreSentinelCodes))
	}
}
