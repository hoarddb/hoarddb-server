package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4/table"
)

// DamagedReadError is a read that reached a table block of the node store that
// cannot be read: it fails its checksum, does not decompress or decrypt, or
// cannot be read from the disk. Upstream Badger answers a point read of such a
// block "key not found" and ends a walk at it as though the data ended there;
// the patched copy in third_party/badger returns the error, and
// this is how the store hands it on.
type DamagedReadError struct {
	Dir   string // the store directory
	Cause error  // Badger's error, which names the table file
}

// Error starts Badger's text at the block failure: what wraps it in a point
// read is the storage key in Badger's quoting, an internal encoding a client
// never sent and cannot act on.
func (e *DamagedReadError) Error() string {
	detail := e.Cause.Error()
	if at := strings.Index(detail, table.ErrBlockRead.Error()); at >= 0 {
		detail = detail[at:]
	}
	return fmt.Sprintf("a file of the node store at %s is damaged, so the read failed and returned nothing (%s). "+
		"Reads of the damaged part keep failing rather than answer wrongly%s", e.Dir, detail, withRepairAdvice(e.Dir))
}

func (e *DamagedReadError) Unwrap() error { return e.Cause }

// storeRepairAdvice is what an operator does with a damaged node store, for
// a refused open and a failed read alike.
func storeRepairAdvice(dir string) string {
	return fmt.Sprintf("Stop this node and move %s aside (keep it: its table files may be the only copy of their data). "+
		"Then restore this node from a backup (see the backup-restore page of the user docs), or, if its buckets have replicas on other nodes, "+
		"remove the node ('remove-node <node-id>'), start it with an empty data directory and add it again, so it is seeded from them", dir)
}

// repairAdvice holds, by store directory, the advice of a ring store's runtime refusals (a damaged
// read, the write refusal and the close of a store stalled behind a damaged table): what is safe
// depends on whether the node is a cluster member, so the vouched open registers none and the ring
// node sets its own (BadgerKV.SetRepairAdvice; review R2 F-4). A directory not in it is a legacy
// store's, which gets storeRepairAdvice. One entry per open ring store, removed at its close.
var repairAdvice sync.Map

// withRepairAdvice returns ". " and the repair advice for the store in dir, or nothing for a ring
// store whose node has set none.
func withRepairAdvice(dir string) string {
	advice := storeRepairAdvice(dir)
	if registered, ok := repairAdvice.Load(dir); ok {
		advice = registered.(string)
	}
	if advice == "" {
		return ""
	}
	return ". " + advice
}

// SetRepairAdvice sets the advice the ring store's runtime refusals end in: its node's advice for
// a member or a single node. It is for a store OpenVouchedBadgerKV opened.
func (kv *BadgerKV) SetRepairAdvice(advice string) {
	repairAdvice.Store(kv.dir, advice)
}

// withoutRepairAdvice marks a refusal of the ring node's store as one whose advice the caller
// adds: what is safe to do with a damaged ring store depends on whether the node is a cluster
// member, and storage does not know (store-vouched-cut review R-1). A refusal with a repair of
// its own (Repair set) keeps it; any other error is returned as it is.
func withoutRepairAdvice(err error) error {
	var damaged *StoreDamagedError
	if errors.As(err, &damaged) && damaged.Repair == "" {
		damaged.AdviceByCaller = true
	}
	return err
}

// IsVouchedOpenRefusal reports whether err is a refusal of OpenVouchedBadgerKV that names the
// state of the store and leaves the advice to the caller: damage, a marker of another format or
// no marker beside data files. Other errors (the disk, a held lock) are not.
func IsVouchedOpenRefusal(err error) bool {
	var damaged *StoreDamagedError
	var markerDamaged *VouchedMarkerDamagedError
	var markerFormat *VouchedMarkerFormatError
	var markerMissing *VouchedMarkerMissingError
	return errors.As(err, &damaged) || errors.As(err, &markerDamaged) || errors.As(err, &markerFormat) ||
		errors.As(err, &markerMissing)
}

var (
	damagedReads      atomic.Int64
	damagedReadLogged atomic.Bool
)

// DamagedReads is the number of reads, point reads and walks, that reached a
// damaged table block since the process started. The server exports it as
// hoarddb_store_damaged_reads_total. It is one node-wide number: which bucket
// a read was for is in the error the read returns, never in a metric label.
func DamagedReads() int64 {
	return damagedReads.Load()
}

// blockUnreadable reports whether a Badger read failed on a table block it
// could not read. Badger's y.Wrap flattens the errors of a point read with
// %+v, so errors.Is cannot see table.ErrBlockRead through one; the text is
// matched, as badgerChecksumMismatch does, and
// TestACorruptTableBlockIsAnErrorNotAMiss pins it.
func blockUnreadable(err error) bool {
	return strings.Contains(err.Error(), table.ErrBlockRead.Error())
}

// damagedRead turns a Badger read error that reached a damaged block into a
// DamagedReadError, counts it, and logs the first one this process sees; the
// rest are counted only, so a client retrying a damaged key cannot fill the
// log. Any other error is returned as it is.
func damagedRead(dir string, readErr error) error {
	if !blockUnreadable(readErr) {
		return readErr
	}
	damaged := &DamagedReadError{Dir: dir, Cause: readErr}
	damagedReads.Add(1)
	if damagedReadLogged.CompareAndSwap(false, true) {
		slog.Error("the node store is damaged: a read reached a table block that cannot be read. "+
			"Later damaged reads are counted in hoarddb_store_damaged_reads_total and not logged",
			"error", damaged)
	}
	return damaged
}
