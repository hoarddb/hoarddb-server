/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Added by the hoardDB project, 2026-10-26. See hoardDB/third_party/README.md.
 */

package table

import (
	"crypto/rand"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/y"
)

// syncCall is one call CreateTable made through tableSyncCalls.
type syncCall struct {
	kind   string // "msync", "start" or "wait"
	fd     uintptr
	offset int64
	length int64
}

// syncRecorder stands in for the platform's sync calls and records them in
// order. startErr, when set, is what startWriteback returns: on every call,
// or from call number failFromStart (1-based) on when that is set. waitErr is
// what waitWriteback returns. beforeStartErr, when set, runs just before a
// startWriteback returns startErr.
type syncRecorder struct {
	calls          []syncCall
	startErr       error
	failFromStart  int
	waitErr        error
	beforeStartErr func()
	startCount     int
}

func (r *syncRecorder) tableSyncCalls(rangedWriteback bool) tableSyncCalls {
	calls := tableSyncCalls{
		msync: func(mapped []byte) error {
			r.calls = append(r.calls, syncCall{kind: "msync", length: int64(len(mapped))})
			return nil
		},
	}
	if !rangedWriteback {
		return calls
	}
	calls.startWriteback = func(fd uintptr, offset, length int64) error {
		r.calls = append(r.calls, syncCall{kind: "start", fd: fd, offset: offset, length: length})
		r.startCount++
		if r.startErr == nil || r.startCount < r.failFromStart {
			return nil
		}
		if r.beforeStartErr != nil {
			r.beforeStartErr()
		}
		return r.startErr
	}
	calls.waitWriteback = func(fd uintptr, offset, length int64) error {
		r.calls = append(r.calls, syncCall{kind: "wait", fd: fd, offset: offset, length: length})
		return r.waitErr
	}
	return calls
}

// recordTableSync replaces the package's sync calls with recorder's for the
// rest of the test.
func recordTableSync(t *testing.T, recorder *syncRecorder, rangedWriteback bool) {
	saved := tableSync
	savedRefused := rangedWritebackRefused.Load()
	tableSync = recorder.tableSyncCalls(rangedWriteback)
	rangedWritebackRefused.Store(false)
	t.Cleanup(func() {
		tableSync = saved
		rangedWritebackRefused.Store(savedRefused)
	})
}

// createTableOfMiB builds and creates a table of about sizeMiB MiB of
// incompressible values in dir, under opts.
func createTableOfMiB(t *testing.T, dir string, sizeMiB int, opts Options) (*Table, error) {
	opts.BlockSize = 4 << 10
	opts.Compression = options.None
	opts.BloomFalsePositive = 0.01
	builder := NewTableBuilder(opts)
	defer builder.Close()
	const valueBytes = 1000
	for i := 0; i < sizeMiB<<20/valueBytes; i++ {
		randomValue := make([]byte, valueBytes)
		_, err := rand.Read(randomValue)
		require.NoError(t, err)
		builder.Add(y.KeyWithTs([]byte(fmt.Sprintf("key-%09d", i)), 0),
			y.ValueStruct{Value: randomValue}, 0)
	}
	return CreateTable(NewFilename(1, dir), builder)
}

// TestTableWritebackIsStepwiseAndWaitedBeforeCreateTableReturns checks
// options.TableSyncStepwise: a new table is never synced with one whole-file msync
// (on ext4 each waits for a journal commit and stalls the writes behind it);
// it is written back in ranges no larger than the step, the ranges cover the
// file once in order, and one wait over the whole file is the last call, made
// before CreateTable returns, so the table is on the device before the
// MANIFEST can name it.
func TestTableWritebackIsStepwiseAndWaitedBeforeCreateTableReturns(t *testing.T) {
	tests := []struct {
		name         string
		stepSize     int
		wantStepSize int64
	}{
		{"default step of 4 MiB", 0, DefaultTableSyncStepSize},
		{"configured 1 MiB step", 1 << 20, 1 << 20},
		{"configured 256 KiB step", 256 << 10, 256 << 10},
		{"step larger than the table", 64 << 20, 64 << 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := &syncRecorder{}
			recordTableSync(t, recorder, true)

			tbl, err := createTableOfMiB(t, t.TempDir(), 9, Options{SyncMode: options.TableSyncStepwise, SyncStepSize: tt.stepSize})
			require.NoError(t, err)
			defer func() { require.NoError(t, tbl.DecrRef()) }()

			tableBytes := int64(len(tbl.Data))
			require.NotZero(t, tableBytes%(256<<10), "the table must not end on a step boundary, or the last short step goes untested")
			require.NotEmpty(t, recorder.calls)
			for i, call := range recorder.calls {
				require.NotEqual(t, "msync", call.kind, "call %d is a whole-file blocking msync: %+v", i, recorder.calls)
			}

			var nextOffset int64
			for i, call := range recorder.calls[:len(recorder.calls)-1] {
				require.Equal(t, "start", call.kind, "call %d: only the last call may wait: %+v", i, recorder.calls)
				require.Equal(t, tbl.Fd.Fd(), call.fd, "call %d is not on the table's file", i)
				require.Equal(t, nextOffset, call.offset, "call %d leaves a gap or overlaps", i)
				require.LessOrEqual(t, call.length, tt.wantStepSize, "call %d writes back more than one step", i)
				require.Positive(t, call.length, "call %d is empty (a zero length means to the end of the file)", i)
				nextOffset += call.length
			}
			require.Equal(t, tableBytes, nextOffset, "the steps do not cover the whole table")

			finalWait := recorder.calls[len(recorder.calls)-1]
			require.Equal(t, syncCall{kind: "wait", fd: tbl.Fd.Fd(), offset: 0, length: tableBytes}, finalWait,
				"the last call must wait for the whole table")
		})
	}
}

// TestTableWritebackFallsBackToMsyncWithoutRangedWriteback checks the
// platforms without ranged writeback (every build but Linux, whose
// platformTableSync has no startWriteback): the stepwise mode syncs the table
// with upstream's one msync, so it is durable there too.
func TestTableWritebackFallsBackToMsyncWithoutRangedWriteback(t *testing.T) {
	recorder := &syncRecorder{}
	recordTableSync(t, recorder, false)

	tbl, err := createTableOfMiB(t, t.TempDir(), 2, Options{SyncMode: options.TableSyncStepwise})
	require.NoError(t, err)
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	require.Equal(t, []syncCall{{kind: "msync", length: int64(len(tbl.Data))}}, recorder.calls)
}

// TestTableWritebackModeMsyncIsUpstreamsOneSync checks that the old behaviour
// stays selectable: options.TableSyncMsync is one whole-file msync and no
// ranged writeback, even where ranged writeback exists.
func TestTableWritebackModeMsyncIsUpstreamsOneSync(t *testing.T) {
	recorder := &syncRecorder{}
	recordTableSync(t, recorder, true)

	tbl, err := createTableOfMiB(t, t.TempDir(), 2, Options{SyncMode: options.TableSyncMsync})
	require.NoError(t, err)
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	require.Equal(t, []syncCall{{kind: "msync", length: int64(len(tbl.Data))}}, recorder.calls)
}

// TestTableWritebackZeroOptionsIsUpstreamsOneSync checks that the stepwise
// writeback is opt-in at the table level too: Options with no SyncMode set (a
// caller that builds its own, as Badger's own tests and tools do) syncs a new
// table with upstream's one whole-file msync even where ranged writeback
// exists. Stepwise is safe only on a filesystem that commits its journal in
// order, which nothing here can check.
func TestTableWritebackZeroOptionsIsUpstreamsOneSync(t *testing.T) {
	recorder := &syncRecorder{}
	recordTableSync(t, recorder, true)

	tbl, err := createTableOfMiB(t, t.TempDir(), 2, Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, tbl.DecrRef()) }()

	require.Equal(t, []syncCall{{kind: "msync", length: int64(len(tbl.Data))}}, recorder.calls)
}

// TestTableWritebackFailureIsAnError checks that a writeback the kernel
// refuses fails CreateTable with the cause, so the flush or compaction fails
// and the MANIFEST never names the table; and that a kernel without the call
// (ENOSYS) falls back to the msync instead.
func TestTableWritebackFailureIsAnError(t *testing.T) {
	t.Run("EIO fails the table", func(t *testing.T) {
		recorder := &syncRecorder{startErr: syscall.EIO}
		recordTableSync(t, recorder, true)

		_, err := createTableOfMiB(t, t.TempDir(), 2, Options{SyncMode: options.TableSyncStepwise})
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.EIO), "the cause is lost: %v", err)
		for _, call := range recorder.calls {
			require.NotEqual(t, "msync", call.kind, "a failed writeback must not be papered over: %+v", recorder.calls)
		}
	})
	t.Run("EIO on a later step fails the table and names the setting", func(t *testing.T) {
		// EIO means the pages may not be on the device, and the kernel has
		// handed that error to this call: an msync after it could report
		// success. Never fall back on it.
		recorder := &syncRecorder{startErr: fmt.Errorf("sync_file_range: %w", syscall.EIO), failFromStart: 2}
		recordTableSync(t, recorder, true)

		_, err := createTableOfMiB(t, t.TempDir(), 9, Options{SyncMode: options.TableSyncStepwise, SyncStepSize: 1 << 20})
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.EIO), "the cause is lost: %v", err)
		require.Contains(t, err.Error(), "storage.badger_table_sync", "the error does not name the setting")
		require.Contains(t, err.Error(), "msync", "the error does not name the value that avoids sync_file_range")
		for _, call := range recorder.calls {
			require.NotEqual(t, "msync", call.kind, "a failed writeback must not be papered over: %+v", recorder.calls)
		}
	})
	t.Run("ENOSYS falls back to msync", func(t *testing.T) {
		// As the Linux startWriteback reports a kernel without the call.
		recorder := &syncRecorder{startErr: fmt.Errorf("sync_file_range: %w", syscall.ENOSYS)}
		recordTableSync(t, recorder, true)

		tbl, err := createTableOfMiB(t, t.TempDir(), 2, Options{SyncMode: options.TableSyncStepwise})
		require.NoError(t, err)
		defer func() { require.NoError(t, tbl.DecrRef()) }()
		finalCall := recorder.calls[len(recorder.calls)-1]
		require.Equal(t, syncCall{kind: "msync", length: int64(len(tbl.Data))}, finalCall)
	})
}

// TestTableWritebackRefusedFallsBackToMsync checks the kernels and sandboxes
// that refuse sync_file_range outright: a seccomp profile that filters it
// answers EPERM, a kernel or filesystem without it EINVAL or EOPNOTSUPP. The
// old msync worked there, so the table must be synced with it and CreateTable
// succeed, wherever in the table the refusal comes. Each is returned before
// the kernel looks at the file's writeback error, so the msync cannot hide one.
func TestTableWritebackRefusedFallsBackToMsync(t *testing.T) {
	refusals := []syscall.Errno{syscall.EPERM, syscall.EINVAL, syscall.EOPNOTSUPP, syscall.ENOSYS}
	places := []struct {
		name     string
		recorder func(refusal error) *syncRecorder
	}{
		{"first step", func(refusal error) *syncRecorder { return &syncRecorder{startErr: refusal} }},
		{"later step", func(refusal error) *syncRecorder { return &syncRecorder{startErr: refusal, failFromStart: 3} }},
		{"final wait", func(refusal error) *syncRecorder { return &syncRecorder{waitErr: refusal} }},
	}
	for _, refusal := range refusals {
		for _, place := range places {
			t.Run(fmt.Sprintf("%s on the %s", refusal, place.name), func(t *testing.T) {
				// As table_sync_linux.go reports what the kernel answered.
				recorder := place.recorder(fmt.Errorf("sync_file_range: %w", refusal))
				recordTableSync(t, recorder, true)

				tbl, err := createTableOfMiB(t, t.TempDir(), 9, Options{SyncMode: options.TableSyncStepwise, SyncStepSize: 1 << 20})
				require.NoError(t, err, "a refused sync_file_range must fall back to the msync")
				defer func() { require.NoError(t, tbl.DecrRef()) }()

				finalCall := recorder.calls[len(recorder.calls)-1]
				require.Equal(t, syncCall{kind: "msync", length: int64(len(tbl.Data))}, finalCall,
					"the table is not synced with a whole-file msync after the refusal: %+v", recorder.calls)
				msyncs := 0
				for _, call := range recorder.calls {
					if call.kind == "msync" {
						msyncs++
					}
				}
				require.Equal(t, 1, msyncs, "one msync, after the refusal: %+v", recorder.calls)
			})
		}
	}
}

// TestTableWritebackRefusalIsRememberedForLaterTables checks that once the
// kernel refused sync_file_range, later tables go straight to the msync: the
// refusal is a property of the kernel or the sandbox, and asking again for
// every table is a wasted system call per flush and per compaction output.
func TestTableWritebackRefusalIsRememberedForLaterTables(t *testing.T) {
	recorder := &syncRecorder{startErr: fmt.Errorf("sync_file_range: %w", syscall.EPERM)}
	recordTableSync(t, recorder, true)
	dir := t.TempDir()

	opts := Options{SyncMode: options.TableSyncStepwise, SyncStepSize: 1 << 20, BlockSize: 4 << 10, Compression: options.None, BloomFalsePositive: 0.01}
	for tableID := uint64(1); tableID <= 2; tableID++ {
		recorder.calls = nil
		builder := NewTableBuilder(opts)
		builder.Add(y.KeyWithTs([]byte("key"), 0), y.ValueStruct{Value: []byte("value")}, 0)
		tbl, err := CreateTable(NewFilename(tableID, dir), builder)
		builder.Close()
		require.NoError(t, err, "table %d", tableID)
		require.Equal(t, "msync", recorder.calls[len(recorder.calls)-1].kind, "table %d: %+v", tableID, recorder.calls)
		if tableID == 2 {
			require.Equal(t, []syncCall{{kind: "msync", length: int64(len(tbl.Data))}}, recorder.calls,
				"the second table asked for sync_file_range again after the kernel refused it")
		}
		require.NoError(t, tbl.DecrRef())
	}
}
