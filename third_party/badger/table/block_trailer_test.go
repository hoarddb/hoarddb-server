/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package table

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/fb"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/badger/v4/y"
)

// damageMasks are XORed into one byte at a time: every bit, and one bit low,
// middle and high.
var damageMasks = []byte{0xff, 0x01, 0x02, 0x10, 0x80}

// everyMask is all 255 masks. An uncompressed, unencrypted block stores its
// trailer as it is, so its last trailerSweepBytes bytes (the checksum, its
// length, the entry count and the last entry offsets) are damaged with every
// mask: a checksum turns into one naming an unknown algorithm only for a few
// particular masks, and y.CalculateChecksum panics on that.
var everyMask = func() []byte {
	masks := make([]byte, 0, 255)
	for mask := 1; mask <= 255; mask++ {
		masks = append(masks, byte(mask))
	}
	return masks
}()

const trailerSweepBytes = 48

// TestEveryDamagedBlockByteIsAnErrorNotAPanic damages each byte of every block
// of a table in turn, with each of damageMasks, and loads the block. Every
// load must return an error. A block ends in its entry offsets, its entry
// count, its checksum and the checksum's length; a damaged count or length
// used to drive a slice out of range before the checksum was compared, and a
// damaged checksum could name an algorithm that y.CalculateChecksum panics
// on. Either panicked on a Get's goroutine and, during a compaction, ended
// the process.
//
// A damaged copy may also load without error when it decodes to the very
// bytes the sound block does: a compressed copy whose back-reference now
// points at an equal run of bytes, or a changed zstd header flag. That is
// not damage to the data, so the sweep accepts it; any other load without
// an error fails the test.
func TestEveryDamagedBlockByteIsAnErrorNotAPanic(t *testing.T) {
	encryptionKey := make([]byte, 32)
	for index := range encryptionKey {
		encryptionKey[index] = byte(index)
	}
	cases := []struct {
		name        string
		compression options.CompressionType
		dataKey     *pb.DataKey
	}{
		{name: "plain", compression: options.None},
		{name: "snappy", compression: options.Snappy},
		{name: "zstd", compression: options.ZSTD},
		{name: "snappy-encrypted", compression: options.Snappy, dataKey: &pb.DataKey{Data: encryptionKey, KeyId: 1}},
	}
	indexCache, err := ristretto.NewCache(&ristretto.Config[uint64, *fb.TableIndex]{
		NumCounters: 1000,
		MaxCost:     1 << 20,
		BufferItems: 64,
	})
	require.NoError(t, err)
	defer indexCache.Close()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				Compression:          tc.compression,
				ZSTDCompressionLevel: 1,
				BlockSize:            1024,
				BloomFalsePositive:   0.01,
				ChkMode:              options.OnBlockRead,
				DataKey:              tc.dataKey,
			}
			if tc.dataKey != nil {
				opts.IndexCache = indexCache
			}
			tbl := buildTestTable(t, "k", 400, opts)
			defer func() { require.NoError(t, tbl.DecrRef()) }()
			sweepEveryBlockByte(t, tbl, tc.compression == options.None && tc.dataKey == nil)
		})
	}
}

// TestADamagedEntryCountWithoutVerificationIsAnErrorNotAPanic covers the
// bounds check on a block's entry count, which the checksum comparison makes
// unreachable in the verifying modes: with options.NoVerification nothing
// compares the checksum first, so a damaged count reaches the slice that
// holds the entry offsets. Each of its four bytes is damaged with every mask;
// no load may panic, and a count that cannot fit in the block must be an
// error. A smaller damaged count loads without error, because nothing
// verifies the block in this mode.
func TestADamagedEntryCountWithoutVerificationIsAnErrorNotAPanic(t *testing.T) {
	opts := Options{
		Compression:        options.None,
		BlockSize:          1024,
		BloomFalsePositive: 0.01,
		ChkMode:            options.NoVerification,
	}
	tbl := buildTestTable(t, "k", 400, opts)
	defer func() { require.NoError(t, tbl.DecrRef()) }()
	var loads, errored int
	for blockIndex := 0; blockIndex < tbl.offsetsLength(); blockIndex++ {
		var blockOffset fb.BlockOffset
		require.True(t, tbl.offsets(&blockOffset, blockIndex))
		blockEnd := int(blockOffset.Offset()) + int(blockOffset.Len())
		checksumLength := int(y.BytesToU32(tbl.Data[blockEnd-4 : blockEnd]))
		countStart := blockEnd - 4 - checksumLength - 4
		for position := countStart; position < countStart+4; position++ {
			for _, mask := range everyMask {
				loads++
				tbl.Data[position] ^= mask
				_, loadErr, panicked := loadBlockRecovering(tbl, blockIndex)
				tbl.Data[position] ^= mask
				require.Nil(t, panicked, "block %d, count byte %d XOR %#02x", blockIndex, position-countStart, mask)
				if loadErr != nil {
					errored++
				}
			}
		}
	}
	require.Positive(t, loads)
	require.Positive(t, errored, "no damaged count was refused")
}

func sweepEveryBlockByte(t *testing.T, tbl *Table, trailerInTheClear bool) {
	var positions, panics, silent int
	for blockIndex := 0; blockIndex < tbl.offsetsLength(); blockIndex++ {
		var blockOffset fb.BlockOffset
		require.True(t, tbl.offsets(&blockOffset, blockIndex))
		start := int(blockOffset.Offset())
		end := start + int(blockOffset.Len())
		soundBlock, loadErr, panicked := loadBlockRecovering(tbl, blockIndex)
		require.Nil(t, panicked)
		require.NoError(t, loadErr)
		for position := start; position < end; position++ {
			masks := damageMasks
			if trailerInTheClear && position >= end-trailerSweepBytes {
				masks = everyMask
			}
			for _, mask := range masks {
				positions++
				tbl.Data[position] ^= mask
				damagedBlock, loadErr, panicked := loadBlockRecovering(tbl, blockIndex)
				tbl.Data[position] ^= mask
				switch {
				case panicked != nil:
					panics++
					if panics <= 3 {
						t.Errorf("block %d, byte %d of [%d, %d) XOR %#02x: the load panicked: %v",
							blockIndex, position, start, end, mask, panicked)
					}
				case loadErr == nil && !bytes.Equal(damagedBlock, soundBlock):
					silent++
					if silent <= 3 {
						t.Errorf("block %d, byte %d of [%d, %d) XOR %#02x: the load returned no error",
							blockIndex, position, start, end, mask)
					}
				}
			}
		}
	}
	require.Positive(t, positions)
	require.Zero(t, panics, "%d of %d damaged copies panicked", panics, positions)
	require.Zero(t, silent, "%d of %d damaged copies loaded with no error", silent, positions)
}

// loadBlockRecovering loads a block and returns a copy of its decoded bytes,
// or the error, or what the load panicked with.
func loadBlockRecovering(tbl *Table, blockIndex int) (decoded []byte, loadErr error, panicked any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = fmt.Sprint(recovered)
		}
	}()
	blk, loadErr := tbl.block(blockIndex, false)
	if loadErr != nil {
		return nil, loadErr, nil
	}
	decoded = append([]byte(nil), blk.data...)
	blk.decrRef()
	return decoded, nil, nil
}
