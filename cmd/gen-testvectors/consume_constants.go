package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hoarddb/hoarddb-server/store"
)

// constantsFileName is the file beside v1.json that holds the numbers a driver's
// consume loop needs (claim-ack-spec 13.4): the server's bounds, which come from
// the store's exported constants so they cannot drift, and the loop's own backoff
// and margin, which the driver owns and the generator states once for all three.
const constantsFileName = "consume-constants.json"

// consumeConstants is the content of constantsFileName.
type consumeConstants struct {
	Format  string `json:"format"`
	License string `json:"license"`

	// The server's, read from the store's constants.
	ClaimMax       int   `json:"claim_max"`
	LeaseMsMin     int64 `json:"lease_ms_min"`
	LeaseMsDefault int64 `json:"lease_ms_default"`
	LeaseMsMax     int64 `json:"lease_ms_max"`
	MaxDeliveries  int   `json:"max_deliveries"`
	ItemIDsMax     int   `json:"item_ids_max"`

	// The consume loop's (claim-ack-spec 13.2).
	ConsumeBackoffFloorMs   int64   `json:"consume_backoff_floor_ms"`
	ConsumeBackoffCeilingMs int64   `json:"consume_backoff_ceiling_ms"`
	ConsumeBackoff          string  `json:"consume_backoff"`
	ConsumeMarginMinMs      int64   `json:"consume_margin_min_ms"`
	ConsumeMarginFraction   float64 `json:"consume_margin_fraction"`
}

// currentConsumeConstants is the file's content at this commit.
func currentConsumeConstants() consumeConstants {
	return consumeConstants{
		Format:                  "hoarddb-consume-constants/1",
		License:                 "Apache-2.0",
		ClaimMax:                store.MaxClaimLimit,
		LeaseMsMin:              store.MinLeaseMs,
		LeaseMsDefault:          store.DefaultLeaseMs,
		LeaseMsMax:              store.MaxLeaseMs,
		MaxDeliveries:           store.MaxDeliveries,
		ItemIDsMax:              store.MaxItemIDsPerAck,
		ConsumeBackoffFloorMs:   100,
		ConsumeBackoffCeilingMs: 5000,
		ConsumeBackoff:          "full_jitter_doubling",
		ConsumeMarginMinMs:      250,
		ConsumeMarginFraction:   0.1,
	}
}

// encodeConsumeConstants is the file's bytes: indented, newline-terminated, so
// two generations compare byte for byte.
func encodeConsumeConstants() ([]byte, error) {
	encoded, err := json.MarshalIndent(currentConsumeConstants(), "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encoding the consume constants: %w", err)
	}
	return append(encoded, '\n'), nil
}

// writeConsumeConstants writes the constants file into dir.
func writeConsumeConstants(dir string) (string, error) {
	encoded, err := encodeConsumeConstants()
	if err != nil {
		return "", err
	}
	target := filepath.Join(dir, constantsFileName)
	if err := os.WriteFile(target, encoded, 0o644); err != nil {
		return "", fmt.Errorf("writing %s: %w", target, err)
	}
	return target, nil
}

// checkConsumeConstants compares the constants file beside vectorPath with the
// current content. It returns nil when they are byte for byte the same.
func checkConsumeConstants(vectorPath string) error {
	target := filepath.Join(filepath.Dir(vectorPath), constantsFileName)
	committed, err := os.ReadFile(target)
	if err != nil {
		return fmt.Errorf("reading the committed consume constants: %w (regenerate with 'make vectors')", err)
	}
	current, err := encodeConsumeConstants()
	if err != nil {
		return err
	}
	if !bytes.Equal(committed, current) {
		return fmt.Errorf("%s does not match the server at this commit; regenerate with 'make vectors' and review the diff: a bound that changed is a change to every driver", target)
	}
	return nil
}
