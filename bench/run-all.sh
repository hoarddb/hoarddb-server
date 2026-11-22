#!/bin/bash
# Run the multi-scale benchmark across all three databases.
# Default scales: 1K, 100K, 1M, 10M records, batch mode (1000 docs/call).
#
# Deliberately NOT `set -e`. The 10M legs dominate wall time (~14 min for
# hoardDB and MongoDB, ~25 min for MySQL), and a single failing leg must not
# silently discard the remaining ones — that is precisely how the QUIC
# idle-timeout bug hid the 10M result. Every leg is timestamped, its exit code
# is recorded, and the script exits non-zero if any leg failed.
#
# Override via env: BENCH, SCALES, MODES, DBS, LOG. Example:
#   SCALES="100000" BENCH=/tmp/hoarddb-bench bash bench/run-all.sh

BENCH=${BENCH:-/tmp/hoarddb-bench}
SCALES=${SCALES:-"1000 100000 1000000 10000000"}
MODES=${MODES:-batch}
# DBS adds engines to the default three; "cockroach" needs HOARDDB_BENCH_COCKROACH_DSN.
# This script drives ./bench. The social harness (bench/social) also takes -engine ring (HARNESS.md, "Ring leg"); ./bench does not.
DBS=${DBS:-"hoarddb mongo mysql"}
LOG=${LOG:-/tmp/hoarddb-bench-$(date +%Y%m%d-%H%M%S).log}

if [ ! -x "$BENCH" ]; then
    echo "error: benchmark binary not executable at: $BENCH" >&2
    echo "  build it first:  go build -o $BENCH ./bench" >&2
    exit 1
fi

: > "$LOG"
failed=0
legs=0

echo "Multi-Scale Benchmark: hoardDB vs MongoDB vs MySQL"
echo "Scales: $SCALES | Modes: $MODES"
echo "Log: $LOG"
echo ""

for mode in $MODES; do
    for scale in $SCALES; do
        for db in $DBS; do
            legs=$((legs + 1))
            start=$(date +%s)
            echo "=== [$(date -u +%H:%M:%S)] -db=$db -mode=$mode -records=$scale ===" >> "$LOG"
            echo "[$legs] $db $mode $scale ..."
            "$BENCH" -db="$db" -mode="$mode" -records="$scale" >> "$LOG" 2>&1
            rc=$?
            took=$(( $(date +%s) - start ))
            if [ $rc -eq 0 ]; then
                echo "--- [$(date -u +%H:%M:%S)] exit 0 (leg took ${took}s)" >> "$LOG"
            else
                failed=$((failed + 1))
                echo "--- [$(date -u +%H:%M:%S)] exit $rc FAILED (leg took ${took}s)" >> "$LOG"
                echo "    FAILED (exit $rc) after ${took}s"
            fi
            echo "" >> "$LOG"
        done
    done
done

echo "" >> "$LOG"
echo "ALL DONE [$(date -u +%H:%M:%S)] — $legs legs, $failed failed" >> "$LOG"

echo ""
echo "============================================"
if [ $failed -eq 0 ]; then
    echo "Benchmark complete: $legs/$legs legs passed"
else
    echo "Benchmark complete: $failed of $legs legs FAILED"
fi
echo "Results: $LOG"
echo "============================================"

exit $(( failed > 0 ? 1 : 0 ))
