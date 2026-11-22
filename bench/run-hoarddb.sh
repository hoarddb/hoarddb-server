#!/bin/bash
# Run the multi-scale benchmark for hoardDB only.
# Default scales: 1K, 100K, 1M, 10M records, batch mode (1000 docs/call).

# Override via env: BENCH, SCALES, MODES, LOG.
BENCH=${BENCH:-/tmp/hoarddb-bench}
SCALES=${SCALES:-"1000 100000 1000000 10000000"}
MODES=${MODES:-batch}
LOG=${LOG:-/tmp/hoarddb-bench-$(date +%Y%m%d-%H%M%S).log}

if [ ! -x "$BENCH" ]; then
    echo "error: benchmark binary not executable at: $BENCH" >&2
    echo "  build it first:  go build -o $BENCH ./bench" >&2
    exit 1
fi

: > "$LOG"
failed=0
legs=0

echo "HoardDB Multi-Scale Benchmark"
echo "Scales: $SCALES | Modes: $MODES"
echo "Log: $LOG"
echo ""

for mode in $MODES; do
    for scale in $SCALES; do
        legs=$((legs + 1))
        start=$(date +%s)
        echo "=== [$(date -u +%H:%M:%S)] -db=hoarddb -mode=$mode -records=$scale ===" >> "$LOG"
        echo "[$legs] hoarddb $mode $scale ..."
        "$BENCH" -db="hoarddb" -mode="$mode" -records="$scale" >> "$LOG" 2>&1
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

echo "" >> "$LOG"
echo "ALL DONE [$(date -u +%H:%M:%S)] - $legs legs, $failed failed" >> "$LOG"
echo "" >> "$LOG"
echo "============================================"
if [ $failed -eq 0 ]; then
    echo "Benchmark complete: $legs/$legs legs passed"
else
    echo "Benchmark complete: $failed of $legs legs FAILED"
fi
echo "Results: $LOG"
echo "============================================"

exit $(( failed > 0 ? 1 : 0 ))
