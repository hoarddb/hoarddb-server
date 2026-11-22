#!/usr/bin/env bash
# capture.sh - captures the first v1.0-written BSON corpus (bson-codec-spec.md
# 9.1.6, bson-codec-build.md step 1d) into records.jsonl.gz and stats.json.
#
# Run from hoardDB/ (the Go module root) on a tree that is still on v1.0's own
# codec, with nothing else loading the host:
#
#   bash internal/bsonoracle/testdata/v1corpus/capture.sh
#
# What it does, in order (PHASES=... runs a subset; each phase has its own
# directory under $CAPTURE_DIR so a rerun of one phase replaces only it):
#   tests   the named tests with -tags bsoncorpus, the storage tap writing every
#           document value to $BSONCORPUS_OUT
#   server  a server built with the tag, loaded by bench/social at its smallest
#           size and by bench/propertyshape through `hoarddb ingest`
#   merge   internal/bsonoracle/corpuscapture classifies, deduplicates by
#           SHA-256 and caps the records, and writes the corpus and stats.json
#
# Needs: go, and free ports $SERVER_PORT_BASE and the next one (default 27433).
# It stops the server it starts. The raw records stay in $CAPTURE_DIR (default
# a new /tmp/bsoncorpus.XXXXXX, which can run to gigabytes: remove it when done).
set -uo pipefail

moduleRoot="$(pwd)"
corpusDir="$moduleRoot/internal/bsonoracle/testdata/v1corpus"
captureDir="${CAPTURE_DIR:-$(mktemp -d /tmp/bsoncorpus.XXXXXX)}"
phases="${PHASES:-tests server merge}"
basePort="${SERVER_PORT_BASE:-27433}"
export GOWORK=off
mkdir -p "$captureDir"
echo "capture directory: $captureDir"
echo "main merge-base: $(git merge-base main next 2>/dev/null)"
echo "tree: $(git rev-parse HEAD)"

# runTests <phase> <go test args...>: one tapped test run, its exit status kept.
runTests() {
    local phase="$1"; shift
    mkdir -p "$captureDir/$phase"
    echo "== $phase: go test -tags bsoncorpus -count=1 $*"
    BSONCORPUS_OUT="$captureDir/$phase" go test -tags bsoncorpus -count=1 "$@" \
        > "$captureDir/$phase.log" 2>&1
    echo "$?" > "$captureDir/$phase.status"
    echo "   exit $(cat "$captureDir/$phase.status"), $(du -sh "$captureDir/$phase" | cut -f1) of raw records"
}

if [[ " $phases " == *" tests "* ]]; then
    runTests store ./store/...
    runTests storage ./storage/...
    runTests server -run 'Test.*(Hash|Btree|Heap|Queue|Fifo|Lifo|Blob|Ingest|Restore|Migrat|Dead|StoreField|Update)' ./server/
    runTests cli -run 'Test.*Ingest' ./cli/...
fi

if [[ " $phases " == *" server "* ]]; then
    serverDir="$captureDir/server-run"
    mkdir -p "$serverDir/raw" "$serverDir/data" "$serverDir/shape"
    go build -tags bsoncorpus -o "$serverDir/hoarddb-server" ./cmd/server || exit 1
    go build -o "$serverDir/hoarddb" ./cmd/cli || exit 1
    export HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD=capture-root-password
    clientAddress="127.0.0.1:$basePort"
    echo "== server: starting on $clientAddress"
    HOARDDB_DATA_DIR="$serverDir/data" HOARDDB_TCP_LISTEN="$clientAddress" \
        HOARDDB_LISTEN="127.0.0.1:$((basePort + 1))" BSONCORPUS_OUT="$serverDir/raw" \
        "$serverDir/hoarddb-server" > "$serverDir/server.log" 2>&1 &
    serverPid=$!
    trap 'kill "$serverPid" 2>/dev/null; wait "$serverPid" 2>/dev/null' EXIT
    for _ in $(seq 1 50); do
        (echo 'show databases' | "$serverDir/hoarddb" -address "$clientAddress" -insecure > /dev/null 2>&1) && break
        sleep 0.2
    done

    echo "== server: bench/social at its smallest size"
    go run ./bench/social -addr "$clientAddress" -password "$HOARDDB_ROOT_PASSWORD" \
        -users 200 -seed-posts 10 -conc 4 -warmup 1s -duration 5s \
        > "$serverDir/social.json" 2> "$serverDir/social.log"
    echo "   exit $?"

    echo "== server: bench/propertyshape through hoarddb ingest"
    go run ./bench/propertyshape -out "$serverDir/shape" > "$serverDir/shape-summary.json"
    "$serverDir/hoarddb" ingest "$serverDir/shape" --into-db propshape \
        -address "$clientAddress" -insecure > "$serverDir/ingest.log" 2>&1
    echo "   exit $?"

    kill "$serverPid"; wait "$serverPid" 2>/dev/null; trap - EXIT
    echo "   $(du -sh "$serverDir/raw" | cut -f1) of raw records"
fi

if [[ " $phases " == *" merge "* ]]; then
    inputs=""
    for rawDir in "$captureDir"/*/ "$captureDir"/server-run/raw/; do
        ls "$rawDir"records-*.jsonl > /dev/null 2>&1 || continue
        inputs="${inputs:+$inputs,}${rawDir%/}"
    done
    echo "== merge: $inputs"
    go run ./internal/bsonoracle/corpuscapture -in "$inputs" \
        -out "$corpusDir/records.jsonl.gz" > "$corpusDir/stats.json" || exit 1
    ls -l "$corpusDir/records.jsonl.gz"
    for statusFile in "$captureDir"/*.status; do
        echo "$(basename "$statusFile" .status) test run exit status: $(cat "$statusFile")"
    done
fi

echo "raw records are in $captureDir; remove it when done"
