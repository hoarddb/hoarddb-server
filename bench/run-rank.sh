#!/bin/bash
# Run the keyed rank (bench/rank_bench.go): hoardDB vs MongoDB vs MySQL, one
# request in flight, fresh state every leg, interleaved across repetitions.
#
# Legs per (rep, scale), in this order:
#   hoardDB         fresh server on a fresh data dir, deleted afterwards
#   MongoDB         implicit {w:1}              — no fsync on the ack path
#   MongoDB j:true  {w:1, j:true}               — journal fsync per write
#   MySQL no-fsync  flush_log_at_trx_commit=2, sync_binlog=0 (SET GLOBAL)
#   MySQL           the server's defaults (1 / 1) — two fsyncs per commit
#
# Credentials come from the environment only (HOARDDB_BENCH_MONGO_URI,
# HOARDDB_BENCH_MYSQL_DSN, and MYSQL_PWD for the SET GLOBAL toggle through
# `docker exec`). The MySQL sync settings are restored to 1/1 on exit.
#
# Address the containers by their bridge IPs, not a published 127.0.0.1 port:
# the published port goes through docker-proxy, a userland hop hoardDB's native
# process does not pay (it adds per-read latency to a containerised competitor).
#
# Env: BENCH, SERVER, DATA_ROOT (must not be tmpfs), REPS, SCALES, OUT,
#      MYSQL_CONTAINER, ADDR, LOAD_MAX. Example:
#   BENCH=/path/rank-bench SERVER=/path/hoarddb-server DATA_ROOT=$PWD/.rankdata \
#     OUT=rank.jsonl bash bench/run-rank.sh
set -u

BENCH=${BENCH:?build it: go build -o <path> ./bench}
SERVER=${SERVER:?build it: go build -o <path> ./cmd/server}
DATA_ROOT=${DATA_ROOT:?a scratch directory on the same kind of disk the other engines use}
REPS=${REPS:-3}
SCALES=${SCALES:-"1000 5000"}
OUT=${OUT:-rank-$(date +%Y%m%d-%H%M%S).jsonl}
MYSQL_CONTAINER=${MYSQL_CONTAINER:-mysql-bench}
ADDR=${ADDR:-127.0.0.1:7433}
: "${HOARDDB_BENCH_MONGO_URI:?}" "${HOARDDB_BENCH_MYSQL_DSN:?}" "${MYSQL_PWD:?}"

# A tmpfs data dir would give hoardDB free I/O the containers' disks do not get.
if [ "$(findmnt -no FSTYPE -T "$(dirname "$DATA_ROOT")")" = "tmpfs" ]; then
    echo "error: DATA_ROOT $DATA_ROOT is on tmpfs; put it on the disk the containers use" >&2
    exit 1
fi
if ss -ltn | grep -q " ${ADDR} "; then
    echo "error: $ADDR is already listening — another server or run holds it" >&2
    exit 1
fi

mysql_sync() {
    docker exec -e MYSQL_PWD "$MYSQL_CONTAINER" mysql -uroot -e \
        "SET GLOBAL innodb_flush_log_at_trx_commit=$1; SET GLOBAL sync_binlog=$2;"
}
trap 'mysql_sync 1 1 && echo "MySQL sync settings restored to 1/1" >&2' EXIT

: > "$OUT"
failed=0

hoarddb_leg() {
    local rep=$1 docs=$2 dir="$DATA_ROOT/rep$1-$2"
    rm -rf "$dir" && mkdir -p "$dir"
    HOARDDB_DATA_DIR="$dir/data" HOARDDB_TCP_LISTEN="$ADDR" HOARDDB_ROOT_USER=admin \
        HOARDDB_ROOT_PASSWORD=bench-root-password "$SERVER" > "$dir/server.log" 2>&1 &
    local pid=$!
    for _ in $(seq 100); do ss -ltn | grep -q " ${ADDR} " && break; sleep 0.1; done
    "$BENCH" -db=rank -engine=hoarddb -records="$docs" -rep="$rep" -addr="$ADDR" >> "$OUT"
    local rc=$?
    kill "$pid" && wait "$pid"
    rm -rf "$dir"
    return $rc
}

# LOAD_MAX (optional): before each block, wait up to 5 minutes for the
# 1-minute load average to drop below it — a shared host's other work moves
# p99s — and record how long the wait was and whether it gave up.
wait_for_quiet() {
    [ -z "${LOAD_MAX:-}" ] && return
    local waited=0
    while awk -v max="$LOAD_MAX" '{exit !($1 >= max)}' /proc/loadavg && [ $waited -lt 300 ]; do
        sleep 5; waited=$((waited + 5))
    done
    echo "   waited ${waited}s for load < $LOAD_MAX" >&2
}

for rep in $(seq "$REPS"); do
    for docs in $SCALES; do
        wait_for_quiet
        echo "== rep $rep, $docs docs/bucket, load: $(cut -d' ' -f1-3 /proc/loadavg)" >&2
        hoarddb_leg "$rep" "$docs" || failed=$((failed + 1))
        "$BENCH" -db=rank -engine=mongo -records="$docs" -rep="$rep" >> "$OUT" || failed=$((failed + 1))
        "$BENCH" -db=rank -engine=mongo -mongo-journal -records="$docs" -rep="$rep" >> "$OUT" || failed=$((failed + 1))
        mysql_sync 2 0 || exit 1
        "$BENCH" -db=rank -engine=mysql -records="$docs" -rep="$rep" >> "$OUT" || failed=$((failed + 1))
        mysql_sync 1 1 || exit 1
        "$BENCH" -db=rank -engine=mysql -records="$docs" -rep="$rep" >> "$OUT" || failed=$((failed + 1))
    done
done

echo "== done, load: $(cut -d' ' -f1-3 /proc/loadavg); $failed failed legs; samples in $OUT" >&2
"$BENCH" -db=rank-summary -summary="$OUT"
exit $(( failed > 0 ? 1 : 0 ))
