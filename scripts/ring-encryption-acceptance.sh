#!/usr/bin/env bash
#
# ring-encryption-acceptance.sh - encryption at rest of the ring, checked on data
# directories that real servers wrote (ring encryption at rest spec section 9.3).
#
# It builds the server and the CLI, starts real nodes on loopback ports, writes
# marker strings through the CLI into documents and into every store type the
# CLI can write (fifo, lifo, hash and btree values, a hash key), and then greps
# the raw files for the markers. The claim is a property of a data directory, so
# it is checked on directories the servers wrote: copied while a node runs,
# after a kill -9 in the middle of writes, and after a clean stop.
#
# Single node, sealed (encryption.mode on, keyfile keystore, the key outside the
# data directory):
#   single-writes        every marker write acknowledged and read back
#   single-running       a copy of the data directory taken while the node runs
#                        holds no marker (memtable logs included)
#   single-kill          writes in flight, kill -9: the directory holds no marker,
#                        and the node restarts sealed and reads every marker back
#   single-stopped       after a clean stop the directory holds no marker
#   single-metrics       /metrics reports mode "on" and the store sealed
#   control-*            the same writes on a plaintext node: the marker IS in its
#                        ring/badger and in its ring/wal, or the scans are blind
#   refuse-wrong-key     another key-encryption key: refused, ring/ unchanged (R-1)
#   refuse-no-key-file   the key file gone: refused, ring/ unchanged (R-2)
#   refuse-mode-off      encryption.mode off over the sealed ring: refused with the
#                        ring's text, ring/ unchanged (R-3)
#   refuse-damaged-key   a flipped byte in ring/keys/store.wrapped: refused, the
#                        ring unchanged (R-8)
#   switch-off           ring/, store/ and keys/store.wrapped moved aside, keystore
#                        kept, mode off: the node starts empty and says its ring is
#                        not encrypted
#   switch-back          everything moved back, mode on: every marker reads back
#
# The escrow rehearsal: a sealed single node recovered from its exported node
# master, the way the configuration page says to ("Losing the key", "When the
# wrapped node master is missing"). The node's key-encryption key and its wrapped
# node master are both deleted, so the exported file is the only copy left:
#   escrow-writes        the markers and 40 more documents written; count() and
#                        every body read back and recorded
#   escrow-live-export   -export-key while the node runs: refused, no file written
#   escrow-export        the node stopped, hoarddb-server -export-key <file>: a
#                        32-byte file, mode 600, outside the data directory, with
#                        the key id the node started with; the data directory is
#                        unchanged
#   escrow-refused       keys/node-master.key.wrapped and the key file deleted, a
#                        new key file configured: the start is refused, names the
#                        missing file and the -import-key way out, creates no node
#                        master and changes nothing in the data directory
#   escrow-wrong-export  -import-key of another 32-byte key: refused, nothing written
#   escrow-short-export  -import-key of a cut-off copy of the export: refused
#   escrow-absent-export -import-key of a file that is not there: refused
#   escrow-still-refused after the three refused imports the node still does not start
#   escrow-import        -import-key of the exported file under the new key file:
#                        accepted, the same key id, ring/ unchanged
#   escrow-recovered     the node starts and serves: count() and every body and
#                        store value are what was recorded before the loss
#   escrow-stopped       after a clean stop the directory holds no marker
#
# Three sealed nodes (each with its own key) form a cluster before any database
# exists, then a database with three copies:
#   cluster-form         the cluster forms and every node serves
#   cluster-writes       every marker write acknowledged
#   cluster-running      each node's data directory, copied while it runs, holds
#                        no marker
#   cluster-kill         writes in flight, kill -9 of one node, restart: it serves
#   cluster-stopped      after a stop of every node, no data directory holds a
#                        marker; ring/raft holds the database name (not sealed, by
#                        design) and no marker
#   cluster-metrics      every node's /metrics reports mode "on" and sealed
#   cluster-pepper       the data node that created the first administrator holds
#                        auth.cluster.pepper sealed (purpose 0x0c)
#   cluster-restart      every node restarts and every marker reads back through
#                        every node
#   cluster-refuse-off   a member started with mode off: refused (R-3)
#
# There is no plaintext-joiner leg (spec ruling 8): this release does not check
# that the nodes of a cluster use the same setting.
#
# A run signals only the server PIDs it started, owns its scratch directory
# (marked .ring-encryption-acceptance; nothing outside it is changed), picks free
# loopback ports unless --base is given, removes the scratch on success and keeps
# it on failure or with --keep. It never reads HOARDDB_DATA_DIR.
#
# Usage:
#   scripts/ring-encryption-acceptance.sh                  # every leg, free ports chosen
#   scripts/ring-encryption-acceptance.sh --leg single     # the single node only
#   scripts/ring-encryption-acceptance.sh --leg escrow     # the escrow rehearsal only
#   scripts/ring-encryption-acceptance.sh --leg cluster    # the three nodes only
#   scripts/ring-encryption-acceptance.sh --base 27400     # ports base to base+49
#   scripts/ring-encryption-acceptance.sh --keep           # keep the scratch directory
#   TMPDIR=/var/tmp scripts/ring-encryption-acceptance.sh --leg escrow   # scratch on a real disk
#
# The scratch directory is made under TMPDIR (/tmp when unset). With
# RING_ENCACC_BIN_DIR=<dir> the run uses <dir>/hoarddb-server and <dir>/hoarddb
# and builds nothing.
#
# Exits 0 when every leg passes, 1 when one fails, 2 on misuse.
set -euo pipefail

base=""
keep=0
leg=all

usage() { sed -n '3,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --base) base=${2:-}; shift 2 ;;
    --keep) keep=1; shift ;;
    --leg) leg=${2:-}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1 (run with --help for the usage)" >&2; exit 2 ;;
  esac
done
case "$leg" in
  all|single|escrow|cluster) ;;
  *) echo "unknown leg \"$leg\": use single, escrow, cluster, or leave --leg out for all three" >&2; exit 2 ;;
esac
case "$base" in
  *[!0-9]*) echo "--base needs a port number, got \"$base\"" >&2; exit 2 ;;
esac
# Ports: node n (0 is the single node and the escrow node, 1 to 3 the cluster, 4 the plaintext
# control) uses base+10n (ring and peer address), +1 (the earlier engine's
# internode listener), +2 (clients) and +3 (metrics).
port_span=50
if [ -n "$base" ] && { [ "$base" -lt 1024 ] || [ $((base + port_span)) -gt 65535 ]; }; then
  echo "--base $base: the run needs ports $base to $((base + port_span - 1)); pick a base from 1024 to $((65535 - port_span))" >&2
  exit 2
fi

root=$(cd "$(dirname "$0")/.." && pwd)
module="$root/hoardDB"
[ -f "$module/go.mod" ] || module="$root" # the published tree is the module itself
unset HOARDDB_DATA_DIR
# shellcheck source=scripts/ring-encryption-acceptance-lib.sh
source "$root/scripts/ring-encryption-acceptance-lib.sh"

port_free() { ! ss -ltnH "sport = :$1" 2>/dev/null | grep -q .; }
range_free() {
  local port
  for ((port = $1; port < $1 + port_span; port++)); do port_free "$port" || return 1; done
}
if [ -z "$base" ]; then
  for _ in $(seq 1 100); do
    candidate=$((20000 + (RANDOM % 2900) * 10))
    if range_free "$candidate"; then base=$candidate; break; fi
  done
  [ -n "$base" ] || { echo "no free run of $port_span ports found in 20000-48999; pass --base <port>" >&2; exit 1; }
elif ! range_free "$base"; then
  echo "a port in $base to $((base + port_span - 1)) is in use; pick another --base, or leave it out and a free range is chosen" >&2
  exit 1
fi
ring_port() { echo $((base + 10 * $1)); }
client_port() { echo $((base + 10 * $1 + 2)); }
metrics_port() { echo $((base + 10 * $1 + 3)); }

scratch=$(mktemp -d "${TMPDIR:-/tmp}/hoarddb-ring-encacc.XXXXXX")
touch "$scratch/.ring-encryption-acceptance"
declare -A pids=()

pid_running() {
  local state
  state=$(ps -o stat= -p "$1" 2>/dev/null || true)
  [ -n "$state" ] && [ "${state#Z}" = "$state" ]
}

# stop_pid PID SIGNAL: signal one server this run started, wait up to 15 s, then
# SIGKILL it; reap it either way.
stop_pid() {
  local pid=$1 signal=$2
  kill "-$signal" "$pid" 2>/dev/null || true
  for _ in $(seq 1 150); do pid_running "$pid" || break; sleep 0.1; done
  if pid_running "$pid"; then
    echo "server pid $pid did not exit within 15 s of SIG$signal; killing it" >&2
    kill -KILL "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do pid_running "$pid" || break; sleep 0.1; done
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local status=$? name
  trap - EXIT INT TERM
  for name in "${!pids[@]}"; do stop_pid "${pids[$name]}" KILL; done
  if [ "$keep" = 1 ] || [ "$status" != 0 ]; then
    echo "scratch kept for inspection: $scratch"
  elif [ -f "$scratch/.ring-encryption-acceptance" ]; then
    rm -rf "$scratch"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

failures=0
pass() { printf 'PASS  %-20s %s\n' "$1" "$2"; }
fail() {
  printf 'FAIL  %-20s expected: %s\n' "$1" "$2"
  printf '      %-20s found: %s\n' "" "$3"
  failures=$((failures + 1))
}
die() { fail "$@"; exit 1; }

own_dir() {
  case "$1" in
    "$scratch"/*) [ -f "$scratch/.ring-encryption-acceptance" ] && return 0 ;;
  esac
  echo "refusing to touch $1: it is not inside this run's scratch directory $scratch" >&2
  exit 1
}

echo "run: ports $base to $((base + port_span - 1)), scratch $scratch"
# RING_ENCACC_BIN_DIR names a directory that already holds hoarddb-server and
# hoarddb: a build made elsewhere, or the selftest's stand-ins.
bin=${RING_ENCACC_BIN_DIR:-$scratch}
if [ -n "${RING_ENCACC_BIN_DIR:-}" ]; then
  for binary in hoarddb-server hoarddb; do
    [ -x "$bin/$binary" ] || { echo "RING_ENCACC_BIN_DIR=$bin holds no executable $binary; build both there (go build -o $bin/hoarddb-server ./cmd/server; go build -o $bin/hoarddb ./cmd/cli) or unset it" >&2; exit 2; }
  done
  echo "using the server and the CLI in $bin"
else
  echo "building the server and the CLI"
  (cd "$module" && go build -o "$bin/hoarddb-server" ./cmd/server && go build -o "$bin/hoarddb" ./cmd/cli)
fi

root_password=$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 20)
# m1 a document id, m2 an indexed-looking field, m3 a document value, m4 the
# store values, m5 a hash and btree key. RING_ENCACC_MARKERS="m1 ... m5" is the
# selftest's seam.
read -r m1 m2 m3 m4 m5 <<<"${RING_ENCACC_MARKERS:-$(marker_set 5)}"
markers=("$m1" "$m2" "$m3" "$m4" "$m5")
database=ringencaccdb

new_key_file() { (umask 077 && head -c 32 /dev/urandom >"$1"); }

# node_env N DATA_DIR: the environment of node N on this run's ports.
node_env() {
  local n=$1 data_dir=$2
  printf '%s\n' "PATH=$PATH" "HOME=$HOME" "HOARDDB_DATA_DIR=$data_dir" \
    "HOARDDB_LISTEN=127.0.0.1:$(ring_port "$n")" "HOARDDB_TCP_LISTEN=127.0.0.1:$(client_port "$n")" \
    "HOARDDB_METRICS_ENABLED=true" "HOARDDB_METRICS_LISTEN=127.0.0.1:$(metrics_port "$n")" \
    "HOARDDB_ROOT_USER=admin" "HOARDDB_LOG_FORMAT=text"
}

# start_node NAME N DATA_DIR LOG [KEY=VALUE ...]: start node N in the background.
start_node() {
  local name=$1 n=$2 data_dir=$3 log=$4 env_lines=()
  shift 4
  own_dir "$data_dir"
  mapfile -t env_lines < <(node_env "$n" "$data_dir")
  env -i "${env_lines[@]}" "$@" "$bin/hoarddb-server" >>"$log" 2>&1 &
  pids[$name]=$!
}

stop_node() {
  local name=$1 signal=${2:-TERM}
  [ -n "${pids[$name]:-}" ] || return 0
  stop_pid "${pids[$name]}" "$signal"
  unset "pids[$name]"
}

# hql N: run the statements on stdin through node N's client port.
hql() {
  env -i PATH="$PATH" HOME="$HOME" HOARDDB_ROOT_PASSWORD="$root_password" \
    "$bin/hoarddb" -address "127.0.0.1:$(client_port "$1")" -insecure -user admin 2>&1
}

# await_serving NAME N LOG: wait until node N answers 'status' with Serving: ok.
await_serving() {
  local name=$1 n=$2 log=$3 out=""
  for _ in $(seq 1 120); do
    pid_running "${pids[$name]}" || { tail -20 "$log" >&2; return 1; }
    out=$(printf 'status\n' | hql "$n" || true)
    if grep -q '^Authenticated as' <<<"$out" && grep -qE '^Serving:[[:space:]]+ok$' <<<"$out"; then return 0; fi
    sleep 0.5
  done
  printf '%s\n' "$out" | tail -10 >&2
  tail -20 "$log" >&2
  return 1
}

# write_markers N TAG: the marker writes, through node N, into document TAG-<m1>
# of bucket People. Prints the CLI output.
write_markers() {
  local n=$1 id="$2$m1"
  printf '%s\n' "use $database" \
    "db.People.insert({_id_: \"$id\", Email: \"$m2@example.com\", Note: \"$m3\"})" \
    "db.People[\"$id\"].Feed.push({P: \"$m4\"})" \
    "db.People[\"$id\"].Stack.push({P: \"$m4\"})" \
    "db.People[\"$id\"].Map.put(\"$m5\", {P: \"$m4\"})" \
    "db.People[\"$id\"].Tree.put(\"$m5\", {P: \"$m4\"})" | hql "$n"
}

# writes_acknowledged OUTPUT: every write of write_markers has its OK line.
writes_acknowledged() {
  local out=$1
  grep -q '^OK  id=' <<<"$out" && [ "$(grep -c '^OK  pushed:' <<<"$out")" -eq 2 ] \
    && [ "$(grep -c "^OK  put \"$m5\":" <<<"$out")" -eq 2 ] && ! grep -q 'CLI error' <<<"$out"
}

create_schema() {
  local n=$1 rf=$2
  printf '%s\n' "create database $database { replication_factor: $rf }" "use $database" \
    'create bucket People { Email string, Note string, Feed fifoStore, Stack lifoStore, Map hashStore, Tree btreeStore }' | hql "$n"
}

# markers_read_back N TAG: document TAG<m1> reads back through node N with every
# marker in it and its stores, within 30 s (a node that just started, or a read
# routed to a copy that is still catching up, answers a retryable refusal).
markers_read_back() {
  local n=$1 id="$2$m1" out
  for _ in $(seq 1 30); do
    out=$(printf '%s\n' "use $database" "db.People[\"$id\"].get()" "db.People[\"$id\"].Feed.peek()" \
      "db.People[\"$id\"].Map.get(\"$m5\")" "db.People[\"$id\"].Tree.get(\"$m5\")" | hql "$n")
    if grep -qF "\"_id_\": \"$id\"" <<<"$out" && grep -qF "$m2@example.com" <<<"$out" && grep -qF "$m3" <<<"$out" \
      && [ "$(grep -cF "\"P\": \"$m4\"" <<<"$out")" -eq 3 ]; then
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "$out" | tail -4 >&2
  return 1
}

copy_running() { own_dir "$2"; cp -a "$1" "$2"; }

# write_loop N TAG COUNT: COUNT documents written one statement each, in the
# background, so a kill -9 lands in the middle of writes.
write_loop() {
  local n=$1 tag=$2 count=$3 index
  for ((index = 0; index < count; index++)); do
    printf '%s\n' "use $database" "db.People.insert({_id_: \"$tag$index$m1\", Note: \"$m3\"})" \
      "db.People[\"$tag$index$m1\"].Feed.push({P: \"$m4\"})"
  done | hql "$n" >/dev/null 2>&1 || true
}

# expect_refused LEG DATA_DIR TEXT N [KEY=VALUE ...]: the start exits non-zero
# without serving, its error contains TEXT, and ring/ is unchanged. A refusal is
# immediate; the wait is for a node that wrongly starts, and
# RING_ENCACC_REFUSAL_WAIT (tenths of a second) shortens it for the selftest.
expect_refused() {
  local name=$1 data_dir=$2 text=$3 n=$4 log before after
  shift 4
  log="$scratch/$name.log"
  before=$(tree_digest "$data_dir/ring")
  start_node "$name" "$n" "$data_dir" "$log" "$@"
  for _ in $(seq 1 "${RING_ENCACC_REFUSAL_WAIT:-300}"); do pid_running "${pids[$name]}" || break; sleep 0.1; done
  if pid_running "${pids[$name]}"; then
    stop_node "$name"
    fail "$name" "the start refused" "the node is still running after $((${RING_ENCACC_REFUSAL_WAIT:-300} / 10)) s ($log)"
    return
  fi
  wait "${pids[$name]}" 2>/dev/null || true
  unset "pids[$name]"
  after=$(tree_digest "$data_dir/ring")
  if ! grep -qF -- "$text" "$log"; then
    fail "$name" "a refusal containing: $text" "$(grep -m1 'level=ERROR' "$log" | cut -c1-300)"
  elif [ "$before" != "$after" ]; then
    fail "$name" "ring/ unchanged by the refused start" "its files changed"
  else
    pass "$name" "refused, ring/ unchanged: $(grep -m1 -o 'error="create server: .*' "$log" | cut -c22-150)..."
  fi
}

check_metrics() {
  local leg=$1 n=$2 got
  got=$(metrics_encryption "$(curl -s --max-time 5 "http://127.0.0.1:$(metrics_port "$n")/metrics" || true)")
  if [ "$got" = "on 1" ]; then
    pass "$leg" "node $n: hoarddb_node_encryption_info mode \"on\", hoarddb_node_encryption_sealed 1"
  else
    fail "$leg" "mode \"on\" and sealed 1 on node $n's /metrics" "mode and sealed: $got"
  fi
}

# --- the single node ----------------------------------------------------------

single_leg() {
  local dir="$scratch/single" kek="$scratch/single.kek" log="$scratch/single.log" out tag=s
  local sealed_env plain_dir="$scratch/plain" plain_log="$scratch/plain.log"
  new_key_file "$kek"
  mkdir -p "$dir" "$plain_dir"
  sealed_env=(HOARDDB_ROOT_PASSWORD="$root_password" HOARDDB_ENCRYPTION_MODE=on HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_KEY_FILE="$kek")

  start_node single 0 "$dir" "$log" "${sealed_env[@]}"
  await_serving single 0 "$log" || die single-writes "the sealed single node serves" "it did not ($log)"
  grep -q 'ring store and log encrypted at rest' "$log" || fail single-writes "the start line 'ring store and log encrypted at rest'" "not in $log"
  out=$(create_schema 0 1; write_markers 0 "$tag")
  if writes_acknowledged "$out" && markers_read_back 0 "$tag"; then
    pass single-writes "documents, fifo, lifo, hash and btree stores written and read back"
  else
    die single-writes "every marker write acknowledged and read back" "$(printf '%s' "$out" | tail -4 | tr '\n' ' ')"
  fi
  check_metrics single-metrics 0
  copy_running "$dir" "$scratch/single-running"
  check_sealed_ring single-running "$scratch/single-running" "${markers[@]}" || true

  write_loop 0 k 400 &
  local writer=$!
  sleep 1.5
  stop_node single KILL
  wait "$writer" 2>/dev/null || true
  check_sealed_ring single-kill "$dir" "${markers[@]}" || true
  start_node single 0 "$dir" "$log" "${sealed_env[@]}"
  if await_serving single 0 "$log" && markers_read_back 0 "$tag"; then
    pass single-kill "restarted after kill -9 mid-write: sealed, every marker read back"
  else
    fail single-kill "a restart after kill -9 that serves the markers" "it did not ($log)"
  fi
  stop_node single TERM
  check_sealed_ring single-stopped "$dir" "${markers[@]}" || true

  # The positive control: the same writes on a plaintext node.
  start_node plain 4 "$plain_dir" "$plain_log" HOARDDB_ROOT_PASSWORD="$root_password"
  await_serving plain 4 "$plain_log" || die control-writes "the plaintext node serves" "it did not ($plain_log)"
  out=$(create_schema 4 1; write_markers 4 "$tag")
  writes_acknowledged "$out" || die control-writes "every write to the plaintext node acknowledged" "$(printf '%s' "$out" | tail -3 | tr '\n' ' ')"
  copy_running "$plain_dir" "$scratch/plain-running"
  stop_node plain TERM
  check_plaintext_ring control-running "$scratch/plain-running" "$m3" || true
  check_plaintext_ring control-stopped "$plain_dir" "$m3" || true

  # Refusals, each on the sealed directory, each leaving ring/ as it was.
  local other_kek="$scratch/other.kek" damaged="$scratch/single-damaged" size last
  new_key_file "$other_kek"
  expect_refused refuse-wrong-key "$dir" "which was sealed under a key with kid" 0 \
    HOARDDB_ENCRYPTION_MODE=on HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_KEY_FILE="$other_kek"
  expect_refused refuse-no-key-file "$dir" "does not exist" 0 \
    HOARDDB_ENCRYPTION_MODE=on HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_KEY_FILE="$scratch/no-such.kek"
  expect_refused refuse-mode-off "$dir" "is encrypted (found:" 0 \
    HOARDDB_ENCRYPTION_MODE=off HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_KEY_FILE="$kek"
  copy_running "$dir" "$damaged"
  size=$(stat -c %s "$damaged/ring/keys/store.wrapped")
  last=$(tail -c 1 "$damaged/ring/keys/store.wrapped" | od -An -tu1 | tr -d ' ')
  # shellcheck disable=SC2059 # the format is the one octal escape built here
  printf "$(printf '\\%03o' $(((last ^ 1) & 255)))" | dd of="$damaged/ring/keys/store.wrapped" bs=1 seek=$((size - 1)) conv=notrunc status=none
  expect_refused refuse-damaged-key "$damaged" "unwrapping the ring key" 0 "${sealed_env[@]}"

  # The mode switch: move the ring and the earlier engine's store aside, keep
  # the keystore, start with mode off; then put everything back and start on.
  local aside="$scratch/single-aside" off_aside="$scratch/single-off-aside" part
  mkdir -p "$aside/keys" "$off_aside/keys"
  for part in ring store keys/store.wrapped; do own_dir "$dir/$part"; mv "$dir/$part" "$aside/$part"; done
  start_node single 0 "$dir" "$log" HOARDDB_ROOT_PASSWORD="$root_password" HOARDDB_ENCRYPTION_MODE=off \
    HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_KEY_FILE="$kek"
  local listing=""
  if await_serving single 0 "$log"; then listing=$(printf 'show databases\n' | hql 0); fi
  if grep -q 'ring store and log are not encrypted' "$log" && grep -q '^Authenticated as' <<<"$listing" \
    && ! grep -q "$database" <<<"$listing"; then
    pass switch-off "started with mode off on an empty ring, and said 'ring store and log are not encrypted'"
  else
    fail switch-off "a start with mode off after the move, serving an empty ring and saying it is not encrypted" \
      "$(printf '%s' "$listing" | tail -3 | tr '\n' ' ') (log $log)"
  fi
  stop_node single TERM
  for part in ring store keys/store.wrapped; do
    [ -e "$dir/$part" ] && mv "$dir/$part" "$off_aside/$part"
    mv "$aside/$part" "$dir/$part"
  done
  start_node single 0 "$dir" "$log" "${sealed_env[@]}"
  if await_serving single 0 "$log" && markers_read_back 0 "$tag"; then
    pass switch-back "moved back and started with mode on: every marker read back"
  else
    fail switch-back "the sealed ring back, serving its markers" "see $log"
  fi
  stop_node single TERM
}

# --- the escrow rehearsal --------------------------------------------------------

# offline_key LOG DATA_DIR KEY_FILE ARG...: run hoarddb-server with one of its
# offline key verbs (-export-key, -import-key) under node 0's settings, the
# output in LOG. Returns the command's exit status.
offline_key() {
  local log=$1 data_dir=$2 key_file=$3 env_lines=()
  shift 3
  own_dir "$data_dir"
  mapfile -t env_lines < <(node_env 0 "$data_dir")
  env -i "${env_lines[@]}" HOARDDB_ENCRYPTION_MODE=on HOARDDB_ENCRYPTION_KEYSTORE=keyfile \
    HOARDDB_ENCRYPTION_KEY_FILE="$key_file" "$bin/hoarddb-server" "$@" >"$log" 2>&1
}

# last_error LOG prints the error of the last ERROR line of LOG, cut short.
last_error() { grep 'level=ERROR' "$1" | tail -1 | sed -e 's/.* error="//' -e 's/"$//' | cut -c1-"${2:-110}"; }

# read_all N TAG COUNT: the answers node N gives for the bucket's count(), the
# marker document TAG<m1>, the COUNT documents TAG<i><m1>, and the marker
# document's stores, without the CLI's greeting. check_read_back_whole reads it.
read_all() {
  local n=$1 tag=$2 count=$3 index id="$2$m1"
  {
    printf '%s\n' "use $database" 'db.People.count()' "db.People[\"$id\"].get()"
    for ((index = 0; index < count; index++)); do printf 'db.People["%s"].get()\n' "$tag$index$m1"; done
    printf '%s\n' "db.People[\"$id\"].Feed.peek()" "db.People[\"$id\"].Map.get(\"$m5\")" "db.People[\"$id\"].Tree.get(\"$m5\")"
  } | hql "$n" | sed -e '1,/^Switched to database/d'
}

# refused_import LEG DATA_DIR KEY_FILE WRAPPED FILE TEXT: -import-key FILE exits
# non-zero with TEXT in its error, writes no node master and changes nothing in
# the data directory.
refused_import() {
  local name=$1 data_dir=$2 key_file=$3 wrapped=$4 file=$5 text=$6 log="$scratch/$1.log" before
  before=$(tree_digest "$data_dir")
  if offline_key "$log" "$data_dir" "$key_file" -import-key "$file"; then
    fail "$name" "-import-key refused" "it exited 0: $(tail -1 "$log" | cut -c1-200)"
  elif ! grep -qF -- "$text" "$log"; then
    fail "$name" "a refusal containing: $text" "$(last_error "$log" 300)"
  elif [ -e "$wrapped" ]; then
    fail "$name" "no node master written by a refused import" "$wrapped exists"
  elif [ "$(tree_digest "$data_dir")" != "$before" ]; then
    fail "$name" "the data directory unchanged by the refused import" "its files changed"
  else
    pass "$name" "refused, nothing written: $(last_error "$log")..."
  fi
}

escrow_leg() {
  local dir="$scratch/escrow" kek="$scratch/escrow.kek" new_kek="$scratch/escrow-new.kek" log="$scratch/escrow.log"
  local recovered_log="$scratch/escrow-recovered.log" vault="$scratch/escrow-vault" expected="$scratch/escrow-expected"
  local read_before="$scratch/escrow-read-before" read_after="$scratch/escrow-read-after"
  local exported="$vault/node-master.key" wrapped="$dir/keys/node-master.key.wrapped"
  local out tag=e count=40 index kid before fault failed_before
  new_key_file "$kek"
  mkdir -p "$dir"
  (umask 077 && mkdir "$vault")

  start_node escrow 0 "$dir" "$log" HOARDDB_ROOT_PASSWORD="$root_password" HOARDDB_ENCRYPTION_MODE=on \
    HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_KEY_FILE="$kek"
  await_serving escrow 0 "$log" || die escrow-writes "the sealed node serves" "it did not ($log)"
  out=$(create_schema 0 1; write_markers 0 "$tag")
  writes_acknowledged "$out" || die escrow-writes "every marker write acknowledged" "$(printf '%s' "$out" | tail -4 | tr '\n' ' ')"
  printf '%s %s\n' "$tag$m1" "$m3" >"$expected"
  out=$({
    printf '%s\n' "use $database"
    for ((index = 0; index < count; index++)); do
      printf 'db.People.insert({_id_: "%s", Email: "%s@example.com", Note: "%s"})\n' "$tag$index$m1" "$index$m2" "$index-$m3"
      printf '%s %s\n' "$tag$index$m1" "$index-$m3" >>"$expected"
    done
  } | hql 0 || true)
  [ "$(grep -c '^OK  id=' <<<"$out")" -eq "$count" ] \
    || die escrow-writes "$count more documents acknowledged" "$(grep -c '^OK  id=' <<<"$out") were: $(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
  markers_read_back 0 "$tag" || die escrow-writes "every marker read back" "it was not ($log)"
  read_all 0 "$tag" "$count" >"$read_before" || true
  fault=$(read_back_fault "$expected" "$read_before")
  [ -z "$fault" ] || die escrow-writes "count() and every document read back before the loss" "$fault"
  pass escrow-writes "$((count + 1)) documents and the fifo, lifo, hash and btree stores written; count() and every body recorded"

  # The export needs the node stopped: while it runs it is refused and writes nothing.
  if offline_key "$scratch/escrow-live-export.log" "$dir" "$kek" -export-key "$exported"; then
    fail escrow-live-export "-export-key refused while the node runs" "it exited 0"
  elif [ -e "$exported" ]; then
    fail escrow-live-export "no file written by a refused export" "$exported exists"
  else
    pass escrow-live-export "refused while the node runs, no file written: $(last_error "$scratch/escrow-live-export.log")..."
  fi
  rm -f "$exported"
  stop_node escrow TERM

  before=$(tree_digest "$dir")
  offline_key "$scratch/escrow-export.log" "$dir" "$kek" -export-key "$exported" \
    || die escrow-export "hoarddb-server -export-key exits 0 on the stopped node" "$(last_error "$scratch/escrow-export.log" 300)"
  kid=$(sed -n 's/^Exported node master with kid \([0-9a-f]*\)$/\1/p' "$scratch/escrow-export.log")
  failed_before=$failures
  if [ -z "$kid" ] || ! grep -qF "encrypted at rest (node master kid $kid)" "$log"; then
    fail escrow-export "the export names the key id of the node master the node started with" \
      "export said \"${kid:-nothing}\"; the node said: $(grep -m1 -o 'encrypted at rest (node master kid [0-9a-f]*)' "$log")"
  fi
  [ "$(tree_digest "$dir")" = "$before" ] || fail escrow-export "the data directory unchanged by the export" "its files changed"
  [ "$failures" -eq "$failed_before" ] || exit 1
  check_exported_key escrow-export "$exported" "$dir" || exit 1

  # The loss: the wrapped node master and the key that wrapped it. What is left
  # of the node's keys is the exported file. The operator configures a new key file.
  own_dir "$wrapped"
  rm -f "$wrapped" "$kek"
  new_key_file "$new_kek"
  local recovery_env=(HOARDDB_ROOT_PASSWORD="$root_password" HOARDDB_ENCRYPTION_MODE=on HOARDDB_ENCRYPTION_KEYSTORE=keyfile
    HOARDDB_ENCRYPTION_KEY_FILE="$new_kek")
  before=$(tree_digest "$dir")
  failed_before=$failures
  expect_refused escrow-refused "$dir" "refusing to start: keys/node-master.key.wrapped is missing from data directory" 0 "${recovery_env[@]}"
  grep -qF "import the escrowed node master with 'hoarddb-server -import-key <file>'" "$scratch/escrow-refused.log" \
    || fail escrow-refused "the refusal names the way out: hoarddb-server -import-key <file>" "$(last_error "$scratch/escrow-refused.log" 300)"
  [ ! -e "$wrapped" ] || fail escrow-refused "no node master created by the refused start" "$wrapped exists"
  [ "$(tree_digest "$dir")" = "$before" ] || fail escrow-refused "the data directory unchanged by the refused start" "its files changed"
  [ "$failures" -eq "$failed_before" ] || exit 1

  # Recovery without the export, or with the wrong one, must not work.
  new_key_file "$vault/another-node.key"
  head -c 31 "$exported" >"$vault/cut-off.key"
  refused_import escrow-wrong-export "$dir" "$new_kek" "$wrapped" "$vault/another-node.key" "does not open"
  refused_import escrow-short-export "$dir" "$new_kek" "$wrapped" "$vault/cut-off.key" "want 32"
  refused_import escrow-absent-export "$dir" "$new_kek" "$wrapped" "$vault/no-such.key" "reading import file"
  expect_refused escrow-still-refused "$dir" "refusing to start: keys/node-master.key.wrapped is missing from data directory" 0 "${recovery_env[@]}"
  [ "$failures" -eq "$failed_before" ] || exit 1

  # The recovery: the exported file, wrapped under the new key file.
  before=$(tree_digest "$dir/ring")
  offline_key "$scratch/escrow-import.log" "$dir" "$new_kek" -import-key "$exported" \
    || die escrow-import "hoarddb-server -import-key of the exported file exits 0" "$(last_error "$scratch/escrow-import.log" 300)"
  if ! grep -qx "Imported node master with kid $kid" "$scratch/escrow-import.log"; then
    die escrow-import "the import names the exported key id $kid" "$(tail -1 "$scratch/escrow-import.log" | cut -c1-200)"
  elif [ ! -f "$wrapped" ] || [ "$(stat -c %a "$wrapped")" != 600 ]; then
    die escrow-import "keys/node-master.key.wrapped written with mode 600" "$(stat -c '%a' "$wrapped" 2>&1 | cut -c1-100)"
  elif [ "$(tree_digest "$dir/ring")" != "$before" ]; then
    die escrow-import "ring/ unchanged by the import" "its files changed"
  fi
  pass escrow-import "the exported node master (kid $kid) imported under a new key file; ring/ unchanged"

  start_node escrow 0 "$dir" "$recovered_log" "${recovery_env[@]}"
  if await_serving escrow 0 "$recovered_log" && markers_read_back 0 "$tag"; then
    read_all 0 "$tag" "$count" >"$read_after" || true
    grep -qF "ring store and log encrypted at rest (node master kid $kid)" "$recovered_log" \
      || fail escrow-recovered "the recovered node sealed under node master $kid" "its start line is not in $recovered_log"
    check_read_back_whole escrow-recovered "$expected" "$read_before" "$read_after" || true
  else
    fail escrow-recovered "the node starts from the imported node master and serves its markers" "it did not ($recovered_log)"
  fi
  stop_node escrow TERM
  check_sealed_ring escrow-stopped "$dir" "${markers[@]}" || true
}

# --- three sealed nodes ---------------------------------------------------------

# make_identity DIR: an ed25519 key and certificate in DIR/keys, as the server
# would make on its first start; prints the fingerprint the others pin.
make_identity() {
  mkdir -p "$1/keys"
  openssl req -x509 -newkey ed25519 -nodes -days 30 -subj "/O=hoardDB/CN=hoarddb-node" \
    -keyout "$1/keys/server.key" -out "$1/keys/server.crt" 2>/dev/null
  chmod 600 "$1/keys/server.key"
  openssl pkey -in "$1/keys/server.key" -pubout -outform DER | tail -c 32 | sha256sum | cut -d' ' -f1
}

cluster_leg() {
  local n name pins="" out seeds tag=c
  local -A dirs=() logs=() envs=()
  for n in 1 2 3; do
    name="node-$n"
    dirs[$n]="$scratch/$name"
    logs[$n]="$scratch/$name.log"
    new_key_file "$scratch/$name.kek"
    pins="$pins,$name=SHA256:$(make_identity "${dirs[$n]}")"
  done
  pins=${pins#,}
  cluster_env() {
    local n=$1 seeds="" m
    for ((m = 1; m < n; m++)); do seeds="$seeds,node-$m@127.0.0.1:$(ring_port "$m")"; done
    printf '%s\n' "HOARDDB_NODE_ID=node-$n" HOARDDB_CLUSTER_ENABLED=true "HOARDDB_SEED_NODES=${seeds#,}" \
      "HOARDDB_SEED_FINGERPRINTS=$pins" \
      HOARDDB_ENCRYPTION_MODE=on HOARDDB_ENCRYPTION_KEYSTORE=keyfile "HOARDDB_ENCRYPTION_KEY_FILE=$scratch/node-$n.kek"
    [ "$n" = 1 ] && printf '%s\n' "HOARDDB_ROOT_PASSWORD=$root_password"
    return 0
  }
  start_cluster_node() {
    local n=$1 lines=()
    mapfile -t lines < <(cluster_env "$n")
    start_node "node-$n" "$n" "${dirs[$n]}" "${logs[$n]}" "${lines[@]}"
  }
  for n in 1 2 3; do start_cluster_node "$n"; done
  await_serving node-1 1 "${logs[1]}" || die cluster-form "node-1 serves" "it did not (${logs[1]})"
  for n in 2 3; do
    for _ in $(seq 1 60); do
      out=$(printf 'join node-%s@127.0.0.1:%s\n' "$n" "$(ring_port "$n")" | hql 1)
      grep -qE "^Added node-$n@" <<<"$out" && break
      sleep 1
    done
    grep -qE "^Added node-$n@" <<<"$out" || die cluster-form "join node-$n answered 'Added node-$n@...'" "$(printf '%s' "$out" | tail -2 | tr '\n' ' ')"
  done
  for n in 1 2 3; do
    await_serving "node-$n" "$n" "${logs[$n]}" || die cluster-form "node-$n serves after the joins" "it did not (${logs[$n]})"
  done
  pass cluster-form "three sealed nodes, each with its own key, form one cluster"

  out=$(create_schema 1 3; write_markers 1 "$tag")
  if writes_acknowledged "$out" && markers_read_back 2 "$tag" && markers_read_back 3 "$tag"; then
    pass cluster-writes "every marker write acknowledged at three copies and read back through node-2 and node-3"
  else
    die cluster-writes "every marker write acknowledged and read back" "$(printf '%s' "$out" | tail -4 | tr '\n' ' ')"
  fi
  for n in 1 2 3; do check_metrics cluster-metrics "$n"; done
  for n in 1 2 3; do
    copy_running "${dirs[$n]}" "$scratch/node-$n-running"
    check_sealed_ring cluster-running "$scratch/node-$n-running" "${markers[@]}" || true
  done

  write_loop 1 k 300 &
  local writer=$!
  sleep 1.5
  stop_node node-3 KILL
  wait "$writer" 2>/dev/null || true
  check_sealed_ring cluster-kill "${dirs[3]}" "${markers[@]}" || true
  start_cluster_node 3
  if await_serving node-3 3 "${logs[3]}"; then
    pass cluster-kill "node-3 killed mid-write restarts sealed and serves"
  else
    fail cluster-kill "node-3 restarts after kill -9" "it did not (${logs[3]})"
  fi
  check_sealed_file cluster-pepper "${dirs[1]}/auth.cluster.pepper" 0c || true

  for n in 3 2 1; do stop_node "node-$n" TERM; done
  for n in 1 2 3; do check_sealed_ring cluster-stopped "${dirs[$n]}" "${markers[@]}" || true; done
  if [ -n "$(marker_files "${dirs[1]}/ring/raft" "$database")" ]; then
    pass cluster-stopped "ring/raft holds the database name $database, in the clear by design, and no marker"
  else
    fail cluster-stopped "the database name in ring/raft (the scan's own control)" "not found under ${dirs[1]}/ring/raft"
  fi

  for n in 1 2 3; do start_cluster_node "$n"; done
  local served=0
  for n in 1 2 3; do
    await_serving "node-$n" "$n" "${logs[$n]}" && markers_read_back "$n" "$tag" && served=$((served + 1))
  done
  if [ "$served" = 3 ]; then
    pass cluster-restart "every node restarted and read every marker back"
  else
    fail cluster-restart "every marker read back through all three nodes" "$served of 3 did"
  fi
  stop_node node-2 TERM
  local off_lines=()
  mapfile -t off_lines < <(cluster_env 2 | sed 's/^HOARDDB_ENCRYPTION_MODE=on$/HOARDDB_ENCRYPTION_MODE=off/')
  expect_refused cluster-refuse-off "${dirs[2]}" "is encrypted (found:" 2 "${off_lines[@]}"
  for n in 3 1; do stop_node "node-$n" TERM; done
}

case "$leg" in
  single) single_leg ;;
  escrow) escrow_leg ;;
  cluster) cluster_leg ;;
  all) single_leg; escrow_leg; cluster_leg ;;
esac

if [ "$failures" -eq 0 ]; then
  echo "ring encryption acceptance ($leg): PASS"
  exit 0
fi
echo "ring encryption acceptance ($leg): FAIL ($failures)"
exit 1
