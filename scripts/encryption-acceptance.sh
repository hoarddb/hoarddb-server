#!/usr/bin/env bash
#
# encryption-acceptance.sh — the at-rest encryption Stage 1 acceptance check
# (spec rev 4 §7, build doc step 11), run against live nodes.
#
# The claim is a property of a data directory, not of a function, so it is
# checked on directories that real servers wrote, with the servers stopped.
# The script builds the server and the CLI, runs a SEALED node
# (encryption.mode=require, keystore=keyfile) and a
# PLAINTEXT node (no encryption settings at all), writes the same markers
# through the CLI into each, stops them, and checks:
#
#   writes                every write to the sealed node was acknowledged, and the
#                         markers read back
#   seal-on-first-touch   the fresh require-mode directory seals its store
#   allowlist             every path in the sealed directory matches exactly one
#                         class; an unmatched path fails (fails closed)
#   allowlist-selftest    a stray file added to a copy is caught (rule 1's guard)
#   structure             TestAcceptanceBadgerStructure on the sealed store
#   greps                 no marker and not the KEK in the sealed directory
#   control-writes        every write to the plaintext node was acknowledged, so
#                         the positive control below has something to find
#   positive-control      every marker IS found in the plaintext store and the KEK
#                         is found in its own file, or the greps above are blind
#                         and the run fails
#   tamper                a flipped byte in either wrapped key refuses the start
#   key-missing           keys/store.wrapped deleted: the start is refused
#   mode-mismatch         the sealed directory started with mode off: refused
#   zero-config           the plaintext node restarts and serves the markers
#   escrow                -export-key writes a 32-byte key at 0600; -import-key -force
#                         rewraps it under a new KEK; the node restarts under that new
#                         KEK and still serves the markers
#
# Legs this stage cannot run are refused by name, never skipped silently:
#   --leg crash    (crash injection between the key's fsync and the first open: owed)
#   --leg mixed    (a sealed node placing data on a plaintext peer: spec §5.5)
#
# --leg blob runs on its own (blob payload files, spec §4.2.1) and does not run
# in the default pass:
#
#   blob-writes           a sealed node, started through the real start path, wrote a
#                         blob bucket holding a file-backed blob and an inline one
#   blob-allowlist        every path matches one class, and every payload file under
#                         blobs/ opens with a purpose-0x06 header
#   blob-greps            the marker is nowhere in the sealed directory, and the blob
#                         payload is a file (so the grep looked at one)
#   blob-control          the same writes at mode off leave the marker in blobs/, so
#                         the grep above can see what it looks for
#   blob-read             the real server binary starts on the sealed directory and
#                         reports the blob's size
#   blob-tamper           a flipped byte in a payload is reported as damage to that
#                         blob, naming the database, the bucket and the key
#   blob-registry-missing keys/blobs.wrapped deleted: the start is refused
#
# The blob is written by a Go test (server: TestAcceptanceWriteBlob) because the
# CLI reads statements a line at a time up to 64 KiB, and a payload that is a
# file is at least 64 KiB.
#
# Safe to run more than once at once. A run signals only the server PIDs it
# started (never a process name), owns its scratch directory, and picks ports
# no other running acceptance run holds. It cleans up on failure and on
# interrupt; the scratch directory is removed on success and kept on failure
# (the failure names it), or always with --keep. It never reads HOARDDB_DATA_DIR
# and refuses to touch any directory it did not create, marked
# .encryption-acceptance.
#
# Usage:
#   scripts/encryption-acceptance.sh                 # every Stage 1 leg, free ports chosen
#   scripts/encryption-acceptance.sh --base 27433    # this run's port triple: base, base+1, base+100
#   scripts/encryption-acceptance.sh --keep          # keep the scratch directory on success too
#   scripts/encryption-acceptance.sh --leg crash     # a leg Stage 1 cannot run
#
# Exits 0 when every leg passes, 1 when a leg fails, 2 on misuse, 3 when asked
# for a leg this stage cannot run.
set -euo pipefail

base=""
keep=0
leg=all

usage() { sed -n '3,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --base) base=${2:-}; shift 2 ;;
    --keep) keep=1; shift ;;
    --leg)  leg=${2:-}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$base" in
  *[!0-9]*) echo "--base needs a port number, got \"$base\"" >&2; exit 2 ;;
esac
if [ -n "$base" ] && [ $((base + 100)) -gt 65535 ]; then
  echo "--base $base leaves no room for the driver port base+100 ($((base + 100))); use a base below 65435." >&2
  exit 2
fi

case "$leg" in
  all|escrow|blob) ;;
  crash)  echo "leg crash: crash injection (N6) is owed; the ordering is landed in createNodeStoreKey but no leg kills the process mid-write yet." >&2; exit 3 ;;
  mixed)  echo "leg mixed: a sealed node placing data on a plaintext peer is spec §5.5, not Stage 1." >&2; exit 3 ;;
  *) echo "unknown leg \"$leg\" (escrow, crash, mixed, blob, or none for the Stage 1 run)" >&2; exit 2 ;;
esac

root=$(cd "$(dirname "$0")/.." && pwd)
module="$root/hoardDB"
[ -f "$module/go.mod" ] || module="$root"  # the published tree is the module itself
unset HOARDDB_DATA_DIR

scratch=""
node_pid=""
claimed_ports=()
lock_root="/tmp/hoarddb-encacc-locks-$(id -u)"

# pid_running PID: alive and not a zombie. A child that has exited but not been
# waited on still answers kill -0, so the process state decides.
pid_running() {
  local state
  state=$(ps -o stat= -p "$1" 2>/dev/null || true)
  [ -n "$state" ] && [ "${state#Z}" = "$state" ]
}

# kill_node_hard stops the server this run started, by PID, and reaps it.
kill_node_hard() {
  [ -n "$node_pid" ] || return 0
  kill -TERM "$node_pid" 2>/dev/null || true
  for _ in $(seq 1 30); do
    pid_running "$node_pid" || break
    sleep 0.1
  done
  pid_running "$node_pid" && kill -KILL "$node_pid" 2>/dev/null
  for _ in $(seq 1 30); do
    pid_running "$node_pid" || break
    sleep 0.1
  done
  wait "$node_pid" 2>/dev/null || true
}

# cleanup runs on every exit path (success, a failed leg, an aborted command,
# INT and TERM). It stops only the PID this run recorded, and never removes the
# scratch directory while that server might still be writing into it: deleting
# a live Badger directory sends it into an unkillable flush loop.
cleanup() {
  local status=$? port
  trap - EXIT INT TERM
  kill_node_hard
  if [ -n "$node_pid" ] && pid_running "$node_pid"; then
    echo "server pid $node_pid did not stop even on SIGKILL; scratch left in place: $scratch" >&2
  elif [ -z "$scratch" ]; then
    :
  elif [ "$keep" = 1 ] || [ "$status" != 0 ]; then
    echo "scratch kept for inspection: $scratch"
  elif [ -f "$scratch/.encryption-acceptance" ]; then
    rm -rf "$scratch"
  fi
  for port in ${claimed_ports[@]+"${claimed_ports[@]}"}; do
    [ "$(cat "$lock_root/$port/pid" 2>/dev/null)" = "$$" ] && rm -rf "${lock_root:?}/$port"
  done
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- ports -----------------------------------------------------------------

port_free() { ! ss -ltnH "sport = :$1" 2>/dev/null | grep -q .; }

port_holder() {
  ss -ltnpH "sport = :$1" 2>/dev/null | grep -o 'users:(("[^"]*",pid=[0-9]*' | head -1 | sed 's/users:(("\([^"]*\)",pid=\(.*\)/pid \2 (\1)/'
}

# claim_port PORT reserves a port against other acceptance runs on this host with
# an atomic mkdir. A lock whose owner is gone is taken over. This closes the gap
# between choosing a port and the server binding it, which spans the build.
claim_port() {
  local lock="$lock_root/$1" holder
  mkdir -p "$lock_root"
  if mkdir "$lock" 2>/dev/null; then
    echo "$$" > "$lock/pid"
    claimed_ports+=("$1")
    return 0
  fi
  holder=$(cat "$lock/pid" 2>/dev/null || true)
  if [ -n "$holder" ] && ! kill -0 "$holder" 2>/dev/null; then
    rm -rf "$lock"
    if mkdir "$lock" 2>/dev/null; then
      echo "$$" > "$lock/pid"
      claimed_ports+=("$1")
      return 0
    fi
  fi
  return 1
}

# claim_triple BASE takes ring BASE, internode BASE+1 and driver BASE+100. Only the
# last two are bound by the server (the ring port is an identity), so only
# they must be free; all three are locked so that another run cannot choose a
# triple that overlaps this one.
claim_triple() {
  local candidate=$1 port
  claimed_ports=()
  for port in "$candidate" $((candidate + 1)) $((candidate + 100)); do
    claim_port "$port" || { release_claims; return 1; }
  done
  if ! port_free $((candidate + 1)) || ! port_free $((candidate + 100)); then
    release_claims
    return 1
  fi
}

release_claims() {
  local port
  for port in ${claimed_ports[@]+"${claimed_ports[@]}"}; do
    [ "$(cat "$lock_root/$port/pid" 2>/dev/null)" = "$$" ] && rm -rf "${lock_root:?}/$port"
  done
  claimed_ports=()
}

if [ -n "$base" ]; then
  if ! claim_triple "$base"; then
    for port in $((base + 1)) $((base + 100)); do
      port_free "$port" && continue
      echo "port $port (base $base: internode is base+1, driver is base+100) is held by $(port_holder "$port" || true)." >&2
    done
    echo "Another acceptance run may also have claimed base $base (locks: $lock_root)." >&2
    echo "Pick a different triple with --base <port> (uses <port>, <port>+1 and <port>+100), or omit --base and a free one is chosen." >&2
    exit 1
  fi
else
  for _ in $(seq 1 200); do
    candidate=$((20000 + RANDOM % 30000))
    if claim_triple "$candidate"; then base=$candidate; break; fi
  done
  if [ -z "$base" ]; then
    echo "no free port triple found in 20000-49999 after 200 tries; pass --base <port> with a free port." >&2
    exit 1
  fi
fi
ring=$base
internode=$((base + 1))
driver=$((base + 100))
binary="hdb-ea$base"

scratch=$(mktemp -d "${TMPDIR:-/tmp}/hoarddb-encryption-acceptance.XXXXXX")
touch "$scratch/.encryption-acceptance"
sealed_dir="$scratch/sealed"
plain_dir="$scratch/plain"
mkdir -p "$sealed_dir" "$plain_dir"

# own_dir refuses any directory this run did not create, before anything is
# changed or deleted in it.
own_dir() {
  case "$1" in
    "$scratch"/*) [ -f "$scratch/.encryption-acceptance" ] && return 0 ;;
  esac
  echo "refusing to touch $1: it is not inside this run's scratch directory $scratch" >&2
  exit 1
}

failures=0
pass() { printf 'PASS  %-20s %s\n' "$1" "$2"; }
known() { printf 'KNOWN %-20s %s\n' "$1" "$2"; }

# fail LEG EXPECTED FOUND: every failure says which leg, what it required and
# what it saw. The scratch directory is named when the run exits.
fail() {
  printf 'FAIL  %-20s expected: %s\n' "$1" "$2"
  printf '      %-20s found: %s\n' "" "$3"
  failures=$((failures + 1))
}

# die LEG EXPECTED FOUND: fail, then stop; nothing after this can be trusted.
die() {
  fail "$1" "$2" "$3"
  exit 1
}

echo "run: base $base (ring $ring, internode $internode, driver $driver), scratch $scratch"
echo "building the server and the CLI"
(cd "$module" && go build -o "$scratch/$binary" ./cmd/server && go build -o "$scratch/hoardDB" ./cmd/cli)

# new_key_file FILE writes 32 random raw bytes at 0600.
new_key_file() {
  (umask 077 && head -c 32 /dev/urandom > "$1")
}
# The run's main key ends in 0x0a on purpose: 31 random bytes and a newline
# byte make a valid 32-byte raw key, which the server must read as it is
# rather than as 31 bytes plus an allowed trailing newline.
(umask 077 && head -c 31 /dev/urandom > "$scratch/kek" && printf '\n' >> "$scratch/kek")

# Markers are built so block compression cannot split one and hide it from
# grep (encryption-acceptance-markers.sh says why). ENCACC_MARKERS="m1 m2 m3 m4"
# is the selftest's seam for forcing a collision.
# shellcheck source=scripts/encryption-acceptance-markers.sh
source "$root/scripts/encryption-acceptance-markers.sh"
random_marker() { head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 18; }
read -r m1 m2 m3 m4 <<< "${ENCACC_MARKERS:-$(marker_set)}"
# m1: a document key, m2: an indexed field's value, m3: a document value,
# m4: a queue payload
root_password=$(random_marker)

# run_node DATA_DIR LOG [KEY=VALUE ...] starts the server in the background
# with this run's ports and the given extra environment, and records its PID.
run_node() {
  local data_dir=$1 log=$2
  shift 2
  own_dir "$data_dir"
  env -i PATH="$PATH" HOME="$HOME" \
    HOARDDB_DATA_DIR="$data_dir" HOARDDB_LISTEN="127.0.0.1:$ring" \
    HOARDDB_TCP_LISTEN="127.0.0.1:$driver" HOARDDB_TCP_INTERNODE_LISTEN="127.0.0.1:$internode" \
    HOARDDB_ROOT_PASSWORD="$root_password" HOARDDB_LOG_FORMAT=text \
    "$@" "$scratch/$binary" > "$log" 2>&1 &
  node_pid=$!
}

cli() {
  env -i PATH="$PATH" HOME="$HOME" HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD="$root_password" \
    "$scratch/hoardDB" --address "127.0.0.1:$driver" --insecure 2>&1
}

# await_listening LOG waits until the server is serving statements: the driver
# listener is up and a statement gets an answer. It fails if the server exits
# first. The log line alone is not enough: the first statement can race the
# server's bootstrap, and that used to surface as a marker that never landed.
await_listening() {
  local log=$1
  for _ in $(seq 1 150); do
    if grep -q "TCP driver listener listening" "$log"; then
      break
    fi
    pid_running "$node_pid" || { cat "$log" >&2; return 1; }
    sleep 0.1
  done
  grep -q "TCP driver listener listening" "$log" || { cat "$log" >&2; return 1; }
  for _ in $(seq 1 100); do
    printf 'show databases\n' | cli >/dev/null && return 0
    pid_running "$node_pid" || { cat "$log" >&2; return 1; }
    sleep 0.1
  done
  cat "$log" >&2
  return 1
}

# stop_node stops the server this run started, by PID: SIGTERM, ten seconds to
# exit, then it is reported as a failure and killed so nothing is left running.
stop_node() {
  [ -n "$node_pid" ] || return 0
  local stopped_pid=$node_pid
  kill -TERM "$stopped_pid" 2>/dev/null || true
  for _ in $(seq 1 100); do
    pid_running "$stopped_pid" || break
    sleep 0.1
  done
  if pid_running "$stopped_pid"; then
    kill_node_hard
    node_pid=""
    echo "the server (pid $stopped_pid) did not exit within 10 s of SIGTERM and was killed" >&2
    return 1
  fi
  wait "$stopped_pid" 2>/dev/null || true
  node_pid=""
}

# expect_refusal NAME DATA_DIR [KEY=VALUE ...]: the start must end in a
# non-zero exit without ever listening.
expect_refusal() {
  local leg_name=$1 data_dir=$2 refused_pid
  shift 2
  local log="$scratch/$leg_name.log"
  run_node "$data_dir" "$log" "$@"
  refused_pid=$node_pid
  for _ in $(seq 1 150); do
    pid_running "$refused_pid" || break
    sleep 0.1
  done
  if pid_running "$refused_pid" || grep -q "listener listening" "$log"; then
    stop_node
    fail "$leg_name" "the start is refused (exit without listening)" "the node started and listened"
    return
  fi
  wait "$refused_pid" 2>/dev/null || true
  node_pid=""
  pass "$leg_name" "refused: $(grep -m1 -o 'failed to prepare server.*' "$log" | cut -c1-160)"
}

# write_markers LABEL sends the marker statements and returns the CLI's exit
# status. ENCACC_INJECT_WRITE_FAILURE=LABEL is the selftest's seam: it points
# one insert at a bucket that does not exist, so the failure reaches the real
# CLI and the real detection below rather than a stub.
write_markers() {
  local label=$1
  local people_insert="db.people.insert({_id_: \"$m1\", Email: \"$m2@example.com\", Note: \"$m3\"})"
  if [ "${ENCACC_INJECT_WRITE_FAILURE:-}" = "$label" ]; then
    people_insert="db.nosuchbucket.insert({_id_: \"$m1\"})"
  fi
  printf '%s\n' \
    'create database accdb' \
    'use accdb' \
    'create bucket people { type: hash, indices: ["Email"] }' \
    "$people_insert" \
    'create bucket ledger { type: btree }' \
    "db.ledger.insert({_id_: \"$m1\", Note: \"$m3\"})" \
    'create bucket jobs { type: fifo }' \
    "db.jobs.push({payload: \"$m4\"})" | cli
}

# unacknowledged_write OUTPUT lists the first write the CLI did not acknowledge,
# or nothing. Each write is named and matched to the line the CLI prints for it;
# the CLI stops at its first error, so the first missing one is the culprit.
unacknowledged_write() {
  local output=$1 spec write_name needle minimum
  for spec in \
    "create database accdb|OK  created database accdb|1" \
    "create bucket people|OK  created bucket accdb.people|1" \
    "insert into people|OK  id=$m1|1" \
    "create bucket ledger|OK  created bucket accdb.ledger|1" \
    "insert into ledger|OK  id=$m1|2" \
    "create bucket jobs|OK  created bucket accdb.jobs|1" \
    "push to jobs|OK  id=|3"; do
    write_name=${spec%%|*}
    minimum=${spec##*|}
    needle=${spec#*|}
    needle=${needle%|*}
    if [ "$(printf '%s\n' "$output" | grep -cF -- "$needle")" -lt "$minimum" ]; then
      printf '%s' "$write_name"
      return 0
    fi
  done
}

# check_writes LEG NODE OUTPUT CLI_STATUS: pass the leg's write half, or fail it
# by naming the write. A grep result is never involved here.
check_writes() {
  local leg_name=$1 node_label=$2 output=$3 cli_status=$4 missing cli_error
  missing=$(unacknowledged_write "$output")
  if [ -z "$missing" ] && [ "$cli_status" = 0 ]; then
    return 0
  fi
  cli_error=$(printf '%s\n' "$output" | grep -m1 -E 'CLI error|error' || true)
  fail "$leg_name" "every write to the $node_label node acknowledged with an OK line" \
    "write \"${missing:-<none missing>}\" was not acknowledged (no OK line for it); CLI exit status $cli_status; ${cli_error:-no error line, output: $(printf '%s' "$output" | tail -3 | tr '\n' ' ')}"
  return 1
}

read_markers() {
  printf '%s\n' 'use accdb' "db.people.find(\"$m1\")" 'db.jobs.peek()' | cli
}

# hits DIR MARKER counts the files under DIR holding MARKER. -a because the
# store's files are binary and a grep that skips binary files proves nothing.
hits() { LC_ALL=C grep -a -r -l -F -- "$2" "$1" 2>/dev/null | wc -l; }

# The allowlist is one definition, read by this run and by the Go test that
# guards the WAL directory (server/wal_allowlist_test.go). See the helper.
# shellcheck source=scripts/encryption-acceptance-classify.sh
source "$root/scripts/encryption-acceptance-classify.sh"

# check_allowlist DIR prints every violation and returns non-zero on any.
check_allowlist() {
  local dir=$1 violations=0 rel type class
  while IFS= read -r entry; do
    type=${entry%% *}
    rel=${entry#* }
    class=$(sealed_dir="$dir" classify "$rel" "$type")
    case "$class" in
      UNMATCHED|*-violated|*-VIOLATED) echo "  $rel: $class" >&2; violations=$((violations + 1)) ;;
      known:*) known "allowlist" "$rel/ — $class: an empty directory named after a database" ;;
    esac
  done < <(cd "$dir" && find . -mindepth 1 \( -type d -printf 'd %P\n' \) -o \( -type f -printf 'f %P\n' \) -o \( ! -type d ! -type f -printf 'x %P\n' \))
  [ "$violations" = 0 ]
}

flip_last_byte() {
  local file=$1 size last
  size=$(stat -c %s "$file")
  last=$(tail -c 1 "$file" | od -An -tu1 | tr -d ' ')
  printf "$(printf '\\%03o' $(( (last ^ 1) & 255 )))" | dd of="$file" bs=1 seek=$((size - 1)) conv=notrunc status=none
}

sealed_env=(HOARDDB_ENCRYPTION_MODE=require HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_KEY_FILE="$scratch/kek")

# --- the blob leg (spec §4.2.1): --leg blob only --------------------------------

if [ "$leg" = blob ]; then
  blob_marker=$(random_marker)
  blob_sealed_dir="$scratch/blob-sealed"
  blob_plain_dir="$scratch/blob-plain"
  own_dir "$blob_sealed_dir"
  own_dir "$blob_plain_dir"

  # write_blob DIR MODE writes the bucket through the real start path at MODE.
  write_blob() {
    (cd "$module" && env HOARDDB_ENCRYPTION_ACCEPTANCE_BLOB_DIR="$1" HOARDDB_ENCRYPTION_ACCEPTANCE_BLOB_MODE="$2" \
       HOARDDB_ENCRYPTION_ACCEPTANCE_BLOB_KEY="$scratch/kek" HOARDDB_ENCRYPTION_ACCEPTANCE_BLOB_MARKER="$blob_marker" \
       go test -count=1 -v -run '^TestAcceptanceWriteBlob$' ./server/ 2>&1)
  }
  blob_payloads() { find "$1/blobs" -type f 2>/dev/null; }

  if blob_output=$(write_blob "$blob_sealed_dir" require) && printf '%s' "$blob_output" | grep -q -- '--- PASS: TestAcceptanceWriteBlob'; then
    pass "blob-writes" "a sealed node was started, wrote a file-backed blob and an inline one, and stopped"
  else
    die "blob-writes" "the sealed node writes a blob bucket" "$(printf '%s\n' "$blob_output" | tail -8)"
  fi

  blob_files=$(blob_payloads "$blob_sealed_dir" | wc -l)
  if [ "$blob_files" -ge 1 ] && check_allowlist "$blob_sealed_dir"; then
    pass "blob-allowlist" "$blob_files payload file(s); every path matches one class and keys/blobs.wrapped is a 0x08 envelope"
  else
    fail "blob-allowlist" "at least one payload file, and every path in the sealed directory matches exactly one class" "$blob_files payload file(s); violations listed above"
  fi

  if [ "$blob_files" -ge 1 ] && [ "$(hits "$blob_sealed_dir" "$blob_marker")" = 0 ]; then
    pass "blob-greps" "the marker is nowhere in the sealed directory, which holds $blob_files payload file(s)"
  else
    fail "blob-greps" "no marker in the sealed directory, and a payload file for the grep to look at" "marker files: $(hits "$blob_sealed_dir" "$blob_marker"); payload files: $blob_files"
  fi

  if blob_control=$(write_blob "$blob_plain_dir" off) && [ "$(hits "$blob_plain_dir/blobs" "$blob_marker")" -ge 1 ]; then
    pass "blob-control" "at mode off the marker is found in blobs/, so the grep above can see it"
  else
    fail "blob-control" "the marker is found in the plaintext node's blobs/" "marker files: $(hits "$blob_plain_dir/blobs" "$blob_marker"); writer: $(printf '%s\n' "${blob_control:-}" | tail -3)"
  fi

  run_node "$blob_sealed_dir" "$scratch/blob-read.log" "${sealed_env[@]}"
  if await_listening "$scratch/blob-read.log"; then
    blob_info=$(printf '%s\n' 'use accdb' 'db.uploads.info("report")' | cli) || true
    stop_node
    if printf '%s' "$blob_info" | grep -q 'Size:'; then
      pass "blob-read" "the real server opened the sealed directory and reported: $(printf '%s' "$blob_info" | grep -m1 'Size:')"
    else
      fail "blob-read" "info on the sealed blob reports its size" "$(printf '%s' "$blob_info" | tail -3 | tr '\n' ' ')"
    fi
  else
    stop_node
    fail "blob-read" "the server starts on the sealed directory" "it exited or never answered (log: $scratch/blob-read.log)"
  fi

  tamper_blob="$scratch/blob-tamper"
  own_dir "$tamper_blob"
  cp -a "$blob_sealed_dir" "$tamper_blob"
  flip_last_byte "$(blob_payloads "$tamper_blob" | head -1)"
  run_node "$tamper_blob" "$scratch/blob-tamper.log" "${sealed_env[@]}"
  if await_listening "$scratch/blob-tamper.log"; then
    tamper_info=$(printf '%s\n' 'use accdb' 'db.uploads.info("report")' | cli) || true
    stop_node
    if printf '%s' "$tamper_info" | grep -q 'failed authentication' && printf '%s' "$tamper_info" | grep -q 'accdb.uploads' && printf '%s' "$tamper_info" | grep -q '"report"'; then
      pass "blob-tamper" "a flipped byte was reported as damage, naming accdb.uploads and the key"
    else
      fail "blob-tamper" "damage to the payload, naming the database, the bucket and the key" "$(printf '%s' "$tamper_info" | tail -3 | tr '\n' ' ')"
    fi
  else
    stop_node
    fail "blob-tamper" "the server starts on a directory with a damaged payload (it is read on demand)" "it exited or never answered (log: $scratch/blob-tamper.log)"
  fi

  missing_registry="$scratch/blob-registry-missing"
  own_dir "$missing_registry"
  cp -a "$blob_sealed_dir" "$missing_registry"
  rm "$missing_registry/keys/blobs.wrapped"
  expect_refusal "blob-registry-missing" "$missing_registry" "${sealed_env[@]}"

  echo
  if [ "$failures" = 0 ]; then
    echo "encryption acceptance (blob leg): PASS"
    exit 0
  fi
  echo "encryption acceptance (blob leg): FAIL ($failures)"
  exit 1
fi

# --- the sealed node -------------------------------------------------------

run_node "$sealed_dir" "$scratch/sealed-first.log" "${sealed_env[@]}"
await_listening "$scratch/sealed-first.log" ||
  die "sealed-start" "the sealed node listening on driver port $driver" "it exited or never answered (its log is above)"
write_status=0
written=$(write_markers sealed) || write_status=$?
readback=$(read_markers) || true
stop_node

if check_writes "writes" "sealed" "$written" "$write_status"; then
  if printf '%s' "$readback" | grep -qF "$m3" && printf '%s' "$readback" | grep -qF "$m4"; then
    pass "writes" "all 7 writes were acknowledged by the sealed node and the markers read back"
  else
    fail "writes" "the document value and the queue payload read back from the sealed node" \
      "the writes were acknowledged but the read-back lacked them: $(printf '%s' "$readback" | tail -3 | tr '\n' ' ')"
  fi
fi

if sealed_class "$sealed_dir/keys/store.wrapped" 03 && grep -q "ENCRYPTION AT REST" "$scratch/sealed-first.log"; then
  pass "seal-on-first-touch" "keys/store.wrapped is a 0x03 envelope; the first-start banner was printed"
else
  fail "seal-on-first-touch" "keys/store.wrapped is a 0x03 envelope and the first-start banner was printed" "no sealed store key, or no first-start banner"
fi

if check_allowlist "$sealed_dir"; then
  pass "allowlist" "every path matches one class from rev 4 §1.2"
else
  fail "allowlist" "every path in the sealed directory matches exactly one class" "unmatched or violating paths listed above"
fi

selftest_dir="$scratch/selftest"
own_dir "$selftest_dir"
cp -a "$sealed_dir" "$selftest_dir"
echo "stray" > "$selftest_dir/stray.txt"
if check_allowlist "$selftest_dir" >/dev/null 2>&1; then
  fail "allowlist-selftest" "a stray file added to a copy fails the allowlist" "it passed: the allowlist does not fail closed"
else
  pass "allowlist-selftest" "a stray file fails the allowlist"
fi
rm -rf "$selftest_dir"

if structure=$(cd "$module" && HOARDDB_ENCRYPTION_ACCEPTANCE_DIR="$sealed_dir" go test -count=1 -v -run '^TestAcceptanceBadgerStructure$' ./server/ 2>&1) &&
   printf '%s' "$structure" | grep -q -- '--- PASS: TestAcceptanceBadgerStructure'; then
  pass "structure" "MANIFEST key ids, KEYREGISTRY refuses an empty key, every log file carries a key id"
else
  fail "structure" "--- PASS: TestAcceptanceBadgerStructure" "$(printf '%s\n' "$structure" | grep -E 'badger_assertion|--- (FAIL|SKIP)|^FAIL' | head -8)"
fi

leaked=""
for marker in "$m1" "$m2" "$m3" "$m4"; do
  [ "$(hits "$sealed_dir" "$marker")" = 0 ] || leaked="$leaked $marker"
done
kek_hex=$(od -An -tx1 -v "$scratch/kek" | tr -d ' \n')
kek_found=0
while IFS= read -r -d '' file; do
  if od -An -tx1 -v "$file" | tr -d ' \n' | grep -q "$kek_hex"; then kek_found=1; echo "  KEK bytes in $file" >&2; fi
done < <(find "$sealed_dir" -type f -print0)
if [ -z "$leaked" ] && [ "$kek_found" = 0 ]; then
  pass "greps" "no marker (M1-M4) and not the KEK anywhere in the sealed directory"
else
  fail "greps" "no marker and not the KEK anywhere in the sealed directory" "found in the sealed directory:$leaked (KEK: $kek_found)"
fi
wal_bytes=$(stat -c %s "$sealed_dir/wal/wal_00001.log")
known "wal-markers" "the WAL holds $wal_bytes bytes (its header): a node with no peer logs no entries, so row B's marker check needs a two-node run; in Go it is TestNewServiceSealsTheWAL"

tamper_dir() {
  local dir="$scratch/$1"
  own_dir "$dir"
  rm -rf "$dir"
  cp -a "$sealed_dir" "$dir"
  echo "$dir"
}

# --- escrow rehearsal: export, rewrap under a new KEK, import ---

escrow_dir=$(tamper_dir escrow)
escrow_key="$scratch/escrow-nm.key"
new_key_file "$scratch/escrow-kek2"

escrow_export_log="$scratch/escrow-export.log"
if env -i PATH="$PATH" HOME="$HOME" \
     HOARDDB_DATA_DIR="$escrow_dir" HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_MODE=require \
     HOARDDB_ENCRYPTION_KEY_FILE="$scratch/kek" HOARDDB_TCP_LISTEN="127.0.0.1:$driver" \
     "$scratch/$binary" -export-key "$escrow_key" > "$escrow_export_log" 2>&1
then
  if [ -f "$escrow_key" ] && [ "$(stat -c %a "$escrow_key")" = "600" ] && [ "$(stat -c %s "$escrow_key")" = 32 ]; then
    pass "escrow-export" "-export-key wrote a 32-byte key at 0600"
  else
    fail "escrow-export" "-export-key leaves a 32-byte file at 0600" "it exited 0 but the file is missing, or is not 32 bytes / mode 600"
  fi
else
  fail "escrow-export" "-export-key exits 0" "it failed: $(cat "$escrow_export_log")"
fi

escrow_import_log="$scratch/escrow-import.log"
if env -i PATH="$PATH" HOME="$HOME" \
     HOARDDB_DATA_DIR="$escrow_dir" HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_MODE=require \
     HOARDDB_ENCRYPTION_KEY_FILE="$scratch/escrow-kek2" HOARDDB_TCP_LISTEN="127.0.0.1:$driver" \
     "$scratch/$binary" -import-key "$escrow_key" -force > "$escrow_import_log" 2>&1
then
  pass "escrow-import" "-import-key -force rewrapped the node master under a new KEK"
else
  fail "escrow-import" "-import-key -force exits 0" "it failed: $(cat "$escrow_import_log")"
fi

# The rewrapped node master must still unwrap the same store key: start the
# node under the new KEK alone (the original KEK is gone from this run's
# env) and confirm it serves the markers written before the escrow.
run_node "$escrow_dir" "$scratch/escrow-restart.log" \
  HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_MODE=require HOARDDB_ENCRYPTION_KEY_FILE="$scratch/escrow-kek2"
if await_listening "$scratch/escrow-restart.log"; then
  escrow_readback=$(read_markers) || true
  stop_node
  if printf '%s' "$escrow_readback" | grep -qF "$m3"; then
    pass "escrow-restart" "the node restarted under the new KEK after export/import and served its markers"
  else
    fail "escrow-restart" "the node serves its markers under the new KEK" "it started but the read-back lacked them: $(printf '%s' "$escrow_readback" | tail -3 | tr '\n' ' ')"
  fi
else
  stop_node
  fail "escrow-restart" "the node starts under the new KEK after the escrow import" "it exited or never answered (log: $scratch/escrow-restart.log)"
fi
rm -rf "$escrow_dir"

dir=$(tamper_dir tamper-store-key)
flip_last_byte "$dir/keys/store.wrapped"
expect_refusal "tamper(store key)" "$dir" "${sealed_env[@]}"
dir=$(tamper_dir tamper-node-master)
flip_last_byte "$dir/keys/node-master.key.wrapped"
expect_refusal "tamper(node master)" "$dir" "${sealed_env[@]}"
dir=$(tamper_dir key-missing)
rm "$dir/keys/store.wrapped"
expect_refusal "key-missing" "$dir" "${sealed_env[@]}"
dir=$(tamper_dir mode-mismatch)
expect_refusal "mode-mismatch" "$dir"

# --- the plaintext node: positive control and zero config ------------------

run_node "$plain_dir" "$scratch/plain-first.log"
await_listening "$scratch/plain-first.log" ||
  die "plain-start" "the plaintext node listening on driver port $driver" "it exited or never answered (its log is above)"
control_status=0
control_output=$(write_markers plain) || control_status=$?
stop_node

# The control writes are checked on their own, so a marker that never landed is
# reported as a write failure and not as a grep that cannot see.
control_landed=0
if check_writes "control-writes" "plaintext" "$control_output" "$control_status"; then
  control_landed=1
  pass "control-writes" "all 7 writes were acknowledged by the plaintext node"
fi

if [ "$control_landed" = 0 ]; then
  fail "positive-control" "every marker found in the plaintext store" \
    "not evaluated: the plaintext writes failed (see control-writes), so nothing is known about whether the greps can see"
else
  blind=""
  split=""
  for marker in "$m1" "$m2" "$m3" "$m4"; do
    [ "$(hits "$plain_dir/store" "$marker")" -ge 1 ] && continue
    blind="$blind $marker"
    # A marker missing a few characters at one end is still there, cut by the
    # store's block compression (a back-reference ate its edge): say so, since
    # that is not a blind grep.
    for cut in 1 2 3 4; do
      if [ "$(hits "$plain_dir/store" "${marker:cut}")" -ge 1 ] || [ "$(hits "$plain_dir/store" "${marker:0:18-cut}")" -ge 1 ]; then
        split="$split $marker"
        break
      fi
    done
  done
  kek_sees_itself=0
  od -An -tx1 -v "$scratch/kek" | tr -d ' \n' | grep -q "$kek_hex" && kek_sees_itself=1
  if [ -n "$split" ]; then
    fail "positive-control" "every marker stored contiguously in the plaintext store" \
      "the store's block compression split${split} (all but its first or last few characters is present): the markers were not built apart (encryption-acceptance-markers.sh), the greps are not shown blind"
  elif [ -z "$blind" ] && [ "$kek_sees_itself" = 1 ]; then
    pass "positive-control" "every marker (M1-M4) is found in the plaintext store and the KEK matcher finds the KEK in its own file, so the sealed greps can see what they look for"
  else
    fail "positive-control" "every marker found in the plaintext store and the KEK found in its own file" \
      "the writes were acknowledged but the plaintext store hides:${blind:- none} (KEK matcher sees the KEK: $kek_sees_itself): the greps are blind"
  fi
fi

run_node "$plain_dir" "$scratch/plain-restart.log"
await_listening "$scratch/plain-restart.log" ||
  die "zero-config" "the plaintext node restarts and listens" "it exited or never answered (its log is above)"
readback=$(read_markers) || true
stop_node
if printf '%s' "$readback" | grep -qF "$m3" && ! grep -q "ENCRYPTION AT REST" "$scratch/plain-first.log"; then
  pass "zero-config" "a node with no encryption settings serves its markers after a restart, with no banner"
else
  fail "zero-config" "the plaintext node serves its markers after a restart, with no banner" "the read-back lacked them, or a banner was printed: $(printf '%s' "$readback" | tail -3 | tr '\n' ' ')"
fi

echo
if [ "$failures" = 0 ]; then
  echo "encryption acceptance (Stage 1): PASS"
  exit 0
fi
echo "encryption acceptance (Stage 1): FAIL ($failures)"
exit 1
