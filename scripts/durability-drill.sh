#!/usr/bin/env bash
#
# durability-drill.sh - kill -9 a node in the middle of a write load, restart it
# on the same data directory, and prove every ACKNOWLEDGED write is still there
# (release checklist VER-2).
#
# It builds the server and the drill client (hoardDB/cmd/durabilitydrill),
# starts one real hoarddb-server process on loopback with its data directory on
# a real disk, and runs rounds on that one directory. A round:
#
#   1. writers insert documents, each with an id no other write has, and record
#      every write in a ledger: "acked" only when the server answered "applied",
#      "unknown" for everything else (a lost connection, a passed deadline, a
#      refusal). An unknown write is never counted as acknowledged.
#   2. after a random delay the server gets SIGKILL, with the writers still
#      writing.
#   3. the server is started again on the same data directory and the drill
#      waits until it serves.
#   4. every id of the ledger, from this round and every round before it, is
#      read back with a Get, and the whole bucket is scanned. The judge prints:
#        acknowledged          writes the server answered "applied" for
#        present               of those, read back whole
#        missing               of those, NOT_FOUND            (must be 0)
#        unknown and present   writes with no answer that did land
#        unknown and absent    writes with no answer that did not
#        duplicates            ids the scan lists twice       (must be 0)
#        never sent            documents not in the ledger    (must be 0)
#      and also: changed body, could not be read, found but not listed (each
#      must be 0), and the server's own count beside the scan's.
#
# The next round writes more to the same directory, so a write acknowledged in
# round 1 is read back after every later kill as well.
#
# Legs: plain (a node with default settings) and encrypted (the same with
# encryption at rest on, a keyfile keystore, the key outside the data
# directory). The server runs with default durability settings: it is started
# with an empty environment, so no setting of yours reaches it.
#
# What this does not prove: kill -9 ends the process and leaves the kernel's
# page cache alone, so a write the server handed to the kernel without an fsync
# survives it. This drill shows no acknowledged write is lost when the server
# process dies. It does not show that for a power loss or a kernel crash.
#
# The data directory must be on a real disk: on tmpfs or ramfs the drill refuses
# to start (exit 2), because a directory in memory keeps every write whatever
# the server does.
#
# A run signals only the PIDs it started, owns its scratch directory (made
# inside --dir, marked .durability-drill; nothing outside it is changed), picks
# free loopback ports unless --base is given, removes the scratch on success
# and keeps it, with the ledger and the server log, on failure or with --keep.
#
# Usage:
#   scripts/durability-drill.sh                                      # both legs, 5 rounds, 4 writers, under /var/tmp
#   scripts/durability-drill.sh --dir /var/tmp --rounds 5 --writers 4
#   scripts/durability-drill.sh --leg plain --rounds 3               # the plain node only, 3 rounds
#   scripts/durability-drill.sh --min-delay 5 --max-delay 20         # kill 5 to 20 s into each round's load
#   scripts/durability-drill.sh --bin-dir /opt/hoarddb/bin           # use hoarddb-server and durabilitydrill from there (no Go needed)
#   scripts/durability-drill.sh --base 27400 --keep                  # ports 27400 to 27419, keep the scratch
#   scripts/durability-drill.sh --inject-loss --leg plain --rounds 1 # proof the drill can fail: deletes one
#                                                                    # acknowledged document before the read; must exit 1
#
# Options:
#   --dir DIR        where the scratch directory is made; must be on a real disk (default /var/tmp)
#   --rounds K       kill-and-restart rounds per leg (default 5)
#   --writers N      concurrent writers, each on its own connection (default 4)
#   --min-delay S    earliest kill, whole seconds into a round's load (default 2)
#   --max-delay S    latest kill (default 6)
#   --leg LEG        plain, encrypted, or all (default all)
#   --bin-dir DIR    take hoarddb-server and durabilitydrill from DIR instead of building them
#   --base PORT      first of 20 loopback ports (default: a free range is chosen)
#   --keep           keep the scratch directory after a pass
#   --inject-loss    delete one acknowledged document after the first restart; the drill must then FAIL
#
# Exits 0 when every round of every leg passes, 1 when an acknowledged write is
# missing or any other check fails, 2 on misuse or a --dir that is not on a disk.
set -euo pipefail

dir=/var/tmp
rounds=5
writers=4
min_delay=2
max_delay=6
leg=all
bin_dir=""
base=""
keep=0
inject_loss=0

usage() { sed -n '3,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }
misuse() {
  echo "durability-drill.sh: $1" >&2
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dir) dir=${2:-}; shift 2 || misuse "--dir needs a directory, for example: scripts/durability-drill.sh --dir /var/tmp" ;;
    --rounds) rounds=${2:-}; shift 2 || misuse "--rounds needs a number, for example: scripts/durability-drill.sh --rounds 5" ;;
    --writers) writers=${2:-}; shift 2 || misuse "--writers needs a number, for example: scripts/durability-drill.sh --writers 4" ;;
    --min-delay) min_delay=${2:-}; shift 2 || misuse "--min-delay needs whole seconds, for example: scripts/durability-drill.sh --min-delay 2" ;;
    --max-delay) max_delay=${2:-}; shift 2 || misuse "--max-delay needs whole seconds, for example: scripts/durability-drill.sh --max-delay 6" ;;
    --leg) leg=${2:-}; shift 2 || misuse "--leg needs plain, encrypted or all, for example: scripts/durability-drill.sh --leg plain" ;;
    --bin-dir) bin_dir=${2:-}; shift 2 || misuse "--bin-dir needs a directory, for example: scripts/durability-drill.sh --bin-dir /opt/hoarddb/bin" ;;
    --base) base=${2:-}; shift 2 || misuse "--base needs a port, for example: scripts/durability-drill.sh --base 27400" ;;
    --keep) keep=1; shift ;;
    --inject-loss) inject_loss=1; shift ;;
    -h | --help) usage; exit 0 ;;
    *) misuse "unknown argument: $1 (run scripts/durability-drill.sh --help for the usage)" ;;
  esac
done

root=$(cd "$(dirname "$0")/.." && pwd)
module="$root/hoardDB"
[ -f "$module/go.mod" ] || module="$root" # the published tree is the module itself
unset HOARDDB_DATA_DIR
# shellcheck source=scripts/durability-drill-lib.sh
source "$root/scripts/durability-drill-lib.sh"

case "$leg" in
  all | plain | encrypted) ;;
  *) misuse "unknown leg \"$leg\": use plain, encrypted or all, for example: scripts/durability-drill.sh --leg plain" ;;
esac
drill_is_count "$rounds" || misuse "--rounds needs a number of at least 1, got \"$rounds\"; for example: scripts/durability-drill.sh --rounds 5"
drill_is_count "$writers" || misuse "--writers needs a number of at least 1, got \"$writers\"; for example: scripts/durability-drill.sh --writers 4"
drill_is_count "$min_delay" || misuse "--min-delay needs whole seconds of at least 1, got \"$min_delay\"; for example: scripts/durability-drill.sh --min-delay 2"
drill_is_count "$max_delay" || misuse "--max-delay needs whole seconds of at least 1, got \"$max_delay\"; for example: scripts/durability-drill.sh --max-delay 6"
[ "$min_delay" -le "$max_delay" ] || misuse "--min-delay $min_delay is after --max-delay $max_delay; for example: scripts/durability-drill.sh --min-delay 2 --max-delay 6"
# Ports: leg n (0 plain, 1 encrypted) uses base+10n (ring and peer address), +1
# (the earlier engine's internode listener) and +2 (clients).
port_span=20
if [ -n "$base" ]; then
  drill_is_count "$base" || misuse "--base needs a port number, got \"$base\"; for example: scripts/durability-drill.sh --base 27400"
  if [ "$base" -lt 1024 ] || [ $((base + port_span)) -gt 65535 ]; then
    misuse "--base $base: the run needs ports $base to $((base + port_span - 1)); pick a base from 1024 to $((65535 - port_span)), for example: scripts/durability-drill.sh --base 27400"
  fi
fi
if ! drill_on_disk "$dir"; then
  drill_disk_refusal "$dir" >&2
  exit 2
fi
if [ -n "$bin_dir" ]; then
  for binary in hoarddb-server durabilitydrill; do
    [ -x "$bin_dir/$binary" ] || misuse "--bin-dir $bin_dir has no executable $binary. Build both there first: (cd hoardDB && go build -o $bin_dir/hoarddb-server ./cmd/server && go build -o $bin_dir/durabilitydrill ./cmd/durabilitydrill), or leave --bin-dir out and scripts/durability-drill.sh builds them"
  done
fi

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
  [ -n "$base" ] || { echo "no free run of $port_span ports found in 20000-48999; pass one: scripts/durability-drill.sh --base 27400" >&2; exit 1; }
elif ! range_free "$base"; then
  echo "a port in $base to $((base + port_span - 1)) is in use; pick another --base, or leave it out and a free range is chosen" >&2
  exit 1
fi

scratch=$(mktemp -d "$dir/hoarddb-durability-drill.XXXXXX")
touch "$scratch/.durability-drill"
declare -A pids=()
writer_pid=""

pid_running() {
  local state
  state=$(ps -o stat= -p "$1" 2>/dev/null || true)
  [ -n "$state" ] && [ "${state#Z}" = "$state" ]
}

# stop_pid PID SIGNAL: signal one process this run started, wait up to 15 s,
# then SIGKILL it; reap it either way.
stop_pid() {
  local pid=$1 signal=$2
  kill "-$signal" "$pid" 2>/dev/null || true
  for _ in $(seq 1 150); do pid_running "$pid" || break; sleep 0.1; done
  if pid_running "$pid"; then
    echo "pid $pid did not exit within 15 s of SIG$signal; killing it" >&2
    kill -KILL "$pid" 2>/dev/null || true
    for _ in $(seq 1 50); do pid_running "$pid" || break; sleep 0.1; done
  fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  local status=$? name
  trap - EXIT INT TERM
  [ -z "$writer_pid" ] || stop_pid "$writer_pid" TERM
  for name in "${!pids[@]}"; do stop_pid "${pids[$name]}" KILL; done
  if [ "$keep" = 1 ] || [ "$status" != 0 ]; then
    echo "scratch kept for inspection (data directories, ledgers, server logs): $scratch"
  elif [ -f "$scratch/.durability-drill" ]; then
    rm -rf "$scratch"
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

failures=0
pass() { printf 'PASS  %-18s %s\n' "$1" "$2"; }
fail() {
  printf 'FAIL  %-18s expected: %s\n' "$1" "$2"
  printf '      %-18s found: %s\n' "" "$3"
  failures=$((failures + 1))
}

echo "run: data on $(drill_fstype_name "$dir") under $scratch, ports $base to $((base + port_span - 1)), $rounds rounds, $writers writers, kill $min_delay to $max_delay s into each load"
echo "note: kill -9 leaves the kernel's page cache alone; this shows what survives the server process dying, not a power loss"
if [ -n "$bin_dir" ]; then
  server_bin="$bin_dir/hoarddb-server"
  drill_bin="$bin_dir/durabilitydrill"
  echo "binaries: $server_bin and $drill_bin (not built by this run)"
else
  server_bin="$scratch/hoarddb-server"
  drill_bin="$scratch/durabilitydrill"
  echo "building the server and the drill client"
  (cd "$module" && go build -o "$server_bin" ./cmd/server && go build -o "$drill_bin" ./cmd/durabilitydrill)
fi

root_password=$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 20)
ring_port() { echo $((base + 10 * $1)); }
internode_port() { echo $((base + 10 * $1 + 1)); }
client_port() { echo $((base + 10 * $1 + 2)); }

# start_node NAME N DATA_DIR LOG [KEY=VALUE ...]: start leg N's server in the
# background with an empty environment plus the lines given.
start_node() {
  local name=$1 n=$2 data_dir=$3 log=$4
  shift 4
  env -i PATH="$PATH" HOME="$HOME" HOARDDB_DATA_DIR="$data_dir" \
    HOARDDB_LISTEN="127.0.0.1:$(ring_port "$n")" HOARDDB_TCP_INTERNODE_LISTEN="127.0.0.1:$(internode_port "$n")" \
    HOARDDB_TCP_LISTEN="127.0.0.1:$(client_port "$n")" HOARDDB_ROOT_USER=admin HOARDDB_ROOT_PASSWORD="$root_password" \
    HOARDDB_LOG_FORMAT=text "$@" "$server_bin" >>"$log" 2>&1 &
  pids[$name]=$!
}

# drill N COMMAND [ARGS...]: run the drill client against leg N's node. Its
# stderr (one TLS notice per connection, and any error) goes to drill.err.
drill() {
  local n=$1 command=$2
  shift 2
  env -i PATH="$PATH" HOME="$HOME" DURABILITY_DRILL_USER=admin DURABILITY_DRILL_PASSWORD="$root_password" \
    "$drill_bin" "$command" --addr "127.0.0.1:$(client_port "$n")" --insecure "$@"
}

# drill_errors FILE: what the drill client said on stderr, without the notice
# it prints for every connection made with --insecure.
drill_errors() {
  grep -v 'TLS certificate verification disabled' "$1" 2>/dev/null | tail -n 5 | tr '\n' ' ' || true
}

# await_ready NAME N LOG ERR: wait until leg N's node serves.
await_ready() {
  local name=$1 n=$2 log=$3 err=$4
  if drill "$n" ready --wait 120s >/dev/null 2>"$err"; then return 0; fi
  echo "the node did not serve: $(drill_errors "$err")" >&2
  if pid_running "${pids[$name]}"; then echo "its process (pid ${pids[$name]}) is still running" >&2; else echo "its process has exited" >&2; fi
  tail -n 20 "$log" >&2
  return 1
}

# run_leg NAME N [KEY=VALUE ...]: the rounds of one leg. Returns non-zero as
# soon as a round fails; the scratch is then kept.
run_leg() {
  local name=$1 n=$2
  shift 2
  local leg_dir="$scratch/$name" data_dir="$scratch/$name/data" log="$scratch/$name/server.log" ledger="$scratch/$name/ledger.csv"
  local err="$scratch/$name/drill.err" round tenths delay summary acknowledged in_flight report status killed create deleted
  mkdir -p "$data_dir"
  echo
  echo "=== leg $name: $rounds rounds on $data_dir"
  start_node "$name" "$n" "$data_dir" "$log" "$@"
  await_ready "$name" "$n" "$log" "$err" || { fail "$name-start" "the node serves" "it did not ($log)"; return 1; }
  if [ "$name" = encrypted ] && ! grep -q 'ring store and log encrypted at rest' "$log"; then
    fail "$name-start" "the start line 'ring store and log encrypted at rest'" "not in $log, so this leg would repeat the plain one"
    return 1
  fi
  for ((round = 1; round <= rounds; round++)); do
    tenths=$(drill_delay_tenths "$min_delay" "$max_delay")
    delay="$((tenths / 10)).$((tenths % 10))"
    create=()
    [ "$round" != 1 ] || create=(--create)
    drill "$n" write --ledger "$ledger" --round "$round" --writers "$writers" "${create[@]}" >"$leg_dir/writer-$round.out" 2>"$leg_dir/writer-$round.err" &
    writer_pid=$!
    sleep "$delay"
    if ! pid_running "$writer_pid"; then
      wait "$writer_pid" 2>/dev/null || true
      writer_pid=""
      fail "$name-round-$round" "the writers still writing $delay s into the load, so the kill lands mid-write" \
        "the load had already ended: $(cat "$leg_dir/writer-$round.out") $(drill_errors "$leg_dir/writer-$round.err")"
      return 1
    fi
    killed=${pids[$name]}
    kill -KILL "$killed"
    wait "$killed" 2>/dev/null || true
    unset "pids[$name]"
    if pid_running "$killed"; then
      fail "$name-round-$round" "the server (pid $killed) gone after SIGKILL" "it is still running"
      return 1
    fi
    # The writers end by themselves once their connections are lost.
    for _ in $(seq 1 300); do pid_running "$writer_pid" || break; sleep 0.1; done
    if pid_running "$writer_pid"; then kill -TERM "$writer_pid" 2>/dev/null || true; fi
    status=0
    wait "$writer_pid" || status=$?
    writer_pid=""
    summary=$(cat "$leg_dir/writer-$round.out")
    acknowledged=$(drill_writer_count acknowledged "$summary")
    in_flight=$(drill_writer_count unknown "$summary")
    if [ "$status" != 0 ] || ! drill_is_count "$acknowledged"; then
      fail "$name-round-$round" "the writers to record at least one acknowledged write before the kill" \
        "exit $status: ${summary:-no summary} $(drill_errors "$leg_dir/writer-$round.err")"
      return 1
    fi
    echo "--- leg $name, round $round of $rounds: SIGKILL of pid $killed after $delay s of load; $summary"

    start_node "$name" "$n" "$data_dir" "$log" "$@"
    if ! await_ready "$name" "$n" "$log" "$err"; then
      fail "$name-round-$round" "the node restarts on the same data directory and serves" "it did not ($log)"
      return 1
    fi
    if [ "$inject_loss" = 1 ] && [ "$round" = 1 ]; then
      deleted=$(drill "$n" delete-acknowledged --ledger "$ledger" 2>"$err") || { fail "$name-inject" "one acknowledged document deleted" "$(drill_errors "$err")"; return 1; }
      echo "INJECTED LOSS (--inject-loss): $deleted through the client. The drill must now FAIL and name that id."
    fi
    if ! drill "$n" read --ledger "$ledger" --out "$leg_dir/seen-$round.csv" >"$leg_dir/read-$round.out" 2>"$err"; then
      fail "$name-round-$round" "every ledger id read back and the bucket scanned" "$(drill_errors "$err")"
      return 1
    fi
    status=0
    report=$("$drill_bin" judge --ledger "$ledger" --observations "$leg_dir/seen-$round.csv" 2>&1) || status=$?
    printf '%s\n' "$report"
    if ! drill_report_passes "$status" "$report"; then
      fail "$name-round-$round" "every acknowledged write present after the restart, no duplicate, nothing never sent" \
        "$(grep '^RESULT' <<<"$report" || echo "no RESULT line; the judge exited $status") Ledger: $ledger. Read back: $leg_dir/seen-$round.csv. Run the judge again: $drill_bin judge --ledger $ledger --observations $leg_dir/seen-$round.csv"
      return 1
    fi
    pass "$name-round-$round" "$(drill_result_count present "$report") of $(drill_result_count acknowledged "$report") acknowledged writes present (all rounds so far), missing 0, duplicates 0, never sent 0; ${in_flight:-0} in flight at this kill"
  done
  stop_pid "${pids[$name]}" TERM
  unset "pids[$name]"
  leg_totals="$leg_totals $name: $(drill_result_count acknowledged "$report") acknowledged, $(drill_result_count present "$report") present, $(drill_result_count missing "$report") missing, $(drill_result_count unknown "$report") unknown ($(drill_result_count 'unknown and present' "$report") present, $(drill_result_count 'unknown and absent' "$report") absent), $(drill_result_count duplicates "$report") duplicates, $(drill_result_count 'never sent' "$report") never sent over $rounds kills;"
}

leg_totals=""
plain_leg() { run_leg plain 0; }
encrypted_leg() {
  local kek="$scratch/encrypted.kek"
  (umask 077 && head -c 32 /dev/urandom >"$kek")
  run_leg encrypted 1 HOARDDB_ENCRYPTION_MODE=on HOARDDB_ENCRYPTION_KEYSTORE=keyfile HOARDDB_ENCRYPTION_KEY_FILE="$kek"
}

case "$leg" in
  plain) plain_leg || true ;;
  encrypted) encrypted_leg || true ;;
  all)
    plain_leg || true
    encrypted_leg || true
    ;;
esac

echo
if [ "$failures" -eq 0 ] && [ -n "$leg_totals" ]; then
  echo "durability drill ($leg): PASS -${leg_totals%;}"
  exit 0
fi
echo "durability drill ($leg): FAIL ($failures)${leg_totals:+ - legs that finished:${leg_totals%;}}"
echo "If the failure is a missing acknowledged write, that is lost data: keep $scratch and file it under docs/bugs/ with the ledger, the seen-N.csv of the failing round and the server log."
exit 1
