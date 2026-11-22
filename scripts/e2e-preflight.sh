#!/usr/bin/env bash
#
# e2e-preflight.sh — give an end-to-end run its own ports, scratch directory and
# process name, and refuse to start on a port another process already holds.
#
# Why this exists: the defaults (:7433 driver, :4434 internode) are one pair per
# host, so two agents running e2e at the same time — or one server left behind by
# an earlier session — collide. The symptom is not a clean failure: the freshly
# built binary exits on "bind: address already in use" while the CLI connects to
# the OLD process and dies authenticating against the new data dir's root user,
# whose password is only a hash in users.json. One verification lost
# 20 minutes to exactly that, and reported it as a product failure.
#
# Usage:
#   scripts/e2e-preflight.sh                      # is the default pair free?
#   scripts/e2e-preflight.sh --name alice         # this run's own scratch + name
#   scripts/e2e-preflight.sh --base 17433 --name alice   # ... and its own ports
#   scripts/e2e-preflight.sh --name alice --stop  # stop this run's server
#
# --base N gives a coherent per-run triple: ring identity N, internode N+1 (the
# server derives it from the ring address), driver N+100. Set the three
# explicitly with --ring/--internode/--driver if you prefer, but a mismatched
# triple is a collision waiting to happen.
#
# Exits 0 with an `export` block to use, 1 when a port is held, 2 on misuse.
set -euo pipefail

name=${HOARDDB_E2E_NAME:-e2e}
driver=7433
internode=4434
ring=4433
stop=0
allow_own=0
base=

usage() { sed -n '3,22p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --name)      name=${2:-}; shift 2 ;;
    --base)      base=${2:-}; shift 2 ;;
    --driver)    driver=${2:-}; shift 2 ;;
    --internode) internode=${2:-}; shift 2 ;;
    --ring)      ring=${2:-}; shift 2 ;;
    --allow-own) allow_own=1; shift ;;
    --stop)      stop=1; shift ;;
    -h|--help)   usage; exit 0 ;;
    *)           echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ -n "$base" ]; then
  case "$base" in
    ''|*[!0-9]*) echo "--base needs a port number, got \"$base\"" >&2; exit 2 ;;
  esac
  ring=$base
  internode=$((base + 1))
  driver=$((base + 100))
fi

for value in "$name" "$driver" "$internode" "$ring"; do
  if [ -z "$value" ]; then
    echo "every option needs a value (--name, --base, --driver, --internode, --ring)" >&2
    exit 2
  fi
done

# pkill -x matches the process name, which the kernel truncates to 15 characters.
# A longer name matches NOTHING while looking correct, so teardown silently fails
# and the next run finds the port held.
binary="hdb-$name"
if [ "${#binary}" -gt 15 ]; then
  echo "name \"$name\" gives the process name \"$binary\" (${#binary} characters)." >&2
  echo "pkill -x only matches up to 15 characters, so teardown would silently stop" >&2
  echo "matching nothing and the server would keep the port. Pick a shorter --name." >&2
  exit 2
fi

scratch="${TMPDIR:-/tmp}/hoarddb-e2e-$name"

# Listeners on a TCP port, as "pid comm" lines. Empty output means free.
port_holders() {
  local port=$1 pid comm
  ss -ltnpH "sport = :$port" 2>/dev/null | grep -o 'pid=[0-9]*' | cut -d= -f2 | sort -u |
    while read -r pid; do
      comm=$(ps -o comm= -p "$pid" 2>/dev/null || true)
      printf '%s %s\n' "$pid" "${comm:-unknown}"
    done
}

if [ "$stop" = 1 ]; then
  if ! pgrep -x "$binary" >/dev/null; then
    echo "nothing to stop: no process named \"$binary\" (run name: $name)."
    exit 0
  fi
  pkill -x "$binary"
  for _ in $(seq 1 50); do
    pgrep -x "$binary" >/dev/null || break
    sleep 0.1
  done
  if pgrep -x "$binary" >/dev/null; then
    echo "stopped nothing: \"$binary\" is still running (pid $(pgrep -x "$binary" | tr '\n' ' '))." >&2
    exit 1
  fi
  echo "stopped $binary"
  exit 0
fi

held=0
own_ok=0
for port in "$driver" "$internode"; do
  while read -r pid comm; do
    [ -n "$pid" ] || continue
    if [ "$comm" = "$binary" ] && [ "$allow_own" = 1 ]; then
      echo "port $port is held by this run's own server (pid $pid, $comm) — allowed by --allow-own"
      own_ok=1
      continue
    fi
    if [ "$comm" = "$binary" ]; then
      echo "port $port is held by pid $pid ($comm): the process name matches this run, so it is" >&2
      echo "a leftover from an earlier run with the same --name. Clean it up first:" >&2
      echo "    scripts/e2e-preflight.sh --name $name --stop" >&2
    else
      echo "port $port is held by pid $pid ($comm) — not this run's server (which would be $binary)." >&2
      echo "The fresh binary would exit on \"bind: address already in use\" while the CLI talked to" >&2
      echo "THAT process. Either stop it, or give this run its own triple:" >&2
      echo "    scripts/e2e-preflight.sh --name <name> --base <port>" >&2
    fi
    held=1
  done < <(port_holders "$port")
done

if [ "$held" = 1 ]; then
  exit 1
fi

if [ "$own_ok" = 1 ]; then
  echo "ports $driver (driver) and $internode (internode) are held by this run's own server;"
  echo "$ring stays an identity only"
else
  echo "ports $driver (driver) and $internode (internode) are free; $ring stays an identity only"
fi
echo "scratch directory: $scratch"
echo
echo "# this run's environment — the server binary must be named $binary (<=15 chars, so"
echo "# pkill -x \"$binary\" works and cannot match another agent's server by accident):"
echo "export HOARDDB_DATA_DIR=$scratch/data"
echo "export HOARDDB_LISTEN=127.0.0.1:$ring"
echo "export HOARDDB_TCP_LISTEN=127.0.0.1:$driver"
echo "export HOARDDB_TCP_INTERNODE_LISTEN=127.0.0.1:$internode"
echo "export HOARDDB_E2E_SCRATCH=$scratch"
echo "export HOARDDB_E2E_BINARY=$scratch/$binary"
# The server keeps only the root password's hash, so the run supplies
# the password: the server seeds root with it and the CLI logs in with it.
echo "export HOARDDB_ROOT_USER=admin"
echo "export HOARDDB_ROOT_PASSWORD=e2e-$(head -c 9 /dev/urandom | od -An -tx1 | tr -d ' \n')"
echo
echo "# then, with those exported:"
echo "#   mkdir -p \"\$HOARDDB_DATA_DIR\" && go build -o \"\$HOARDDB_E2E_BINARY\" ./cmd/server"
echo "#   \"\$HOARDDB_E2E_BINARY\"          # background it; \$HOARDDB_E2E_BINARY is what pkill -x matches"
echo "#   scripts/e2e-preflight.sh --name $name --stop"
