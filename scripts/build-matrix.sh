#!/usr/bin/env bash
# scripts/build-matrix.sh — the platform matrix build (GAP-25, REL-5's
# "binaries for the supported platforms").
#
# Cross-builds hoarddb-server and hoarddb for every platform in the
# supported matrix, asserts each artifact's architecture with `file` (so a
# build that silently produces the host architecture — the exact failure
# mode a cross-compile flag typo causes — fails loudly instead of shipping
# quietly wrong), and writes SHA-256 checksums for whatever it produced.
#
# The checksums step is a SEAM, not a release: this script never publishes
# anything. REL-5's own release mechanics (the tag cut, the checksum
# publication alongside a GitHub release, the release checklist) read
# dist/checksums.txt and dist/*, they are not generated here. Do not extend
# this script to do that — that is REL-5's dispatch, not this one's.
#
# Usage:
#   scripts/build-matrix.sh            build every platform, assert, checksum
#   scripts/build-matrix.sh --list     print the platform matrix and exit
#
# Exit status: non-zero if any platform's server OR cli build fails, if
# any built artifact's `file` output does not name the expected architecture, or
# if the cross-platform vet step fails for darwin/arm64 or freebsd/amd64.
# Every platform in the matrix is attempted even after an earlier one fails,
# so one failure does not hide the others — see run_all below.
#
# No new dependencies: go, file, sha256sum (or shasum -a 256 on a machine
# without sha256sum, e.g. macOS). CGO_ENABLED=0 throughout; Badger is pure
# Go, so this is expected to work for all four (see the header of
# scripts/ci.sh's caller and the dispatch guard it was written against).

set -uo pipefail

# Layout-aware: the private repository keeps the module in hoardDB/, the
# published tree (scripts/publish-tree.sh) IS the module. No git needed.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_MODULE_DIR="$REPO_ROOT/hoardDB"
[ -f "$GO_MODULE_DIR/go.mod" ] || GO_MODULE_DIR="$REPO_ROOT"
DIST_DIR="$REPO_ROOT/dist"

RED=$'\033[0;31m'
GREEN=$'\033[0;32m'
YELLOW=$'\033[1;33m'
NC=$'\033[0m'

# The matrix (GAP-25, publish-checklist.md GAP-25 row; market study A-1/G-7).
# GOOS/GOARCH/expected-`file`-substring, one row per supported platform.
PLATFORMS=(
    "linux:amd64:ELF 64-bit LSB executable, x86-64"
    "linux:arm64:ELF 64-bit LSB executable, ARM aarch64"
    "darwin:arm64:Mach-O 64-bit arm64 executable"
    "darwin:amd64:Mach-O 64-bit x86_64 executable"
)

BINARIES=(hoarddb-server:./cmd/server hoarddb:./cmd/cli)

if [ "${1:-}" = "--list" ]; then
    for row in "${PLATFORMS[@]}"; do
        IFS=':' read -r goos goarch _ <<<"$row"
        echo "${goos}/${goarch}"
    done
    exit 0
fi

for tool in go file; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "${RED}build-matrix.sh: required tool '${tool}' not found on PATH — SKIPPING the platform matrix build gate${NC}" >&2
        echo "${RED}  install it and re-run; gates 1-10 in scripts/ci.sh are unaffected by this${NC}" >&2
        exit 1
    fi
done

SHA256_CMD=()
if command -v sha256sum >/dev/null 2>&1; then
    SHA256_CMD=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
    SHA256_CMD=(shasum -a 256)
else
    echo "${RED}build-matrix.sh: neither sha256sum nor shasum found on PATH — SKIPPING${NC}" >&2
    exit 1
fi

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

FAILURES=0
BUILT_ARTIFACTS=()

# build_one GOOS GOARCH NAME PKG
# Builds one binary, reporting (and counting, not exiting on) failure so
# every platform in the matrix is attempted — the dispatch guard is explicit
# that a failing target must be reported, never silently dropped.
build_one() {
    local goos="$1" goarch="$2" name="$3" pkg="$4"
    local out="$DIST_DIR/${name}-${goos}-${goarch}"
    echo "  building ${name} for ${goos}/${goarch}..."
    if ! (cd "$GO_MODULE_DIR" && CGO_ENABLED=0 GOOS="$goos" GOARCH="$goarch" \
            go build -ldflags="-s -w" -o "$out" "$pkg" 2>&1); then
        echo "${RED}  FAIL: ${name} ${goos}/${goarch} did not build (see error above)${NC}" >&2
        FAILURES=$((FAILURES + 1))
        return 1
    fi
    BUILT_ARTIFACTS+=("$out")
    return 0
}

# assert_arch PATH EXPECTED_SUBSTRING
# Fails loudly — naming the exact mismatch — rather than assuming a build
# that exited 0 produced the right architecture. This is the check that
# catches a cross-compile flag silently producing the host arch.
assert_arch() {
    local path="$1" expected="$2"
    local actual
    actual="$(file "$path")"
    if [[ "$actual" != *"$expected"* ]]; then
        echo "${RED}  FAIL: $(basename "$path") — expected \`file\` to report '${expected}', got: ${actual}${NC}" >&2
        FAILURES=$((FAILURES + 1))
        return 1
    fi
    echo "${GREEN}  OK: $(basename "$path") — ${actual#*: }${NC}"
    return 0
}

echo "${YELLOW}--- platform matrix build: ${#PLATFORMS[@]} platforms x ${#BINARIES[@]} binaries ---${NC}"
for row in "${PLATFORMS[@]}"; do
    IFS=':' read -r goos goarch expected_file <<<"$row"
    for bin_row in "${BINARIES[@]}"; do
        IFS=':' read -r name pkg <<<"$bin_row"
        if build_one "$goos" "$goarch" "$name" "$pkg"; then
            assert_arch "$DIST_DIR/${name}-${goos}-${goarch}" "$expected_file"
        fi
    done
done
echo

# The binaries build for darwin, but nothing compiled the rest of the module
# for it, so a linux-only file (syscall.SYS_FADVISE64) went unnoticed — BUG-0536.
# Vet compiles every package, tests included, which the binary builds do not.
echo "${YELLOW}--- cross-platform vet: darwin/arm64 ---${NC}"
if ! vet_output="$(cd "$GO_MODULE_DIR" && CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./... 2>&1)"; then
    echo "${RED}go vet ./... does not compile for darwin/arm64${NC}" >&2
    echo "${RED}${vet_output}${NC}" >&2
    FAILURES=$((FAILURES + 1))
else
    echo "${GREEN}  OK: GOOS=darwin GOARCH=arm64 go vet ./...${NC}"
fi
echo

# FreeBSD is a source platform, not a released one (docs/internals/freebsd-port-note.md), so it has no
# PLATFORMS row. Its types differ where darwin's do not: Statfs_t.Bavail and Rlimit.Cur are signed and
# syscall.O_DSYNC does not exist, so the same vet is the gate that keeps it building.
echo "${YELLOW}--- cross-platform vet: freebsd/amd64 ---${NC}"
if ! vet_output="$(cd "$GO_MODULE_DIR" && CGO_ENABLED=0 GOOS=freebsd GOARCH=amd64 go vet ./... 2>&1)"; then
    echo "${RED}go vet ./... does not compile for freebsd/amd64${NC}" >&2
    echo "${RED}${vet_output}${NC}" >&2
    FAILURES=$((FAILURES + 1))
else
    echo "${GREEN}  OK: GOOS=freebsd GOARCH=amd64 go vet ./...${NC}"
fi
echo

if [ "${#BUILT_ARTIFACTS[@]}" -gt 0 ]; then
    echo "${YELLOW}--- checksums (seam for REL-5's release publication) ---${NC}"
    (
        cd "$DIST_DIR" && "${SHA256_CMD[@]}" $(printf '%s\n' "${BUILT_ARTIFACTS[@]}" | xargs -n1 basename) \
            | tee "$DIST_DIR/checksums.txt"
    )
    echo "checksums written to ${DIST_DIR#$REPO_ROOT/}/checksums.txt (${#BUILT_ARTIFACTS[@]} of $(( ${#PLATFORMS[@]} * ${#BINARIES[@]} )) artifacts — see failures above if fewer)"
    echo
fi

if [ "$FAILURES" -gt 0 ]; then
    echo "${RED}build-matrix.sh: ${FAILURES} failure(s) across the platform matrix — see above${NC}" >&2
    exit 1
fi

echo "${GREEN}build-matrix.sh: all $(( ${#PLATFORMS[@]} * ${#BINARIES[@]} )) artifacts built and verified${NC}"
