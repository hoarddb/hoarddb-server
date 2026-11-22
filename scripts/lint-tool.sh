#!/usr/bin/env bash
# scripts/lint-tool.sh - the one place the pinned golangci-lint version lives.
#
# Two callers, one pin (REL-1):
#   - scripts/ci.sh sources this file for its lint gate;
#   - the Makefile (`make lint`) runs it as `lint-tool.sh --lint-bin` to get the
#     pinned binary's path, in the private tree AND in the published tree
#     (scripts/publish-tree.sh copies this file in), so `make lint` does not
#     need ci.sh, hugo or python.
#
# Usage (when executed):
#   lint-tool.sh --bootstrap   install the pinned binary if absent, print nothing
#   lint-tool.sh --lint-bin    install if absent, print the binary's path (stdout)
#
# The tool cache lives in <this script's parent>/.ci-tools: the repository root
# in the private tree, the tree root in the published one. Set CI_TOOLS_DIR to
# override. Never falls back to an unpinned golangci-lint on PATH.

LINT_TOOL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CI_TOOLS_DIR="${CI_TOOLS_DIR:-$LINT_TOOL_ROOT/.ci-tools}"
RED="${RED-$'\033[0;31m'}"
NC="${NC-$'\033[0m'}"

# Pinned exactly, per the dispatch guard. v2 matches hoardDB/.golangci.yml's
# `version: "2"` — the local v1.64.8 binary a developer may already have on
# PATH fails on this config with a version mismatch, not a lint finding.
GOLANGCI_LINT_VERSION="v2.13.2"
GOLANGCI_LINT_BIN="$CI_TOOLS_DIR/golangci-lint-${GOLANGCI_LINT_VERSION}/golangci-lint"

# golangci-lint is the one tool this script is allowed to bootstrap itself
# (dispatch guard: "no new runtime dependency beyond the pinned lint tool").
# Every other tool above must already be present.
#
# ensure_golangci_lint is a function so `make lint` can call it through
# `lint-tool.sh --lint-bin` and get the pinned binary without the pin being defined
# twice: the Makefile used to fall back to whatever `golangci-lint` was on PATH,
# which is the v1-vs-v2 schema mismatch this pin exists to prevent.
ensure_golangci_lint() {
    if [ ! -x "$GOLANGCI_LINT_BIN" ]; then
        echo "lint-tool.sh: bootstrapping golangci-lint ${GOLANGCI_LINT_VERSION} (pinned) into ${CI_TOOLS_DIR}"
        mkdir -p "$(dirname "$GOLANGCI_LINT_BIN")"
        if ! GOBIN="$(dirname "$GOLANGCI_LINT_BIN")" go install \
                "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}"; then
            echo "${RED}lint-tool.sh: could not install golangci-lint ${GOLANGCI_LINT_VERSION}${NC}" >&2
            echo "${RED}  This needs network access the first time (or a warm module cache)." >&2
            echo "${RED}  Never falling back to an unpinned 'golangci-lint' on PATH — that is${NC}" >&2
            echo "${RED}  exactly the v1-vs-v2 mismatch this pin exists to prevent.${NC}" >&2
            return 1
        fi
    fi
    INSTALLED_VERSION="$("$GOLANGCI_LINT_BIN" version 2>&1 | grep -o 'version [0-9.]*' | awk '{print $2}')"
    if [ "v${INSTALLED_VERSION}" != "$GOLANGCI_LINT_VERSION" ]; then
        echo "${RED}lint-tool.sh: golangci-lint at ${GOLANGCI_LINT_BIN} reports version ${INSTALLED_VERSION}, expected ${GOLANGCI_LINT_VERSION#v}${NC}" >&2
        return 1
    fi
    export PATH="$(dirname "$GOLANGCI_LINT_BIN"):$PATH"
}


if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    case "${1:-}" in
        --bootstrap | --lint-bin)
            ensure_golangci_lint >&2 || exit 1
            if [ "$1" = "--lint-bin" ]; then
                echo "$GOLANGCI_LINT_BIN"
            fi
            ;;
        *)
            echo "usage: lint-tool.sh --bootstrap | --lint-bin" >&2
            exit 2
            ;;
    esac
fi
