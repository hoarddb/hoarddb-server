.PHONY: all build-cli build-server build clean run-cli run-server lint golangci fmt vet test docs-links e2e-preflight e2e-stop encryption-acceptance ring-encryption-acceptance ci build-matrix

# One Makefile, two trees. In the private repository this file sits in hoardDB/
# and the scripts are one level up (../scripts). In the published tree
# (scripts/publish-tree.sh, REL-13) the module is the tree root and the scripts
# are in ./scripts. SCRIPTS resolves to whichever exists; override it with
# `make SCRIPTS=...` if you keep them elsewhere. The private-only targets
# (the gate list, the doc index and audience checks, the selftests) are defined
# only when the scripts they call are present, so they simply do not exist in
# the published tree.
SCRIPTS ?= $(if $(wildcard scripts/lint-tool.sh),scripts,../scripts)
PRIVATE_TREE := $(wildcard $(SCRIPTS)/ci.sh)

BINARY_CLI   := hoarddb
BINARY_SRVR  := hoarddb-server

# Version ldflags inject the build metadata into the version package, so -v,
# the startup log and the audit header all report the same tag. A plain
# `go build` (or `go run`) leaves them at their defaults and reports devel.
VERSION_LDFLAGS := -X github.com/hoarddb/hoarddb-server/version.Version=$(shell git describe --tags --always --dirty 2>/dev/null || echo devel) \
	-X github.com/hoarddb/hoarddb-server/version.Commit=$(shell git rev-parse --short HEAD 2>/dev/null) \
	-X github.com/hoarddb/hoarddb-server/version.BuildDate=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)

# scripts/lint-tool.sh bootstraps this pinned v2 binary into .ci-tools/.
# .golangci.yml is a v2-schema config, so a v1 binary a developer may already
# have on PATH fails outright ("configuration file for golangci-lint v2 with
# golangci-lint v1") rather than reporting a lint finding.
# The lint pin lives in scripts/lint-tool.sh (one source of truth, REL-1) and is resolved
# through it rather than restated here. `--lint-bin` bootstraps the pinned binary
# on first use and prints its path; the recipe deliberately does NOT fall back to
# an unpinned `golangci-lint` on PATH, because that fallback is the v1-vs-v2 schema
# mismatch the pin exists to prevent — it fails with a version error that reads
# like a lint failure. Recursive `=` on purpose: a simple `:=` would run the
# bootstrap when make parses this file, for every target including `make build`.
GOLANGCI_LINT = $(shell bash $(SCRIPTS)/lint-tool.sh --lint-bin 2>/dev/null)

all: build-cli build-server

build-cli:
	go build -ldflags="$(VERSION_LDFLAGS)" -o $(BINARY_CLI) ./cmd/cli

build-server:
	go build -ldflags="$(VERSION_LDFLAGS)" -o $(BINARY_SRVR) ./cmd/server

build: all

clean:
	rm -f $(BINARY_CLI) $(BINARY_SRVR)

run-cli: build-cli
	./$(BINARY_CLI)

run-server: build-server
	./$(BINARY_SRVR)

# --- Quality gates (run before every push) ---

fmt:
	go fmt ./...

vet:
	go vet ./...

test:
	go test -timeout 30m ./...

lint: fmt vet test golangci

# The pinned linter alone, without lint's fmt/vet/test chain: agents on the
# shared host may not run the full suite locally (scripts/ci.sh is that run).
golangci:
	@bin="$(GOLANGCI_LINT)"; \
	if [ -z "$$bin" ] || [ ! -x "$$bin" ]; then \
		echo "make golangci: could not resolve the pinned golangci-lint (see scripts/lint-tool.sh)." >&2; \
		echo "  Bootstrap it once with network access: bash $(SCRIPTS)/lint-tool.sh --bootstrap" >&2; \
		exit 1; \
	fi; \
	"$$bin" run

# The platform matrix alone (GAP-25, gate 11 of scripts/ci.sh): cross-builds
# hoarddb-server and hoarddb for linux/amd64, linux/arm64, darwin/arm64
# and darwin/amd64, asserts each artifact's architecture with `file`, and
# writes dist/checksums.txt. Useful on its own without running the full
# (slow) gate set: `make build-matrix`.
build-matrix:
	@bash $(SCRIPTS)/build-matrix.sh

# Every relative link in docs/ must resolve (30 files moved at once, once).
docs-links:
	@bash $(SCRIPTS)/docs-link-check.sh

ifneq ($(PRIVATE_TREE),)
.PHONY: perf-gate docs-fast docs-check docs-sync docs-selftest docs-audience docs-defaults docs-env docs-names docs-store-layout binary-names docs-all hygiene docker-smoke-selftest publish-tree-check export-tool-selftest encryption-acceptance-selftest ring-encryption-acceptance-selftest durability-drill durability-drill-selftest test-shards test-shards-selftest check check-selftest vectors vectors-check bson-vectors bson-vectors-check trust-vectors trust-vectors-check

# The driver wire vectors (clients/handbook/testing.md section 1): regenerate
# clients/testdata/driver-vectors/v1.json by driving a server in this process
# and a three-node cluster of processes built from this tree (about a minute).
# Review the diff before committing it: a payload change is a change to every
# driver. vectors-check compares without writing; go test ./cmd/gen-testvectors
# (part of `make test`) runs the same comparison. Private tree only: clients/
# is not part of the published server tree.
vectors:
	go run ./cmd/gen-testvectors -out ../clients/testdata/driver-vectors

vectors-check:
	go run ./cmd/gen-testvectors -check ../clients/testdata/driver-vectors/v1.json

# The BSON codec corpus (clients/handbook/testing.md section 1.3): bytes the
# server's own encoder writes, which the in-tree codecs of the Python and C++
# drivers are held to byte for byte. Needs no server, so it takes a second.
# go test ./cmd/gen-testvectors (part of `make test`, so ci.sh gate 3) runs the
# same comparison as bson-vectors-check.
bson-vectors:
	go run ./cmd/gen-testvectors -bson-out ../clients/testdata/bson-vectors

bson-vectors-check:
	go run ./cmd/gen-testvectors -bson-check ../clients/testdata/bson-vectors/v1.json

# The known_servers trust corpus (clients/handbook/testing.md section 1.4): the
# rules of clients/handbook/trust.md section 4 as data, every expected value
# produced by hoardDB/transport's own known_servers code. Needs no server.
# go test ./cmd/gen-testvectors runs the same comparison as trust-vectors-check.
trust-vectors:
	go run ./cmd/gen-testvectors -trust-out ../clients/testdata/trust-vectors

trust-vectors-check:
	go run ./cmd/gen-testvectors -trust-check ../clients/testdata/trust-vectors/v1.json

# --- Private tree only: the gate list and the internal doc gates ---

# The full gate set (REL-1). scripts/ci.sh is the normative definition -
# this target, and .github/workflows/ci.yml, both just call it. Runnable
# with no GitHub account and no push: `cd hoardDB && make ci`.
ci:
	@bash $(SCRIPTS)/ci.sh

# The whole suite with the server package split into concurrent shards
# (scripts/test-shards.sh; a sixth of the serial wall time). Extra flags:
#   make test-shards ARGS="--race -n 4"
#   make test-shards ARGS="--record"      refresh scripts/test-timings/server.tsv
test-shards:
	@bash $(SCRIPTS)/test-shards.sh $(ARGS)

# Proves the shard runner cannot lose a test or swallow a failure (~5 s).
test-shards-selftest:
	@bash $(SCRIPTS)/test-shards-selftest.sh

# The fast check (docs/internals/process/fast-check-tier.md): what the change since
# the base can have broken, in a minute or two. NOT a push gate; scripts/ci.sh is.
#   make check                     base = merge-base with upstream / the work branch
#   make check ARGS="--base REF"   or ARGS="--list" to print the plan only
# make reports any failure as exit 2; run scripts/check.sh directly for the exact code.
check:
	@bash $(SCRIPTS)/check.sh $(ARGS)

check-selftest:
	@bash $(SCRIPTS)/check-selftest.sh

# The org-repo export tool (scripts/export-repo.sh) on a throwaway fixture repository:
# local only, no network, a fake noreply identity. See docs/internals/process/export-tool.md.
export-tool-selftest:
	@bash $(SCRIPTS)/export-repo-selftest.sh

# The doc index is generated from each document's **Status:** line, so it cannot
# drift. See docs/specs/README.md for the scheme.
docs-check:
	@bash $(SCRIPTS)/docs-status.sh --check

docs-sync:
	@bash $(SCRIPTS)/docs-status.sh --sync

# Proves the archive sweep cannot touch anything outside docs/. Mutates
# the tree, then restores it.
docs-selftest:
	@bash $(SCRIPTS)/docs-status-selftest.sh

# docs/user/ must not reference internals/, specs/ or bugs/.
docs-audience:
	@bash $(SCRIPTS)/docs-audience-check.sh

# A documented config default must match hoardDB/config/config.go.
docs-defaults:
	@bash $(SCRIPTS)/docs-defaults-check.sh

# Every metric, HOARDDB_ variable and db.<bucket>.<verb>() a user page names
# must exist in the code. Hard; about 4 s; the same check the site runs, alone.
docs-names:
	@bash ../website/scripts/claims-check.sh --stage docs

# A live document must spell an environment variable the way the code reads it
# (BUG-0265).
docs-env:
	@bash $(SCRIPTS)/docs-env-check.sh

# A user page must not describe the retired per-bucket layout (BUG-0281). A
# warning, not a gate: three words, quiet since the rewrite (docs gate
# slimming, 2026-10-29).
docs-store-layout:
	@bash $(SCRIPTS)/docs-store-layout-check.sh || echo "docs-store-layout: WARNING (not blocking): see the lines above"

# The CLI is hoarddb and the server hoarddb-server, in every build and every
# live document (BUG-0271). The selftest proves the check fails on each old name.
binary-names:
	@bash $(SCRIPTS)/binary-name-check.sh
	@bash $(SCRIPTS)/binary-name-check-selftest.sh

# read_back retries instead of reading once (BUG-0185); needs no Docker.
docker-smoke-selftest:
	@bash $(SCRIPTS)/docker-smoke-selftest.sh

# ci's gate 7 runs these two too; running them here catches a fixed address or a
# stale citation in the author's own worktree, in seconds, instead of stopping a
# full ci run. Absent from the published tree, so skipped there.
# HOARDDB_HYGIENE_DONE=1 is set by scripts/ci.sh's preflight, which runs these
# four steps (as part of gate 7's list) immediately before docs-all on the same tree.
hygiene:
ifeq ($(HOARDDB_HYGIENE_DONE),1)
	@echo "hygiene: skipped (scripts/ci.sh ran these steps on this tree just before)"
else
	@if [ -f $(SCRIPTS)/private-ref-check.sh ]; then bash $(SCRIPTS)/private-ref-check.sh; fi
	@if [ -f $(SCRIPTS)/citation-check.sh ]; then bash $(SCRIPTS)/citation-check.sh; fi
	@if [ -f $(SCRIPTS)/bench/bench-scripts-selftest.sh ]; then out=$$(bash $(SCRIPTS)/bench/bench-scripts-selftest.sh 2>&1) || { printf '%s\n' "$$out" | grep -E '^(FAIL|bench-scripts-selftest)' >&2; exit 1; }; fi
	@if [ -f $(SCRIPTS)/perf-gate-selftest.sh ]; then bash $(SCRIPTS)/perf-gate-selftest.sh >/dev/null || { bash $(SCRIPTS)/perf-gate-selftest.sh | grep -v '^PASS'; exit 1; }; fi
endif

# The performance gate (ci.sh gate 15): one loopback node on a real disk, a seed
# and a 20 s leg, several minutes. Not part of docs-all or lint.
perf-gate:
	@bash $(SCRIPTS)/perf-gate.sh

# The per-change docs gate: the four fast checks, about 22 s on this host (measured
# 2026-11-20). docs-all adds hygiene (125 s) and six more checks, 3m01s in total, and
# stays the pre-push and CI gate; docs-fast is deliberately not a prerequisite of it.
docs-fast: docs-check docs-links docs-audience docs-selftest

docs-all: docs-check docs-links docs-audience docs-defaults docs-env docs-names docs-store-layout binary-names docker-smoke-selftest docs-selftest hygiene

# Assemble the public tree and prove it builds, lints and tests on its own
# (REL-13, docs/internals/process/publish-tree.md). Publishes nothing.
publish-tree-check:
	@bash $(SCRIPTS)/publish-tree-check.sh

# Proves the script below is safe to run twice at once and reports a failed
# write as one (about 30 s; it starts real servers, so it is not in docs-all).
encryption-acceptance-selftest:
	@bash $(SCRIPTS)/encryption-acceptance-selftest.sh

# Proves the checks of the ring encryption acceptance run can fail: a marker
# planted in a ring store, a ring log or the rest of a data directory is found
# (seconds; no server is started).
ring-encryption-acceptance-selftest:
	@bash $(SCRIPTS)/ring-encryption-acceptance-selftest.sh

# The durability drill (release checklist VER-2): kill -9 a node in the middle
# of a write load, restart it on the same data directory, and count every
# acknowledged write back. It starts real servers and takes minutes, so it is
# run by hand or on the test VMs, not by ci.sh.
durability-drill:
	@bash $(SCRIPTS)/durability-drill.sh

# Proves the drill can fail: a store that lost one acknowledged document, a
# duplicate, a document never sent, and a data directory on tmpfs (seconds; no
# server is started). Gate 7 of ci.sh runs it.
durability-drill-selftest:
	@bash $(SCRIPTS)/durability-drill-selftest.sh
else
# The published tree has no gate list; "ci" is the public subset: format,
# vet, tests and the pinned linter (lint runs all four), then the doc links.
ci: lint docs-links
endif

# --- End-to-end runs ---
# The default ports are one pair per host (:7433 driver, :4434 internode), so a
# second run - or a server left behind by an earlier one - collides, and the
# symptom is a fresh binary dying on "address already in use" while the CLI
# talks to the OLD process. These refuse that instead of binding under it:
#
#   make e2e-preflight                      is the default pair free?
#   make e2e-preflight ARGS="--name alice --base 17433"    this run's own triple
#   make e2e-stop ARGS="--name alice"       stop this run's server
e2e-preflight:
	@bash $(SCRIPTS)/e2e-preflight.sh $(ARGS)

e2e-stop:
	@bash $(SCRIPTS)/e2e-preflight.sh $(ARGS) --stop

# At-rest encryption, Stage 1 acceptance (spec rev 4 section 7): a sealed and a
# plaintext node, written through the CLI and checked with the servers stopped.
# It builds its own binaries into its own scratch directory and never touches
# ./data:
#
#   make encryption-acceptance                        every Stage 1 leg
#   make encryption-acceptance ARGS="--base 27433"    a port triple of your choosing (default: free ports are picked)
encryption-acceptance:
	@bash $(SCRIPTS)/encryption-acceptance.sh $(ARGS)

# Ring encryption at rest acceptance (ring encryption spec section 9.3): a sealed
# single node with its refusals and mode switch, a plaintext control, the escrow
# rehearsal (a sealed node recovered from its exported node master), and three
# sealed nodes in a cluster, written through the CLI and grepped raw. About a
# minute; it builds its own binaries into its own scratch directory and never
# touches ./data. It replaces encryption-acceptance when the earlier engine goes.
#
#   make ring-encryption-acceptance                        every leg, free ports chosen
#   make ring-encryption-acceptance ARGS="--leg escrow"    one leg (single, escrow or cluster)
#   make ring-encryption-acceptance ARGS="--base 27400"    ports 27400 to 27449
ring-encryption-acceptance:
	@bash $(SCRIPTS)/ring-encryption-acceptance.sh $(ARGS)
