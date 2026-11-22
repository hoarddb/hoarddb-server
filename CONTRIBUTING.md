# Contributing to hoardDB

Thank you for wanting to help. This page is what to know before you open a change:
how to build and test, the checks a change must pass, how to write the commit,
and what you are agreeing to about licensing. It is self-contained: everything a
contributor needs is here or in the repository next to it.

If you found a security problem, stop here and read [SECURITY.md](SECURITY.md)
instead. Do not report it in a public issue.

## The one rule behind all the others

hoardDB wins or loses on developer experience. Before you open a change, ask:
**could a developer with no context use this at 3am without reading the
source?** If not, fix that first. In practice that means:

- an error says what to do, not only what went wrong: *Bucket "users" not found
  in database "app". Run 'hoarddb bucket list app' to see available buckets, or
  'hoarddb bucket create app users --type=hash' to create it.*;
- every feature, command and API ships with an example that runs;
- the same verb means the same thing on every store type;
- nothing breaks a user's data or program silently: deprecate loudly, warn on
  stderr with a deadline;
- zero configuration to start: `hoarddb-server` with no arguments must work.

## Set up

You need the Go version named in `go.mod` (Go 1.26.9 or newer). With an older Go
that manages toolchains, the first build downloads the right one.

```bash
make build            # hoarddb-server and hoarddb
make test             # the test suite
```

The code is laid out by job: `cmd/server` and `cmd/cli` are entry points and stay
minimal; `cli/` is the command-line logic; `config/` is configuration and TLS;
`ring/` is consistent hashing; `storage/` is persistence; `store/` holds the six
store types; `transport/` is the TCP+TLS protocol; `logging/` and `debug/` are
what they say. User documentation is in [`docs/`](docs/README.md).

## Before you push

Run these in order. A change that fails any of them will not be merged.

```bash
make lint             # gofmt, go vet, the tests, and golangci-lint (pinned)
make fmt              # go fmt ./...
go test ./...         # all tests
go vet ./...          # standard vet checks
make docs-links       # if you touched anything under docs/
```

`make lint` needs a pinned `golangci-lint` v2; the first run fetches it into
`.ci-tools/`, so it needs network access once. `make ci` is `make lint` plus the
documentation link check.

## What a good change looks like

- **A bug fix starts with a test that fails without the fix.** A bug is closed
  only when such a test exists. A fix without one is a promise.
- **Prefer an existing library** to new code, and a boring, well-known one to a
  clever one. A new dependency must have a licence from the allowed list below,
  and the reason must be written down.
- **Small interfaces; errors returned and wrapped; goroutines with a clean exit.**
  Accept interfaces, return structs. Wrap with `fmt.Errorf("opening bucket %q: %w", name, err)`
  and match with `errors.Is` / `errors.As`, never `==`. Every goroutine needs a
  wait or a cancellation. Every exported symbol has a doc comment.
- **Rejected on sight:**
  - `panic` outside `main` and tests: this is a database, and a panic in a read
    or write path is a crash the caller cannot handle;
  - a swallowed error (`_ = f()`, or a bare `if err != nil { return }`);
  - vague names (`data`, `result`, `value`, `temp`, `info`, `item`);
  - defensive code for impossible cases, and unreachable code;
  - a function with six or more parameters (take a request struct);
  - a comment that says what the code does instead of why.
- **Say, for any change that adds a bucket-level cost, what it costs.** hoardDB keeps
  data with the entity it describes, so a change states the
  per-bucket cost it adds (bytes, file descriptors, goroutines), whether it adds a
  per-bucket metric (it should not), and what it does at 10^6 buckets (an internal
  review bar, not a promise).
- **Do not describe something that does not work yet.** A page that promises a
  feature that is not built costs whoever finds out at 3am.

## Commits

- Imperative mood, a subject under 72 characters: `Refuse a blob over the cap`,
  not `Refused` or `Refusing`.
- The body explains **why**; the diff already shows what.
- Reference what the change settles: `fixes #123` for the issue number.
- One logical change per commit, and stage files by name, not with `git add -A`.

## Licensing

- hoardDB is licensed under the Business Source License 1.1 (see
  [LICENSE](LICENSE)). Contributions are accepted under that licence.
- Client libraries live in their own repositories under Apache-2.0. A client must not import a package from this module (`github.com/hoarddb/hoarddb-server/transport`,
  `github.com/hoarddb/hoarddb-server/server`, and so on); the wire format is the only thing the two share.
- Every dependency must be compatible. **Allowed:** MIT, BSD (2-clause and
  3-clause), Apache-2.0, ISC, Unlicense, CC0, MPL-2.0. **Forbidden:** GPL (any
  version), AGPL, SSPL, EUPL and any copyleft that requires derivative works to
  use the same licence. LGPL is avoided. If in doubt, ask before you add it.

## Sending a change

Pull requests are welcome. For anything larger than a fix, open an issue or a Discussion
first, so you do not build something that cannot be merged.

1. Fork, branch, and make the change with the checks above passing.
2. **Sign off every commit** with `git commit -s`. That adds a `Signed-off-by: Your Name
   <you@example.com>` line, and it certifies that you wrote the change or have the right
   to submit it under this repository's licence (the Developer Certificate of Origin 1.1,
   at developercertificate.org). There is no other agreement to sign. A pull request with
   an unsigned commit will be asked to add the sign-off before it is merged.
3. Open the pull request and say what the change does and why. A bug fix names the test
   that fails without it.
4. A maintainer reviews it and merges it. Expect questions: the rules above are applied
   as written.

## Getting help

Open an issue with the command you ran, what you expected and what you got,
together with the output of `hoarddb-server -v`. A page in the documentation whose
example does not work is a defect in the documentation, and worth reporting as one.
