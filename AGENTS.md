# Agent and contributor guidance

Read [CONTRIBUTING.md](CONTRIBUTING.md) for the CLA, style, commit and review
requirements. Keep changes focused and independently reviewable; discuss public
API or behavior changes with maintainers before making them.

## Code map and invariants

- The root `sqlair` package owns the public API, DB/TX operations and prepared
  statement cache (`cache.go`). Keep database interaction out of
  `internal/expr`.
- Read [internal/expr/doc.go](internal/expr/doc.go) before changing expressions:
  parsing builds an AST, type binding uses types rather than runtime values,
  and input binding/query building uses values without analyzing new types.
- [internal/typeinfo/doc.go](internal/typeinfo/doc.go) describes type
  validation, reflection and scanning; keep reflection there where practical.
- Generated output aliases (`_sqlair_N`) and input placeholders link query
  generation to result scanning and argument binding. Preserve their mapping
  and cover changes with expression and integration tests.
- The cache holds one driver statement per `Statement`/`DB` pair, reusing it
  only when generated SQL matches. Preserve locking, replacement and statement
  cleanup behavior; changes need cache and statement-leak tests.
- Preserve existing error context and wrapping behavior: preparation uses
  `cannot prepare statement:` and expression-specific prefixes; internal
  failures use `internal error:`. Some paths use `%w`, others format with
  `%s`; do not change error identity or text incidentally.

## Validation and tests

Use Go 1.24 or newer and enable CGO with a C compiler for the SQLite tests
(`github.com/mattn/go-sqlite3`). From the repository root run:

```sh
gofmt -l .
go vet ./...
go test ./...
govulncheck ./...
```

`gofmt -l .` should print nothing. Install `govulncheck` separately if needed:
`go install golang.org/x/vuln/cmd/govulncheck@latest`.
CI tests Go 1.24.x and the latest stable Go; it also runs govulncheck.

- Tests use `gopkg.in/check.v1` suites via Go test entry points; examples also
  run under `go test`. Restrict `-check.f` to packages that register go-check,
  for example `go test ./internal/expr -check.f TestParseErrors`; use `-run`
  for Go test entry points and examples.
- Extend the table-driven cases in `internal/expr/expr_test.go` for generated
  SQL, arguments, scanning or errors. `package_test.go` exercises SQLite
  integration; follow its deferred `dropTables` cleanup for created tables.
- For cache/lifetime changes, use the prepared-statement leak instrumentation in
  `driver_test.go` and the existing cache tests.

## Documentation and changes

- `docs/` uses the Canonical Sphinx starter pack and Diátaxis: put tutorials,
  how-to guides, reference and explanation in their respective directories.
  Link new pages into the documentation; Sphinx builds with warnings as errors.
- The spelling checker uses `en_GB` and `docs/.custom_wordlist.txt`; use its
  configured spelling rather than assuming US English. From `docs/`, relevant
  checks include `make spelling`, `make woke` and `make linkcheck`.
  Distinguish pre-existing CI failures from regressions; don't add words merely
  to hide errors.
- Follow CONTRIBUTING's conventional commit format: lowercase `type`, optional
  scope, concise description, and a body explaining what and why, including
  behavior before/after when relevant. PR descriptions may become squash commit
  messages. Include breaking changes and issue references in footers as needed.
- Follow existing Go file header conventions, but do not silently reconcile the
  inconsistent Apache 2.0 and AGPLv3 headers; raise licensing questions
  separately.
