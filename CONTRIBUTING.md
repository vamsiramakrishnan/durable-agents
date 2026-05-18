# Contributing to Tape

Thanks for being here. Tape is a small project with a strong central idea —
*the journal is the centre, everything else is a projection or a reactor* —
and most contributions land cleanly when they reconcile to that idea. The two
documents to read before writing code are:

- [`CLAUDE.md`](CLAUDE.md) — repo orientation, layout, conventions.
- [`design-principles/tape.md`](design-principles/tape.md) — the design spec.

## Getting set up

```bash
git clone https://github.com/vamsiramakrishnan/durable-agents
cd durable-agents
./setup.sh
make doctor        # tick/cross diagnostic
make sdk-test-all  # round-trip every SDK
```

The setup script installs `mise` and every toolchain the repo touches (Rust,
Python, Node, Go, Java, just). If you only need one language, `./setup.sh
--minimal` keeps to Rust + Python.

## The contribution loop

1. **Pick a gap or a bug.** [`SDK_PARITY.md`](SDK_PARITY.md) tracks the open
   gaps with deliverables and acceptance criteria; the GitHub issues are
   smaller scopes.
2. **Branch.** Off `main`. Name it `<your-handle>/<short-slug>`.
3. **Write the change.** One logical change per commit. If you touch the wire
   protocol, follow the order in `CLAUDE.md`: proto → server → Python SDK →
   the other three SDKs.
4. **Test.** Whatever fits:
   - One language: `make sdk-test-<language>`
   - The protocol contract across all four: `make sdk-parity`
   - The Rust server: `make test`
5. **Open a PR.** Link the issue. Describe what changed and *why* — the diff
   already shows the *what*.

## House rules

- **One contract, four SDKs.** A feature isn't done until every SDK can reach
  it. If full parity is too big for one PR, file a row in `SDK_PARITY.md` and
  link it from the PR.
- **Safety invariants live at construction time.** The
  `non_idempotent` rule (no `business_key`, no `status_check`, no
  `compensate` ⇒ refuse to build the tool) is enforced both by every SDK at
  decoration time *and* by the server at `BeginEffect` time. Don't weaken
  either side without weakening both.
- **Examples are first-class tests.** `tape/examples/treasury/` and
  `tape/examples/non_idempotent_bank/` are referenced by integration tests;
  changing an example means running `make test`.
- **Default to no comments.** Names should do the work. A comment is for the
  non-obvious *why* — a hidden invariant, a workaround for a specific bug, a
  surprise. If removing the comment wouldn't confuse a future reader, don't
  write it.
- **No backwards-compat shims pre-1.0.** Rename freely; we use semver and
  callers pin a version.

## Code style

| Language   | Formatter / Linter                                     | How                                                |
|------------|---------------------------------------------------------|----------------------------------------------------|
| Python     | `ruff format` + `ruff check`                            | `ruff format tape/sdk/python tape/cli tape/tests`  |
| Rust       | `cargo fmt` + `cargo clippy`                            | `cargo fmt --all` in `tape/server/`                |
| TypeScript | `tsc --noEmit`                                          | `npx tsc --noEmit` in `tape/sdk/typescript/`       |
| Go         | `gofmt` + `go vet`                                      | `gofmt -w . && go vet ./...` in `tape/sdk/go/`     |
| Java       | Maven default                                           | `mvn -q -DskipTests package`                       |

A pre-commit config ([`.pre-commit-config.yaml`](.pre-commit-config.yaml))
handles the trailing-whitespace / merge-conflict / large-file checks. Install
it with `pip install pre-commit && pre-commit install`.

## Documentation

The docs site is MkDocs Material; the source lives under `tape/docs/`. To
build locally:

```bash
make docs-serve   # http://127.0.0.1:8000
```

If you add a primitive, add a how-to page that shows it in context — not just
a reference entry. Tape's docs are journey-shaped, not API-shaped.

## Where to ask

- **Bugs / feature requests** — open an issue with a runnable repro.
- **Design questions** — start with the treatise + `tape.md` and reference the
  section your question is against; that gives us a shared vocabulary.
- **Security** — please follow [`SECURITY.md`](SECURITY.md). Don't open a
  public issue for vulnerabilities.

## License

By contributing you agree your work is licensed under the
[Apache 2.0](LICENSE) license that covers the rest of the project.
