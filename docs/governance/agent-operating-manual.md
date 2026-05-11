# Agent Operating Manual

This document holds detailed repository instructions that are too large for
`AGENTS.md`. Open it when a task touches code style, module boundaries,
testing, release flow, changelogs, persistence safety, or repo navigation.

## Project Map

- `crates/icydb`: public meta-crate re-exporting the workspace API.
- `crates/icydb-core`: runtime, storage, executors, and core types.
- `crates/icydb-schema-derive`: derive and codegen macros.
- `crates/icydb-schema`: schema AST/builders and validation.
- `crates/icydb-build`: build/codegen helpers and canister glue.
- `canisters/audit/*`: SQL audit harnesses for wasm baselines.
- `canisters/demo/rpg`: character-only RPG demo/perf canister harness.
- `canisters/test/sql_parity`: broad SQL parity/explain/perf harness.
- `canisters/test/sql`: lightweight SQL smoke-test harness.
- `schema/demo/rpg`: demo schema fixtures and seed data.
- `schema/audit/*`: audit schema fixtures.
- `schema/test/*`: shared macro/e2e and SQL fixtures.
- `testing/macro-tests`: macro and schema contract tests.
- `testing/pocket-ic`: Pocket-IC integration tests.
- `assets/`: images and docs assets.
- `scripts/`: release/version helpers.
- `Makefile`: common tasks.
- `Cargo.toml`: workspace manifest; edition 2024, rust-version 1.95.0.

## Workflow

- Pre-commit gate: `make fmt-check && make clippy && make check && make test`.
- Fast CI gate: `make check && make clippy`.
- Release: `make security-check && make release`.
- Hooks path: `.githooks`; common Make targets auto-configure `core.hooksPath`.
- Hook tools: `make install-dev` installs `cargo-sort` and `cargo-sort-derives`.
- CI uses Rust `1.95.0`, `rustfmt`, `clippy -D warnings`, `cargo test`, and release builds.

## Concurrent Editing

- User edits during agent runs are expected.
- Treat mid-run file changes as collaboration, not a stop condition.
- Re-read affected files and continue unless there is a real conflict on the same logic block.
- Ignore unrelated dirty files.

## Import And Module Boundaries

- Imports are part of a module's public shape and architectural contract.
- Required top-of-file order: `mod ...;`, blank line, `use ...;`, blank line, `pub use ...;`.
- All non-test modules declare imports at the top.
- Prefer one top-level grouped `use crate::{ ... };`.
- For prelude imports, use `prelude::*` or no prelude import.
- Group symbols under their owning subtree instead of repeating sibling paths.
- Prefer imported symbols over inline fully-qualified `crate::...` paths in code bodies.
- Avoid `use super::...`, `use self::...`, scattered imports, inline imports, and relative imports outside allowed exceptions.
- Tests may use `use super::*;`.
- Macro-generated or narrowly scoped helper modules may use `super::` only when it materially improves readability and a brief comment explains why `crate::{...}` is not appropriate.
- Module roots own their child export boundary: child modules stay private unless the root re-exports an intentional API.
- External callers import from module roots, not deep implementation paths.
- Level 1 is namespace, level 2 is subsystem boundary, level 3+ is internal unless re-exported.
- When splitting a Rust module, always use a directory module with `mod.rs`; never use `#[path]`.

## Coding Style

- Rustfmt: 4-space indent, edition 2024.
- Naming: `snake_case` modules/functions/files, `CamelCase` types/traits, `SCREAMING_SNAKE_CASE` constants.
- Prefer `?` over `unwrap()` and handle errors explicitly.
- Keep functions under 100 lines and 7 arguments when feasible.
- If a function legitimately exceeds those limits, use `#[expect(clippy::too_many_lines)]` or `#[expect(clippy::too_many_arguments)]`.
- Backwards compatibility is not a goal before `1.0.0`; prefer breaking changes when they simplify the model.
- Avoid unnecessary clones; prefer borrowing and iterator adapters.
- Use saturating arithmetic for counters/totals.
- Optimize proven hot paths only.

## Comments And Documentation

- Code must be readable top-down without reverse-engineering intent.
- Public API items need doc comments.
- Candid wire surfaces should use plain `//` comments when doc strings would bloat wasm.
- Every `struct`, `enum`, and `trait`, public or private, needs the standard doc block:

```rust
///
/// TypeName
///
/// Natural-language description of ownership, purpose, and use.
///

struct TypeName;
```

- The `TypeName` line must exactly match the declared type name.
- Descriptive lines should explain what the type owns, why it exists, and how the module uses it.
- Put comments/doc comments before lint/control attributes.
- Put inherent `impl TypeName` blocks immediately after the type when feasible.
- Non-trivial private functions and types need explanatory comments.
- Functions with multiple phases need inline phase comments.
- Non-trivial functions over roughly 30 lines need phase-level comments.
- In non-trivial functions, separate the final return expression from prior logic with a blank line.
- Use three-line section banners only in large files with distinct responsibilities.
- Inside dense attributed structs/enums, leave a blank line between documented/attributed fields or variants.

## Error Handling

- Prefer typed errors and `thiserror`.
- Avoid panics in library code.
- Do not match error strings in code or tests; assert on variants or kinds.
- Error classes:
  - `Unsupported`: user input rejected before persistence.
  - `Corruption`: malformed or hostile persisted bytes.
  - `Internal`: logic bugs or invariant violations.
- Helpers that construct an error type must be associated functions on that error type.
- Error constructors should populate fields or perform minimal normalization only.
- Keep defensive/internal and user-facing error construction on their owning error types.

## Persistence Safety

- Persisted bytes must never panic the system.
- Persisted decoding must be locally bounded and fallible.
- No domain type may decode directly from stable memory.
- Safety must not rely on undocumented third-party behavior.
- Thin wrappers are acceptable until a helper becomes a trust boundary; then enforce invariants at that boundary.

## Tests

- Use Rust test harness.
- Unit tests live near code when they stay within the owning module boundary.
- Cross-module behavior suites belong under the owning subsystem `tests/` directory.
- Macro-driven entity/index tests belong in `testing/macro-tests`; do not add ad-hoc `DummyEntity` types in `icydb-core` tests.
- Every inline unit test module must be preceded by:

```rust
///
/// TESTS
///
```

- Leave one blank line before and after that test banner.
- Run focused tests for changed code; use `make test` for full validation when appropriate.
- If `make test` fails during a Codex run, do not rerun it in that same run unless asked.
- PocketIC-backed tests and perf probes must run outside the sandbox by default.
- If PocketIC appears stuck before the test body or fixture loading, treat it as an environment execution problem first.
- If tests fail due to environment-specific build/linker issues, report and stop retrying.

## Changelog And Release

- Follow `docs/governance/changelog.md`.
- Update `CHANGELOG.md` for user-visible changes.
- Governance-only edits do not need release notes unless explicitly requested.
- In `docs/changelog/0.*.md`, separate every `## 0.x.y` entry with `---`.
- Root changelog summaries should be plain-language, user-impact first, and concise.
- Root minor-line summaries use exactly one bullet per patch version.
- Put implementation detail in `docs/changelog/0.*.md`.
- Releases use `make patch|minor|major`, then `make release-stage`, `make release-commit`, and `make release-push`; never hand-edit tags.
- Before `make patch|minor|major`, do not pre-bump package versions, `Cargo.lock`, or changelog patch entries.
- Never modify pushed release tags.

## Design Docs

- Do not assume patch numbers for design or status docs.
- Use explicit patch numbers only when the user provides them in the current conversation.
- Otherwise use neutral labels like `next patch` or `subsequent patch`.
- Do not renumber planned slices based on assumptions about future releases.

## Wasm Measurement

- Raw non-gzipped `.wasm` bytes are the primary metric.
- Primary decision metrics: built `.wasm`, shrunk `.wasm`.
- Deterministic `.wasm.gz` artifacts are secondary transport metrics.
- Mention gzip deltas only as support or when unexpectedly large.

## Historical Cleanup Notes

Post-0.34 cleanup candidates from the DB narrative pass:

- `db/index/store.rs`: split persistence concerns from raw-range scan/resolve logic.
- `db/index/range.rs` and `db/index/envelope.rs`: consolidate continuation-envelope helpers under one owner.
- `db/index/plan/load.rs`: consider collapsing into `db/index/plan/mod.rs`.
- `db/predicate/fingerprint.rs`: keep under predicate only if predicate remains hash authority; otherwise move to query planning/fingerprint.

## Review Checklist

- Imports declared once at top using grouped `crate::{...}`.
- No unjustified `super::` outside tests.
- No large unexplained logic blocks.
- Non-trivial long functions have phase comments.
- Public APIs document invariants and intent.
- Error construction belongs to owning error types.
- Persistence decode paths are bounded and fallible.
