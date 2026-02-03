# Repository Guidelines

## Project Structure & Module Organization

* `crates/icydb`: Public meta-crate re-exporting the workspace API.
* `crates/icydb-core`: Runtime, storage, executors, and core types.
* `crates/icydb-schema-derive`: Derive and codegen macros.
* `crates/icydb-schema`: Schema AST/builders and validation.
* `crates/icydb-build`: Build/codegen helpers and canister glue.
* `crates/icydb-schema-tests`: Integration and design tests.
* `assets/`: Images and docs assets. `scripts/`: release/version helpers. `Makefile`: common tasks.
* Workspace manifest: `Cargo.toml` (edition 2024, rust-version 1.93.0).

---

## Build, Test, and Development Commands

* `make check`: Fast type-check for all crates.
* `make test`: Run all unit/integration tests (`cargo test --workspace`).
* `make build`: Release build for the workspace.
* `make clippy`: Lints with warnings denied.
* `make fmt` / `make fmt-check`: Format or verify formatting.
* Versioning: `make version|tags|patch|minor|major|release` (see `VERSIONING.md`).

---

## Common Workflows

* Pre-commit gate (local): `make fmt-check && make clippy && make check && make test`.
* Fast CI gate (local): `make check && make clippy`.
* Release (local): `make security-check && make release`.

---

## Git Hooks

* Hooks path: `.githooks` (auto-configured via `core.hooksPath`).
* Pre-commit runs: `cargo fmt --all -- --check`, `cargo sort --check`, `cargo sort-derives --check`.
* Auto-setup: running common Make targets (`fmt`, `fmt-check`, `clippy`, `check`, `test`, `build`, `install-dev`) ensures hooks are enabled.
* Tools: install with `make install-dev` (installs `cargo-sort` and `cargo-sort-derives`).

---

## Imports & Module Boundaries

Imports are considered part of a module’s public shape and architectural contract.

### Required

* All non-test modules MUST declare imports at the top of the file.
* Prefer a single top-level `use crate::{ ... };` block per module.
* Use nested paths to reflect hierarchy and ownership.

### Prohibited (by default)

* `use super::...`
* `use self::...`
* Scattered or inline imports
* Relative imports that obscure module boundaries

### Allowed Exceptions

* Test modules may use `use super::*;`.
* Macro-generated code or narrowly scoped helper modules may use `super::` **only** when:

  * It materially improves readability, and
  * A brief comment explains why `crate::{...}` is not appropriate.

### Rationale

`crate::{...}` imports make dependencies explicit, grep-friendly, and resilient to refactors.
Relative imports hide coupling and complicate auditing and large-scale reorganization.

---

## Coding Style & Naming Conventions

* Rustfmt: 4-space indent, edition 2024; run `cargo fmt --all` before committing.
* Naming:

  * `snake_case` for modules, functions, and files
  * `CamelCase` for types and traits
  * `SCREAMING_SNAKE_CASE` for constants
* Linting: Code must pass `make clippy`; prefer `?` over `unwrap()`, handle errors explicitly.
* Keep public APIs documented; co-locate small unit tests in the same file under `mod tests`.
* Backwards compatibility is **not** a goal; prefer breaking changes when they simplify the model.

---

## Commenting & Code Narration

Code must be readable top-down without reverse-engineering intent.

### Required

* Every public `struct`, `enum`, `trait`, and `fn` MUST have a doc comment (`///`).
* Public `struct` definitions MUST be preceded by **at least three consecutive doc comment lines**.
* After the doc comment block for a `struct`, there MUST be a blank line before the `struct` definition.
* Every non-trivial private function or struct MUST have at least a brief explanatory comment.
* Functions with multiple logical phases MUST include inline comments separating those phases.

### Inline Comment Guidance

* Large blocks of logic MUST be visually segmented.
* As a rule of thumb, no uninterrupted block of complex logic should exceed ~8–12 lines without an explanatory comment.
* Comments should explain intent, invariants, and risk — not restate syntax.
* In non-trivial functions, insert a blank line immediately before the final return expression (or last `return` at the bottom) to visually separate the result from the preceding logic.

### Section Banners

Section banners are a **heavyweight tool** and should be used sparingly.

### When to Use

* Only in large files with multiple distinct responsibilities or phases.
* Only when they materially improve scanability for reviewers.
* Do **not** use banners for small helpers or obvious groupings.

### Required Style

* Banners MUST be visually prominent and occupy **three lines**.
* Use wide dashed separators and a centered or clearly labeled title.
* Example:

```rust
// -----------------------------------------------------------------------------
// Access Path Analysis
// -----------------------------------------------------------------------------
```

### Guidance

* Prefer fewer, clearer banners over many subtle ones.
* If banners visually disappear into surrounding comments, remove them.
* Normal inline comments are preferred for most structure.

### Prohibited

* Single-line or low-contrast banners that blend into surrounding code.

* Overuse of banners that fragment otherwise readable code.

* “Wall-of-code” functions where intent is only inferable from control flow.

* Long helpers with no high-level summary comment.

### Definition: Non-Trivial Code

Code is considered non-trivial if it:

* Enforces invariants or safety properties
* Handles persistence, decoding, or external input
* Contains branching logic beyond simple error propagation
* Performs indexing, validation, normalization, or planning
* Would be difficult to reconstruct correctly from types alone

---

## Error Handling & Classification

* Prefer typed errors (`thiserror`); avoid panics in library code.
* Do not match error strings in code or tests; assert on variants or kinds instead.

### Error Classes

* `Unsupported`: user-supplied values rejected before persistence.
* `Corruption`: malformed or hostile persisted bytes.
* `Internal`: logic bugs or invariant violations.

---

## Persistence Safety Invariants

* Persisted bytes must never panic the system.
* Persisted decoding must be locally bounded and fallible.
* No domain type may decode directly from stable memory.
* Safety must not rely on undocumented behavior of third-party crates.
* Thin wrappers are acceptable until a helper becomes a trust boundary; enforce invariants at that boundary.

---

## Performance & Correctness

* Avoid unnecessary clones; prefer borrowing and iterator adapters.
* Use saturating arithmetic for counters and totals; avoid wrapping arithmetic.
* Only optimize proven hot paths; consider pre-allocation when it clearly pays off.

---

## Testing Guidelines

* Framework: Rust test harness.
* Unit tests live near code (`mod tests`); integration tests live in `crates/icydb-schema-tests`.
* Run all tests with `make test`.
* In `icydb-core` tests, do not create ad-hoc `DummyEntity` types; macro-driven entity and index tests belong in `crates/icydb-schema-tests`.

---

## CI Overview

* Toolchain: Rust `1.93.0` with `rustfmt` and `clippy`.
* Checks job (PRs/main): `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`.
* Release job (tags): `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`, `cargo build --release`.
* Package cache: clears `~/.cargo/.package-cache` before running cargo.
* Versioning: separate job runs `scripts/app/check-versioning.sh`.
* Canisters: release job builds `test_canister` to WASM, extracts `.did` via `candid-extractor`, and uploads artifacts.

---

## Commit & Pull Request Guidelines

* Commits: imperative mood, concise scope (e.g., "Fix index serialization").
* PRs: clear description, rationale, before/after notes; include tests and docs updates.
* Changelog: update `CHANGELOG.md` for user-visible changes (follow the rules below).
* Releases: use `make patch|minor|major`; never hand-edit tags.

---

## Changelog Rules

* Keep the existing changelog structure and header format (e.g., `## [x.y.z] - YYYY-MM-DD - Short Title`).
* Smaller changelog entries may omit the title segment; use `## [x.y.z] - YYYY-MM-DD` when no title is needed.
* Use section headers like `### <emoji> Added/Changed/Fixed/Removed/Breaking/Migration Notes/Summary` only as needed; keep the header wording consistent with the file.
* Emoji must be **random and deliberately unrelated** to the section meaning; pick a different, inappropriate emoji for each section within a release.
* When updating the changelog, use the version the user specifies or the existing latest entry; do not create a new version header if the newest entry already exists (e.g., if `0.6.5` is present while the current version is `0.6.4`, add to `0.6.5`).
* Write in plain, industry-friendly language: lead with the outcome and user impact, use technical terms only when they improve clarity.
* Bullets should be short (1–2 sentences), avoid deep implementation details, and use inline code for API/type names.

---

## Review Checklist (Non-Exhaustive)

* [ ] Imports declared once at top using `crate::{...}`
* [ ] No `super::` usage outside tests without justification
* [ ] No large unexplained blocks of logic
* [ ] Complex functions are commented in phases
* [ ] Public APIs document invariants and intent

---

## Security & Configuration

* Run `make security-check` before release.
* Never modify pushed release tags.
* Pin git dependencies by tag in downstream projects.
* Ensure local toolchain matches CI (`rustup toolchain install 1.93.0`).
