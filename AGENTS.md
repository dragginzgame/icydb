# Repository Guidelines

## Project Structure & Module Organization
- `crates/icydb`: Public meta-crate re-exporting the workspace API.
- `crates/icydb-core`: Runtime, storage, executors, and core types.
- `crates/icydb-macros`: Derive and codegen macros.
- `crates/icydb-schema`: Schema AST/builders and validation.
- `crates/icydb-build`: Build/codegen helpers and canister glue.
- `crates/test` and `crates/test_design`: Integration and design tests.
- `assets/`: Images and docs assets. `scripts/`: release/version helpers. `Makefile`: common tasks.
- Workspace manifest: `Cargo.toml` (edition 2024, rust-version 1.92.0).

## Build, Test, and Development Commands
- `make check`: Fast type-check for all crates.
- `make test`: Run all unit/integration tests (`cargo test --workspace`).
- `make build`: Release build for the workspace.
- `make clippy`: Lints with warnings denied.
- `make fmt` / `make fmt-check`: Format or verify formatting.
- Versioning: `make version|tags|patch|minor|major|release` (see `VERSIONING.md`).

## Common Workflows
- Pre-commit gate (local): `make fmt-check && make clippy && make check && make test`.
- Fast CI gate (local): `make check && make clippy`.
- Release (local): `make security-check && make release`.

## Git Hooks
- Hooks path: `.githooks` (auto-configured via `core.hooksPath`).
- Pre-commit runs: `cargo fmt --all -- --check`, `cargo sort --check`, `cargo sort-derives --check`.
- Auto-setup: running common Make targets (`fmt`, `fmt-check`, `clippy`, `check`, `test`, `build`, `install-dev`) ensures hooks are enabled.
- Tools: install with `make install-dev` (installs `cargo-sort` and `cargo-sort-derives`).

## Coding Style & Naming Conventions
- Rustfmt: 4-space indent, edition 2024; run `cargo fmt --all` before committing.
- Naming: `snake_case` for modules/functions/files, `CamelCase` types/traits, `SCREAMING_SNAKE_CASE` consts.
- Linting: Code must pass `make clippy`; prefer `?` over `unwrap()`, handle errors explicitly.
- Keep public APIs documented; co-locate small unit tests in the same file under `mod tests`.
- Don't worry about backwards compatibility. Prefer breaking changes for the time being.

### Additional Style Guidance
- Docs: rustdoc triple-slash `/// ` with a space; include brief examples when practical.
- Errors: prefer typed errors (thiserror); avoid panics in library code.
- Functions: keep small and focused, except at trust boundaries where single, centralized validation is preferred.
- Borrowing: avoid unnecessary clones; prefer iterator adapters.
- Imports: group per-crate, nest items (e.g., `use crate::{a, b};`); pull common std items into scope at top.
- Imports: prefer a single `use crate::{...}` block per module for internal hierarchy; use `use super::*;` inside test modules only, and avoid `super::` elsewhere.
- Counters: use saturating arithmetic for totals; avoid wrapping arithmetic.
- Performance: only optimize on proven hot paths; consider pre-allocation when it clearly pays off.
- Codegen (icydb_build): generate minimal glue and delegate to `icydb::interface::*`.

## Persistence Safety Invariants
- Persisted bytes must never panic the system.
- Persisted decoding must be locally bounded and fallible.
- No domain type may decode directly from stable memory.
- Safety must not rely on undocumented behavior of third-party crates.
- Thin wrappers are fine until a helper becomes a trust boundary; enforce invariants locally at that boundary.

## Error Classification Guidance
- `Unsupported`: user-supplied values rejected before persistence.
- `Corruption`: malformed or hostile persisted bytes.
- `Internal`: logic bugs or invariant violations.

## CI Overview
- Toolchain: Rust `1.92.0` with `rustfmt` and `clippy`.
- Checks job (PRs/main): `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`.
- Release job (tags): `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`, `cargo build --release`.
- Package cache: clears `~/.cargo/.package-cache` before running cargo to avoid stale lock issues.
 - Versioning: separate job runs `scripts/app/check-versioning.sh` for repository/tag hygiene checks.
- Canisters: release job builds `test_canister` to WASM, extracts `.did` via `candid-extractor`, and uploads artifacts.

## Testing Guidelines
- Framework: Rust test harness. Place unit tests near code; integration tests live in `crates/test` and `crates/test_design`.
- Run all: `make test`. Single crate/test: `cargo test -p icydb <filter>` for the meta-crate; target other crates explicitly as needed.
- Aim for meaningful coverage on entity macros, query paths, and index behavior. Add fixtures where helpful.
- In `icydb-core` tests, do not create ad-hoc `DummyEntity` types; macro-driven entity/index tests belong in `crates/test_design`.

## Commit & Pull Request Guidelines
- Commits: Imperative mood, concise scope (e.g., "Fix index serialization"; "Bump version to 0.15.1").
- PRs: Clear description, linked issues, rationale, before/after notes; include tests and docs updates.
- Changelog: Update `CHANGELOG.md` under `[Unreleased]` for user-visible changes.
- Releases: Use `make patch|minor|major` and push with tags; do not hand-edit tags.

## Security & Configuration Tips
- Tag immutability: run `make security-check`; never modify pushed release tags.
- Pin git dependencies by tag in downstream projects.
- Toolchain: install Rust `1.92.0` (`rustup toolchain install 1.92.0`) and ensure CI matches.
