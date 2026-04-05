# Repository Guidelines

## Project Structure & Module Organization

* `crates/icydb`: Public meta-crate re-exporting the workspace API.
* `crates/icydb-core`: Runtime, storage, executors, and core types.
* `crates/icydb-schema-derive`: Derive and codegen macros.
* `crates/icydb-schema`: Schema AST/builders and validation.
* `crates/icydb-build`: Build/codegen helpers and canister glue.
* `canisters/minimal`: Minimal SQL canister harness used for wasm audit baseline.
* `canisters/quickstart`: SQL quickstart canister harness used for onboarding and integration flows.
* `schema/quickstart`: Quickstart canister schema fixtures.
* `schema/minimal`: Minimal schema fixtures for lightweight wasm auditing.
* `schema/test`: Shared schema fixtures for macro/e2e test harnesses.
* `testing/macro-tests`: Macro and schema contract tests.
* `testing/pocket-ic`: Pocket-IC integration tests.
* `assets/`: Images and docs assets. `scripts/`: release/version helpers. `Makefile`: common tasks.
* Workspace manifest: `Cargo.toml` (edition 2024, rust-version 1.94.1).

---

## Build, Test, and Development Commands

* `make check`: Fast type-check for all crates.
* `make test`: Run all unit/integration tests (`cargo test --workspace`).
* `make build`: Release build for the workspace.
* `make clippy`: Lints with warnings denied.
* `make fmt` / `make fmt-check`: Format or verify formatting.
* Versioning: `make version|tags|patch|minor|major|release`.

---

## Common Workflows

* Pre-commit gate (local): `make fmt-check && make clippy && make check && make test`.
* Fast CI gate (local): `make check && make clippy`.
* Release (local): `make security-check && make release`.

---

## Language Policy

* Do not add Python code to this repository.
* New tooling, scripts, test helpers, and build helpers must use the repo’s existing Rust/shell patterns instead of introducing Python.

---

## Wasm Measurement Priority

* For wasm optimization decisions, treat non-gzipped wasm bytes as the primary metric.
* Primary decision metrics are:
  * built `.wasm`
  * shrunk `.wasm`
* Deterministic gzip artifacts (`.wasm.gz`) are secondary transport metrics only.
* Do not reject or accept a wasm change primarily because of gzip movement when raw non-gzipped wasm improves.
* Mention gzip deltas only as supporting context or when they are unexpectedly large enough to warrant a note.

---

## Concurrent Editing During Agent Runs

* User edits made while the coding agent is running are expected in this repository.
* Mid-run file changes should be treated as normal collaboration, not an automatic stop condition.
* When concurrent edits are detected, the agent should re-read the affected files and continue unless there is a real conflict on the same logic block.
* Do not treat unrelated concurrent file changes as an error state.

---

## Agent File Links

* When referencing files in agent responses, always use absolute filesystem paths so links are clickable in VS Code.
* Relative paths are allowed in code and commands, but response links must be absolute.
* Example: `/home/adam/projects/icydb/crates/icydb-core/src/db/session.rs:53`

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

* All module import declarations MUST appear at the top of the file, immediately after module-level doc comments (if any).
* Required top-of-file order is strict: first `mod ...;` declarations, then one blank line, then `use ...;` imports, then one blank line, then `pub use ...;` re-exports.
* This strict import/declaration order applies to all files, including test files.
* All non-test modules MUST declare imports at the top of the file.
* Prefer a single top-level `use crate::{ ... };` block per module.
* Prefer grouping related module imports into that single block (instead of multiple top-level `use` lines) when possible, e.g.:

```rust
use crate::{
    db::query::{
        plan::{OrderSpec, validate::PlanError},
        predicate::SchemaInfo,
    },
    model::entity::EntityModel,
};
```
* When importing multiple symbols from the same subtree, group them under that subtree instead of repeating sibling paths. For example, prefer `executor::aggregate::{ field::{...}, projection::{...}, ... }` over repeated `executor::aggregate::field::...`, `executor::aggregate::projection::...`, etc.
* Use nested paths to reflect hierarchy and ownership.
* Prefer imported symbols over inline fully-qualified `crate::...` paths in code bodies (including tests); bring dependencies into top-level `use` blocks instead.

### Prohibited (by default)

* `use super::...`
* `use self::...`
* Scattered or inline imports
* Relative imports that obscure module boundaries
* `#[path = \"...\"]` module wiring attributes

### Allowed Exceptions

* Test modules may use `use super::*;`.
* Macro-generated code or narrowly scoped helper modules may use `super::` **only** when:

  * It materially improves readability, and
  * A brief comment explains why `crate::{...}` is not appropriate.

### Rationale

`crate::{...}` imports make dependencies explicit, grep-friendly, and resilient to refactors.
Relative imports hide coupling and complicate auditing and large-scale reorganization.

### Module Export Boundary Rule

1. Every module defines its own boundary.

If a module has submodules, then:

mod.rs (or the module root file) is the only place that may export items from those children.

External callers must import from the module root.

Deep submodules are implementation detail by default.

2. Export Rule

Inside a module:

```rust
mod child_a;
mod child_b;

pub use child_a::{TypeA, TypeB};
```

child_a and child_b remain private (or pub(crate) if needed).

Only explicitly re-exported items form the module's public surface.

3. Caller Rule

Outside the module subtree:

Import from the module root only.

Do not import from deep paths.

Correct:

```rust
use crate::db::query::Predicate;
```

Incorrect:

```rust
use crate::db::query::predicate::internal::NormalizePass;
```

4. Nested Modules

If db::query::predicate has its own submodules:

predicate/mod.rs defines its own export surface.

External callers use:

```rust
crate::db::query::predicate::{...}
```

Not deeper.

5. Deep Imports Allowed Only Internally

Inside the module subtree itself, deep imports are allowed.

For example, inside db::query:

```rust
use super::predicate::normalize::NormalizePass;
```

This is acceptable because it remains inside the boundary.

6. Visibility Tiering

Level 1 (crate root): namespace only.

Level 2 (subsystem root): public boundary.

Level 3+: internal unless explicitly re-exported.

Why This Is Correct

This rule:

Prevents deep coupling.

Prevents namespace leakage.

Allows internal refactors.

Preserves your two-tier public surface model.

Avoids accidental third-level APIs.

Important Clarification

This rule does not mean:

Flatten everything to second level.

It means:

Each module is responsible for its own boundary.

If something is nested three levels deep and is part of the API, that module root must re-export it intentionally.

### Module Split Rule

When splitting a Rust module into multiple files:

* Always convert it to a directory module with `mod.rs` as the root (for example `foo/mod.rs` + `foo/bar.rs`).
* Keep module wiring in `mod.rs` via `mod child;` and explicit re-exports where needed.
* Never use `#[path]` to wire modules. No exceptions.

### 0.34 Follow-Up Consolidation Notes

These are intentional post-0.34 cleanup candidates identified during the DB narrative pass:

* `db/index/store.rs`: split persistence concerns from raw-range scan/resolve logic. Keep storage primitives in `store.rs`; move scan/continuation traversal to a dedicated module.
* `db/index/range.rs` and `db/index/envelope.rs`: consolidate continuation-envelope helpers (`anchor_within_envelope`, `continuation_advanced`, `resume_bounds_from_refs`) under one clear ownership boundary.
* `db/index/plan/load.rs`: consider collapsing into `db/index/plan/mod.rs` unless maintaining file-per-phase separation is preferred.
* `db/predicate/fingerprint.rs`: keep under predicate only if predicate domain remains the hash authority; otherwise migrate to query planning/fingerprint boundary in a dedicated follow-up.

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
Commenting quality is a merge gate: code that is correct but under-documented is incomplete.

### Priority & Default

* Favor more explanation over less when intent is not obvious from types and signatures alone.
* For non-trivial logic, assume reviewers should understand the flow from comments without tracing every branch first.
* If uncertain whether a block needs a comment, add one.

### Required

* Public API items (`struct`, `enum`, `trait`, `fn`, type aliases, and public re-exports) MUST have doc comments (`///`).
* Exception: types that derive `CandidType`, plus their public fields and enum variants, MUST use plain `//` comments instead of `///` doc comments so those strings are not retained in normal canister wasm builds.
* `mod` declarations are wiring and SHOULD NOT be individually documented; document the module in its root file instead when needed.
* Every public `struct`, `enum`, `trait`, and `fn` MUST have a doc comment (`///`).
* The normal public-doc-comment requirement does not apply to `CandidType` wire surfaces; keep those comments as plain `//` narration unless the docs are explicitly needed for rustdoc-only output.
* Public `struct` and `trait` definitions MUST be preceded by **at least three consecutive doc comment lines**.
* Every non-trivial `struct` or `trait` (public or private) MUST be preceded by **at least three consecutive doc comment lines**.
* For every non-trivial `struct` or `trait`, use this exact doc-block shape:
  * Line 1: `///`
  * Line 2: `/// <TypeName>`
  * Line 3: `///`
  * Line 4+: one or more descriptive `///` lines
  * Final line: `///`
  * Then one blank source line before `struct <TypeName> { ... }` or `trait <TypeName> { ... }`
* For every non-trivial `struct` or `trait`, the `<TypeName>` line MUST exactly repeat the declared type name.
* After the doc comment block for a `struct` or `trait`, there MUST be a blank line before the type definition.
* The multi-line doc-block shape above does not apply to `CandidType` wire surfaces; use concise `//` comments there instead.
* Every non-trivial private function or type MUST have at least a brief explanatory comment.
* For any function/struct/enum/trait/type with lint/control attributes (`#[expect]`, `#[allow]`, `#[cfg_attr]`, etc.), comments/doc comments for that item MUST come first, then attributes, then the item.
* Inherent `impl <TypeName>` blocks SHOULD appear immediately after the type definition (after derives/attrs and doc block) and before unrelated items whenever feasible.
* Functions with multiple logical phases MUST include inline comments separating those phases.
* Non-trivial functions longer than ~30 lines MUST include phase-level header comments that label each major step.

### Inline Comment Guidance

* Large blocks of logic MUST be visually segmented.
* As a rule of thumb, no uninterrupted block of complex logic should exceed ~8–12 lines without an explanatory comment.
* In larger functions, add a little more phase-level commentary around major logic blocks (light-touch, not excessive) to improve scanability.
* Comments should explain intent, invariants, and risk — not restate syntax.
* Explain why branches exist when behavior is defensive, security-sensitive, or invariant-preserving.
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

## Error Construction Discipline

* Any helper that constructs an error type MUST be implemented as an associated function on the owning error type.
* Free-floating functions that return an error type (for example `fn some_error(...) -> MyError`) are prohibited.
* Error construction logic is domain-owned. Constructors belong to the error type that defines the taxonomy.
* Constructors must only populate fields or perform minimal normalization. Business logic does not belong in constructors.
* Error taxonomy boundaries must remain explicit. Constructors must not collapse, blur, or hide domain separation.
* Defensive and internal error construction must remain on internal error types.
* User-facing error construction must remain on user-facing error types.

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
* Unit tests live near code (`mod tests`); macro/schema integration tests live in `testing/macro-tests`.
* Every inline unit test module (`mod tests`) MUST be preceded by the exact doc banner:

```rust
///
/// TESTS
///
```

* Leave exactly one blank line before and one blank line after that banner block.
* Run all tests with `make test`.
* If `make test` fails during a Codex run, do not run `make test` a second time in that same run unless the user explicitly asks; treat the failure as likely blocked by a build lock or environment contention and report it.
* In `icydb-core` tests, do not create ad-hoc `DummyEntity` types; macro-driven entity and index tests belong in `testing/macro-tests`.
* If test execution fails due to environment-specific build or linker issues, notify the user and stop retrying; those tests must be run manually by the user in a working environment.

---

## CI Overview

* Toolchain: Rust `1.94.1` with `rustfmt` and `clippy`.
* Checks job (PRs/main): `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`.
* Release job (tags): `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`, `cargo build --release`.
* Package cache: clears `~/.cargo/.package-cache` before running cargo.
* Versioning: separate job runs `scripts/app/check-versioning.sh`.
* Canisters: release job builds `canister_quickstart` to WASM, extracts `.did` via `candid-extractor`, and uploads artifacts.

---

## Commit & Pull Request Guidelines

* Codex must never run `git commit` or `git push`; prepare/stage changes and hand off commit/push to the user.
* Commits: imperative mood, concise scope (e.g., "Fix index serialization").
* PRs: clear description, rationale, before/after notes; include tests and docs updates.
* Routine feature PRs should satisfy the slice-shape and domain-span rules in `docs/governance/velocity-preservation.md`.
* If a PR exceeds the enforced slice-shape limits, include `Slice-Override: yes` and `Slice-Justification: ...` in the PR body.
* Changelog: update `CHANGELOG.md` for user-visible changes (follow `docs/governance/changelog.md`).
* In `docs/changelog/0.*.md`, every `## 0.x.y` entry MUST be separated from the next entry by a standalone `---` divider.
* Releases: use `make patch|minor|major`; never hand-edit tags.
* Before running `make patch|minor|major`, do not manually bump workspace/package version numbers, `Cargo.lock`, or changelog patch entries; those release targets own the version advance and pre-bumping them creates conflicts.

---

## Design Doc Versioning Rules

* Do not assume or infer patch version numbers for design docs or status docs.
* Use explicit patch numbers only when the user provides them in the current conversation.
* If patch numbering is not explicitly provided, use neutral labels such as `next patch` / `subsequent patch` instead of `0.x.y`.
* Do not renumber planned slices in design docs based on assumptions about upcoming releases.

---

## Changelog Readability Rules

* Audience split is mandatory:
  * Root `CHANGELOG.md` is written for junior engineers and new contributors.
  * `docs/changelog/0.*.md` is written for senior engineers and domain experts who need deeper technical context.
* In root `CHANGELOG.md`, prefer plain language and user impact first; avoid dense internal wording when a simpler phrase is accurate.
* In root `CHANGELOG.md`, every bullet MUST be understandable without prior subsystem context.
* In root `CHANGELOG.md`, avoid internal-only jargon unless required for migration/debugging; when unavoidable, add a short plain-language explanation in the same bullet.
* In root `CHANGELOG.md`, keep each minor-line summary focused on shipped behavior and outcomes; avoid WIP/meta narration.
* In root `CHANGELOG.md`, use exactly one bullet per patch version in each minor-line summary.
* In root `CHANGELOG.md`, each patch bullet is a summary sentence, not a full change list.
* In root `CHANGELOG.md`, do not chain many clauses to enumerate every internal change from that patch; move detail to `docs/changelog/0.*.md`.
* In `docs/changelog/0.*.md`, include implementation detail, architectural rationale, and subsystem terminology needed by maintainers and domain experts.
* In `docs/changelog/0.*.md`, prefer precision over simplification, but keep claims concrete and avoid unnecessary verbosity.
* Never copy internal design-doc phrasing directly into changelog summaries.
* Keep bullets short; in root minor-line summaries keep one concise bullet per patch, and use `docs/changelog/0.*.md` for exhaustive breakdowns.
* Inline fenced examples are optional, not mandatory.
* In root `CHANGELOG.md`, include at most one inline fenced example per minor version (`0.x.x` line) when it materially improves clarity.
* In `docs/changelog/0.*.md`, include at most one inline fenced example per patch entry (`## 0.x.y`) when it materially improves clarity.
* Use inline fenced examples only for meaningful code, config, or flow snapshots that explain behavior better than prose; if no good example exists, skip it.
* Before finalizing root `CHANGELOG.md` text, perform one plain-language pass that removes unnecessary architecture jargon.

---

## Review Checklist (Non-Exhaustive)

* [ ] Imports declared once at top using `crate::{...}`
* [ ] No `super::` usage outside tests without justification
* [ ] No large unexplained blocks of logic
* [ ] Non-trivial functions over ~30 lines include labeled phase/header comments
* [ ] Complex functions are commented in phases
* [ ] Public APIs document invariants and intent

---

## Security & Configuration

* Run `make security-check` before release.
* Never modify pushed release tags.
* Pin git dependencies by tag in downstream projects.
* Ensure local toolchain matches CI (`rustup toolchain install 1.94.1`).
