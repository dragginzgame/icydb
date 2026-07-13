# IcyDB Agent Rules

Keep this file small. Open detailed governance docs only when the task needs them.

## Hard Rules

- Do not add Python to committed files; Codex may use local Python for one-off analysis/audit extraction when it does not become project code.
- Do not run `git commit` or `git push`.
- Do not edit Cargo workspace/package version numbers in `Cargo.toml` or `Cargo.lock`; `make patch` owns version bumps. If version churn is present, report it and leave it alone unless the user explicitly asks for release tooling.
- Do not revert user or unrelated dirty-worktree changes; re-read affected files and continue.
- Do not start or stop the local ICP network; the user manages its lifecycle elsewhere.
- Do not run the full `make test` suite; the user runs it as part of the push workflow. Use targeted tests for the changed slice instead.
- Use absolute filesystem paths in final file references.
- Before `1.0.0`, follow the hard-cut compatibility rules below; do not keep legacy fallbacks.
- For wasm decisions, prioritize raw non-gzipped `.wasm` bytes; gzip is secondary context.

## Pre-1.0 Hard Cuts

- Before `1.0.0`, removed or renamed surfaces are hard-cut. Do not add aliases,
  shims, compatibility wrappers, legacy fallback paths, dual dispatch,
  backwards-compatibility layers, or legacy feature support unless the user
  explicitly asks.
- Internal protocols, persisted/runtime formats, generated API shapes, cursor
  formats, and schema/catalog representations should move directly to the
  latest current form. Either decode/execute the current form or fail with a
  typed error; do not silently reconstruct, translate, or tolerate old forms.
- Before `1.0.0`, do not add, keep, or maintain anti-resurrection tests for
  removed legacy behavior, old aliases, retired feature spellings, or deleted
  compatibility paths. Delete tests whose only purpose is proving the old path
  stays gone; keep or add tests for the maintained current surface instead.
- When deleting stale code, remove the old path completely and update active
  docs, examples, diagnostics, and fixtures to the current surface instead of
  preserving compatibility breadcrumbs.

## IcyDB Architecture Rules

- Accepted schema snapshots are runtime authority.
- Generated `EntityModel` / `IndexModel` are allowed only for proposal, reconciliation, model-only convenience, and tests.
- Do not add runtime fallback reconstruction from generated models.
- Schema mutation work must remain catalog-native; SQL DDL is a frontend, not the source of mutation semantics.
- Generated canister endpoint exports use `icydb_*` public method names; generated hidden Rust wrappers may use `__icydb_*` names to avoid collisions with plain non-exported user hooks.

## Cost / Scope Control

- Start with `rg` and targeted inspection; do not read broad directories unless the task requires it.
- Make the smallest safe patch that satisfies the request.
- Do not perform opportunistic refactors; list them as follow-up instead.
- Prefer focused code slices. A slice is a review/landing unit, not automatically a patch release.
- Batch coherent routine work before asking whether to push; do not stop after every small slice unless the user asks.
- Run `cargo fmt --all` after code edits; reserve `cargo fmt --all --check` for non-mutating release/readiness verification.
- Run focused checks after edits; run broader checks only when the slice is otherwise ready.
- Do not repeatedly rerun expensive failing commands; capture the first failure and report it.
- Report perf and wasm-size deltas alongside a complexity delta: files touched,
  approximate line delta, and whether the implementation shape got simpler,
  stayed neutral, or became more complex.

## Lookup Docs

- Agent details: `docs/governance/agent-operating-manual.md`
- Changelog rules: `docs/governance/changelog.md`
- Slice/PR governance: `docs/governance/velocity-preservation.md`
- Code hygiene/style: `docs/governance/code-hygiene/README.md`

## Defaults To Remember

- Imports: `mod`, blank line, `use`, blank line, `pub use`; prefer grouped `use crate::{...}`.
- Copyable style examples live under `docs/governance/code-hygiene/example-crate/`.
- Avoid `super::` outside tests unless narrowly justified. Never use `#[path]` module wiring.
- Public APIs need docs; non-trivial private logic needs intent/invariant comments.
- Public APIs with reachable panic paths need `# Panics` docs; prefer typed errors or invariant helpers.
- Production executor code must not use panicking `panic!`, `assert!`, `.unwrap()`, or `.expect()`; return `InternalError`/typed errors instead. Tests and `debug_assert!` may still document invariants.
- Same-file impl order: type, inherent `impl Type`, then trait impls alphabetically.
- Do not match error strings in code or tests.
- Persisted decoding must be bounded and fallible.

## Changelog / Release Notes

- Before any changelog edit, open and follow `docs/governance/changelog.md`; it is the changelog source of truth.
- Root `CHANGELOG.md` is the only `Unreleased` location; do not add `Unreleased` sections to detailed minor files.
- Keep root `CHANGELOG.md` `Unreleased` current as part of every unpushed code slice; update it before reporting the slice complete instead of waiting for a separate changelog request.
- When the user names a target version or asks whether it is ready to push, automatically prepare its root and detailed changelog entries as part of readiness; do not wait for another changelog request.
- Create or update patch-numbered root/detailed changelog entries only during release prep for a user-named target version.
- Do not invent patch numbers, do not infer patch numbers for design/status docs, and keep release prep details governed by `docs/governance/changelog.md`.
- Governance-only edits do not need release notes unless requested.

## Push / Commit Boundaries

- Do not run `git commit` or `git push`; the user owns commits and pushes.
- If the user asks "push?", report whether the current slice is ready to push and summarize validation.
- If the user says a patch is live/pushed, start the next slice from the current worktree state and do not rewrite the published changelog unless asked.

## Final Response

Final reports should be brief, nicely formatted, and include only:

- summary
- files changed, using absolute paths
- whether validation passed
- failures or skipped checks, if any
- follow-up items

Do not list individual test/check commands unless requested.
Do not include long architectural essays unless requested.
