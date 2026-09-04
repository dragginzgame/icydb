# IcyDB Agent Rules

Keep this file small. Open detailed governance docs only when the task needs them.

## Hard Rules

- Do not add Python to committed files; Codex may use local Python for one-off analysis/audit extraction when it does not become project code.
- Do not run `git commit` or `git push`.
- Do not edit Cargo workspace/package version numbers in `Cargo.toml` or `Cargo.lock`; `make patch` owns version bumps. If version churn is present, report it and leave it alone unless the user explicitly asks for release tooling.
- Do not revert user or unrelated dirty-worktree changes; re-read affected files and continue.
- Codex may start, stop, or restart local ICP and PocketIC networks when required
  by the requested development, validation, or measurement work. Avoid
  unrelated lifecycle churn and report any network lifecycle action taken.
- Do not run full repository or workspace test suites, including `make test`, `cargo test --workspace`, `cargo test --all`, or equivalent commands. The user owns full-suite execution through the explicit validation or release workflow. Run only focused package, target, or named-test selections for the changed slice; when release instructions list a full suite, report it as user-owned validation instead of executing it.
- Use absolute filesystem paths in final file references.
- Before `1.0.0`, follow the hard-cut compatibility rules below; do not keep legacy fallbacks.
- For wasm decisions, prioritize raw non-gzipped `.wasm` bytes; gzip is secondary context.

## SemVer Terminology

- In user instructions, bare `patch` always means the SemVer patch component in
  `major.minor.patch`. For example, "patch 6" on the authorized `0.249` line
  means version `0.249.6`.
- Never interpret `patch` as a design-plan unit, tracker entry, implementation
  slice, worktree handoff, or diff. Call those units `landing slices` or
  `tracker items`; only use another meaning when the user explicitly names it.

## Pre-1.0 Hard Cuts

- Before `1.0.0`, removed or renamed surfaces are hard-cut. Do not add aliases,
  shims, compatibility wrappers, legacy fallback paths, dual dispatch,
  backwards-compatibility layers, or legacy feature support unless the user
  explicitly asks.
- Before `1.0.0`, every internal protocol, persisted/runtime format, generated
  API encoding, cursor encoding, or schema/catalog encoding that has a format
  version discriminator uses exactly version `1`. Do not increment a current
  pre-1.0 format to version `2`, `3`, or later. A version field exists only
  where the representation genuinely needs a versioned boundary; it is not a
  release counter or an implementation-history counter.
- Internal protocols, persisted/runtime formats, generated API shapes, cursor
  formats, and schema/catalog representations move directly to the latest
  current version-1 form by replacing the encoder, decoder, and canonical shape
  in place. Decode and execute only that current version-1 form or fail with a
  typed error. Do not retain or add predecessor-version constants, decoders,
  inspectors, upgrade bridges, translators, repair shims, fallback tags, dual
  formats, or old-form fixtures.
- An incompatible pre-1.0 representation change requires reinstall,
  recreation, or explicit regeneration of the current form. Never preserve an
  old pre-1.0 representation merely to make an in-place upgrade succeed.
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

- Avoid scope creep and incidental complexity; prioritise simplicity and
  maintainability. Prefer deleting, reusing, narrowing, or changing an existing
  authority over adding modes, abstractions, configuration, persisted states,
  or compatibility paths.
- Before adding an independent behavior axis such as a mode, configuration
  option, persisted state, execution route, cursor format, or widely consumed
  enum variant, record the demonstrated need, simplest alternative, canonical
  owner, and state-space delta.
- Prefer one semantic authority and one converged execution flow. Tests protect
  maintained behavior and boundaries, not incidental implementation shape.
- Start with `rg` and targeted inspection; do not read broad directories unless the task requires it.
- Make the smallest safe change that satisfies the request.
- Do not perform opportunistic refactors; list them as follow-up instead.
- Before implementing a minor-version line, ensure its design/status tracker
  groups the then-intended line into a practical set of meaningful landing
  slices, normally 1-12. This is an initial planning range, not a lifetime cap:
  new evidence and explicit authorization may extend the tracker without
  widening, renumbering, or combining otherwise independent landing slices.
- Make each landing slice substantive and end-to-end: one bounded outcome plus
  its direct tests, diagnostics, docs, fixtures, and mechanical propagation.
  Do not create micro-slices for fallout from the same change, and do not
  combine independent planned outcomes into a multi-hour mega-slice.
- One planned landing slice is one reviewable worktree handoff and the default
  implementation-turn boundary. Complete that slice, validate it, update its
  status and latest active-version changelog notes, then stop and hand it back;
  do not begin the next planned slice in the same turn.
- Generic continuation such as "continue", "keep going", or "next" authorizes
  exactly the next planned landing slice within the current minor-version
  line. It never authorizes starting a different minor. Implement multiple
  landing slices in one turn only when the user explicitly names them and asks
  to combine them.
- Batch coherent routine work within the current landing slice, never across
  planned slice boundaries. Record it under the latest active changelog
  version; changelog governance owns automatic next-SemVer-patch selection
  after publication.
- Treat file and delivery-domain counts as reporting signals, not execution
  limits. Include direct tests, documentation, fixtures, exhaustive matches,
  and mechanical propagation required by the current planned outcome. If work
  reveals another independently reviewable outcome, stop and split or update
  the tracker instead of folding it into the active landing slice.
- Run `cargo fmt --all` after code edits; reserve `cargo fmt --all --check` for non-mutating release/readiness verification.
- Run focused checks after edits; run broader checks only when the slice is otherwise ready.
- Do not repeatedly rerun expensive failing commands; capture the first failure and report it.
- Report perf and wasm-size deltas alongside a complexity delta: files touched,
  approximate line delta, and whether the implementation shape got simpler,
  stayed neutral, or became more complex.

## Lookup Docs

- Agent details: `docs/governance/agent-operating-manual.md`
- Changelog rules: `docs/governance/changelog.md`
- Simplicity, state-space, and debt rules: `docs/governance/simplicity-and-maintainability.md`
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
- Do not create or retain an `Unreleased` section. Record every code slice
  directly in the latest active root version and its shared minor-line notes.
- Keep the latest active root and detailed entries current before reporting a
  slice complete; do not wait for a separate changelog request.
- This is agent authoring discipline, not a mechanical push or release gate;
  a missing note must be reported and repaired when practical, but its absence
  alone does not make an otherwise ready slice unpushable.
- When the user names a target version or asks whether it is ready to push, automatically prepare its root and detailed changelog entries as part of readiness; do not wait for another changelog request.
- Treat the newest root patch without a matching release tag and not reported
  pushed/published as active. Once its tag exists or the user reports it
  published, automatically open the next patch in the same explicitly
  authorized minor line; an explicitly started new minor opens at `.0`. Never
  cross a minor boundary without the existing user authorization.
- Automatic patch selection applies only to changelog release entries. Do not
  infer patch numbers for design/status docs, and keep release-prep details
  governed by `docs/governance/changelog.md`.
- Governance-only edits do not need release notes unless requested.

## Push / Commit Boundaries

- Do not run `git commit` or `git push`; the user owns commits and pushes.
- If the user asks "push?", report whether the current slice is ready to push and summarize validation.
- A statement that a patch is live/pushed records the completed boundary but
  does not by itself authorize more implementation. If the user also says to
  continue, start exactly the next planned landing slice in the same minor line
  and do not rewrite the published changelog unless asked.
- When the current minor's planned landing slices are exhausted, generic
  continuation stays in that minor and starts a read-only closeout audit.
  Report findings before making closeout corrections; keep approved corrections
  in the same minor line.
- Do not start a new minor-version line until the current minor has a reported
  ready/complete closeout verdict and the user then explicitly names the target
  minor and directs the agent to start it (for example, "start 0.212"). A
  roadmap, existing next design, clean worktree, successful push, or question
  such as "what is next?" is not authorization to cross the minor boundary.

## Final Response

Final reports should be brief, nicely formatted, and include only:

- summary
- files changed, using absolute paths
- whether validation passed
- failures or skipped checks, if any
- follow-up items

Do not list individual test/check commands unless requested.
Do not include long architectural essays unless requested.
