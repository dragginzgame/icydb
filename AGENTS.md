# IcyDB Agent Rules

Apply the local [shared engineering baseline](DRAGGINZGAME.md), recorded in
[the snapshot manifest](.shared-tooling.snapshot). This file is IcyDB's local overlay.
The shared [approved layout](rules/rust-workspaces.md#approved-icydb-layout)
retains existing packages under `crates/`, `canisters/`, `schema/` and `testing/`
in the single root workspace and lockfile. Adoption does not authorize relocation
or another workspace; [issue #310](https://github.com/dragginzgame/icydb/issues/310)
owns future placement decisions. See [shared-tooling adoption](docs/governance/shared-tooling.md)
for provenance and consumer boundaries. Keep this file small; open detailed docs as needed.

## Hard Rules

- Do not add Python to committed files; Codex may use local Python for one-off analysis/audit extraction when it does not become project code.
- Use GitHub issues exclusively for upstream feedback and follow-up tracking.
  Check issue bodies and comments; local notes may summarize linked evidence
  but must not become a separate feedback queue.
- Do not edit Cargo workspace/package version numbers in `Cargo.toml` or `Cargo.lock`; the shared release workflow owns version bumps. If version churn is present, report it and leave it alone unless the user explicitly asks for release tooling.
- Do not revert user or unrelated dirty-worktree changes; re-read affected files and continue.
- IC Testkit is the sole PocketIC infrastructure owner: server selection,
  provisioning, admission and lifecycle. IcyDB consumes Testkit; do not add
  application-owned server pins, version parsers, installers or supervision.
  The remaining published setup handoff is tracked in
  [Testkit #38](https://github.com/dragginzgame/ic-testkit/issues/38).
- Codex may start, stop, or restart local ICP and PocketIC networks when required
  by the requested development, validation, or measurement work. Avoid
  unrelated lifecycle churn and report any network lifecycle action taken.
- Retain the local validation exception, documented in the
  [operating manual](docs/governance/agent-operating-manual.md#workflow), to keep
  broad gates explicitly selected. Run focused checks during development.
  Full repository/workspace tests, broad
  checks and release gates require an explicit request or configured CI.
  Continuation and readiness do not authorize those gates; report them as
  skipped user-owned validation when they were not requested.
- Use absolute filesystem paths in final file references.
- macOS host workflows are required by the baseline. See the local
  [host qualification matrix](docs/governance/shared-tooling.md#host-qualification)
  for declared hosts and evidence gaps; Linux checks do not qualify macOS.
- Before `1.0.0`, follow the hard-cut compatibility rules below; do not keep legacy fallbacks.
- For wasm decisions, prioritize raw non-gzipped `.wasm` bytes; gzip is secondary context.
- Performance metrics are Wasm size, IC cycles, and instruction counts only.
  Never use wall-clock/native timing as a performance metric, proxy, or release
  gate; do not run timing benchmarks or investigate timing regressions.
  If the relevant permitted measurement is unavailable, report it as unmeasured
  rather than substituting elapsed time.

## SemVer Terminology

- In user instructions, bare `patch` always means the SemVer patch component in
  `major.minor.patch`. For example, "patch 6" on the authorized `0.249` line
  means version `0.249.6`.
- Never interpret `patch` as a design-plan unit, tracker entry, implementation
  slice, worktree handoff, or diff. Call those units `landing slices` or
  `tracker items`; only use another meaning when the user explicitly names it.

## Pre-1.0 hard cuts

Follow the [shared hard-cut contract](DRAGGINZGAME.md#pre-10-contracts), including
retained-data obligations and same-contract recovery. Remove superseded paths
and update active docs, diagnostics and fixtures; test the maintained surface
rather than prohibiting retired names.

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
- For saved-review repairs, qualify affected boundary families and prevent facts
  drifting between owners; follow the repair discipline in `docs/code-review/status.md`.
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
- A landing slice is a reviewable outcome, not a compulsory agent-turn limit.
  Complete the accepted coherent in-repository batch through its implementation,
  focused checks, direct propagation, cleanup and current changelog draft.
- Ordinary continuation resumes that accepted scope in the current release line.
  Stop at new independent scope or a release boundary; continuation never
  authorizes starting another minor or publishing work.
- Split independently reviewable outcomes rather than compiler fallout or each
  proof of one change. Do not invent a release version for each landing slice.
- Treat file and delivery-domain counts as reporting signals, not execution
  limits. Include direct tests, documentation, fixtures, exhaustive matches,
  and mechanical propagation required by the current planned outcome. If work
  reveals another independently reviewable outcome, stop and split or update
  the tracker instead of folding it into the active landing slice.
- Run `cargo fmt --all` after code edits; reserve `cargo fmt --all --check` for non-mutating release/readiness verification.
- Run focused checks after edits; run broader checks only when the slice is otherwise ready.
- When focused validation reports a Clippy warning, stop later validation, fix
  every warning in that selected gate, and rerun it before handoff. Do not
  expand to workspace-wide `make clippy` without explicit authorization.
- Do not repeatedly rerun expensive failing commands; capture the first failure and report it.
- Report measured cycle/instruction and wasm-size deltas alongside a complexity delta: files touched,
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

- Before any changelog edit, follow `docs/governance/changelog.md`.
- Keep meaningful completed behavior/tooling notes in the latest current root
  and minor-line draft before handoff. Governance-only edits need no note unless
  requested. Never create an `Unreleased` section or a separate notes queue.
- Keep one numbered, undated pending entry under `rules/changelogs.md`.
  Derive its version from the latest finalized release and the whole pending
  batch; move only pending detail notes when its minor line changes. Numbering
  does not authorize Cargo version changes or release execution.
- Preserve published notes and tags. An explicitly supplied target needs root
  and detailed release notes; report SemVer conflicts rather than renumbering it.
- Changelog position, draft labels or a missing chosen version must not block
  deployment. Report and repair missing notes when practical.

## Push / Commit Boundaries

- Follow the shared [contribution rules](rules/contributions.md). Ordinary work
  leaves staging, commits and pushes to the user. An explicitly requested PR
  includes its scoped branch, commits, branch push and PR; merging, integration
  pushes and release effects retain separate authority. A PR is not permission
  to change package versions or run the maintainer-owned release workflow.
- If the user asks "push?", report whether the current slice is ready to push and summarize validation.
- A statement that a release is live/pushed records the completed boundary.
  Continued work resumes the accepted coherent scope in that line, uses the
  current draft and preserves published notes. Publication itself does not
  authorize new implementation or a different minor.
- When accepted implementation scope is exhausted, continuation starts a
  read-only closeout audit in the current line. Report independent findings
  before extending scope; keep approved compatible corrections in that line.
- Do not start a new minor-version line until the current minor has a reported
  ready/complete closeout verdict and the user then directs the agent to start
  the next minor (for example, "start the next minor"). Select its number
  automatically under the shared numbering rules: increment the minor by one,
  reset patch to zero, and reuse that pending line until released. Do not ask
  the user to supply or confirm the number. A
  roadmap, existing next design, clean worktree, successful push, or question
  such as "what is next?" is not authorization to cross the minor boundary.
  Automatically selecting a pending changelog heading is documentation work,
  not authorization to implement another minor line.

## Final Response

Final reports should be brief, nicely formatted, and include only:

- summary
- files changed, using absolute paths
- whether validation passed
- failures or skipped checks, if any
- follow-up items

Do not list individual test/check commands unless requested.
Do not include long architectural essays unless requested.
