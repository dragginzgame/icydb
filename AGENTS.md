# IcyDB Agent Rules

Keep this file small. Open detailed governance docs only when the task needs them.

## Hard Rules

- Do not add Python to committed files; Codex may use local Python for one-off analysis/audit extraction when it does not become project code.
- Do not run `git commit` or `git push`.
- Do not edit Cargo workspace/package version numbers in `Cargo.toml` or `Cargo.lock`; `make patch` owns version bumps.
- Do not revert user or unrelated dirty-worktree changes; re-read affected files and continue.
- Do not start or stop `dfx`; the user manages its lifecycle elsewhere.
- Use absolute filesystem paths in final file references.
- Before `1.0.0`, hard-cut internal protocols/formats to the latest version; do not keep compatibility fallbacks.
- For wasm decisions, prioritize raw non-gzipped `.wasm` bytes; gzip is secondary context.

## IcyDB Architecture Rules

- Accepted schema snapshots are runtime authority.
- Generated `EntityModel` / `IndexModel` are allowed only for proposal, reconciliation, model-only convenience, and tests.
- Do not add runtime fallback reconstruction from generated models.
- Schema mutation work must remain catalog-native; SQL DDL is a frontend, not the source of mutation semantics.

## Cost / Scope Control

- Start with `rg` and targeted inspection; do not read broad directories unless the task requires it.
- Make the smallest safe patch that satisfies the request.
- Do not perform opportunistic refactors; list them as follow-up instead.
- Prefer one patch per requested slice; do not expand scope without explicit instruction.
- Run focused checks after edits; run broader checks only when the slice is otherwise ready.
- Do not repeatedly rerun expensive failing commands; capture the first failure and report it.

## Lookup Docs

- Agent details: `docs/governance/agent-operating-manual.md`
- Changelog rules: `docs/governance/changelog.md`
- Slice/PR governance: `docs/governance/velocity-preservation.md`
- Code hygiene background: `docs/governance/code-hygiene.md`

## Defaults To Remember

- Imports: `mod`, blank line, `use`, blank line, `pub use`; prefer grouped `use crate::{...}`.
- Avoid `super::` outside tests unless narrowly justified. Never use `#[path]` module wiring.
- Public APIs need docs; non-trivial private logic needs intent/invariant comments.
- Do not match error strings in code or tests.
- Persisted decoding must be bounded and fallible.
- Update changelogs for user-visible changes; governance-only edits do not need release notes unless requested.
- Do not infer patch numbers for design/status docs.

## Final Response

Final reports should be concise and include only:

- summary
- files changed, using absolute paths
- whether validation passed
- failures or skipped checks, if any
- follow-up items

Do not list individual test/check commands unless requested.
Do not include long architectural essays unless requested.