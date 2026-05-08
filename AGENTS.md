# IcyDB Agent Rules

Keep this file small. Open detailed governance docs only when the task needs them.

## Hard Rules

- No Python: use existing Rust/shell patterns for tooling, scripts, tests, and build helpers.
- Do not run `git commit` or `git push`.
- Do not revert user or unrelated dirty-worktree changes; re-read affected files and continue.
- Do not start or stop `dfx`; the user manages its lifecycle elsewhere.
- Use absolute filesystem paths in final file references.
- Before `1.0.0`, hard-cut internal protocols/formats to the latest version; do not keep compatibility fallbacks.
- For wasm decisions, prioritize raw non-gzipped `.wasm` bytes; gzip is secondary context.

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
- Run focused checks after edits. If `make test` fails once, do not rerun it in the same run unless asked.
- Update changelogs for user-visible changes; governance-only edits do not need release notes unless requested.
- Do not infer patch numbers for design/status docs.
