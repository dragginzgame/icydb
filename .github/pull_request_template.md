## Summary

Describe the problem and the approach taken. Keep it brief and specific.

## Changes

- What changed at a high level
- Notable internal refactors or new modules
- User-visible behavior/API changes

## Breaking Changes / Deprecations

- List breaking changes (if any) and migration notes

## Tests

- Outline test coverage added/updated
- How to reproduce locally

## Slice Shape

Primary domains touched:

- [ ] Parser
- [ ] Lowering / Session
- [ ] Executor / Planner
- [ ] Build / Canister
- [ ] Integration Tests

If this PR exceeds the slice-shape limits, include these exact trailer lines in
the PR body:

`Slice-Override: yes`

`Slice-Justification: <why the cross-layer change is unavoidable>`

## Screenshots / Logs (optional)

Attach outputs that help reviewers verify behavior.

## Checklist

- [ ] Ran `make fmt-check` (or `cargo fmt --all -- --check`)
- [ ] Ran `make clippy` (no warnings)
- [ ] Ran `make test` (all green)
- [ ] Updated `CHANGELOG.md` (under `[Unreleased]`) when user-visible
- [ ] Updated docs/examples where relevant
- [ ] Linked related issues (e.g., `Closes #123`)
