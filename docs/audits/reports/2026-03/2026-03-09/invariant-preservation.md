# Invariant Preservation Audit - 2026-03-09

## Report Preamble

- scope: identity, ordering, structural, mutation, and recovery invariants in `icydb-core`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-08/invariant-preservation.md`
- code snapshot identifier: `b29df45d`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Invariant Registry Snapshot

| Category | Key Invariants Checked | Status | Risk |
| ---- | ---- | ---- | ---- |
| Identity | cursor anchor index-id/namespace/arity match | PASS | Low |
| Ordering | excluded-anchor resume + strict continuation advancement | PASS | Low |
| Structural | planner policy gating and access-shape stability | PASS | Medium |
| Mutation | commit-window preflight + marker authority | PASS | Medium |
| Recovery | replay idempotence and deterministic rebuild sequencing | PASS | Medium |

## Boundary Mapping

| Boundary | Evidence | Result |
| ---- | ---- | ---- |
| cursor decode -> plan/runtime checks | `db/cursor/{token,spine,anchor,error}.rs` | Preserved |
| semantic bounds -> raw range bounds | `db/index/envelope.rs` + `db/index/scan.rs` | Preserved |
| prepared mutation -> commit guard apply | `db/executor/mutation/commit_window.rs` + `db/commit/guard.rs` | Preserved |
| marker replay -> restored runtime state | `db/commit/recovery.rs` + `db/commit/replay.rs` | Preserved |

## Overall Invariant Risk Index

**4/10**

## Follow-Up Actions

- None required for this run.

## Verification Readout

- `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
- `bash scripts/ci/check-field-projection-invariants.sh` -> PASS
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
