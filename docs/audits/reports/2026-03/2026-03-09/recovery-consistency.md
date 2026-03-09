# Recovery Consistency & Replay Equivalence Audit - 2026-03-09

## Report Preamble

- scope: parity between normal commit-window execution and recovery replay
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-08/recovery-consistency.md`
- code snapshot identifier: `b29df45d`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Mutation Inventory

| Mutation Type | Normal Execution Entry | Recovery Entry |
| ---- | ---- | ---- |
| Marker lifecycle | `begin_commit` / `finish_commit` (`db/commit/guard.rs`) | `ensure_recovered` (`db/commit/recovery.rs`) |
| Prepared row apply | `open_commit_window` / apply path (`db/executor/mutation/commit_window.rs`) | `replay_commit_marker_row_ops` (`db/commit/replay.rs`) |
| Reverse/index side effects | normal row-op application | replayed row-op application + rebuild |

## Side-by-Side Flow Comparison

| Phase | Normal Execution | Recovery Replay | Identical? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Pre-mutation checks | preflight prepare before apply | marker decode + replay preparation | Yes | Low |
| Marker persistence authority | marker persisted before apply | marker consumed as durable source of truth | Yes | Low |
| Failure behavior | marker retained on error | replay retried until successful or fail-fast | Yes | Medium |
| Success finalization | marker clear post-success | marker clear post-successful replay | Yes | Low |

## Overall Recovery Risk Index

**4/10**

## Follow-Up Actions

- None required for this run.

## Verification Readout

- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
- `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
