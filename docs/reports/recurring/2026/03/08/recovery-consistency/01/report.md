# Recovery Consistency & Replay Equivalence Audit - 2026-03-08

Scope: parity between normal commit-window execution and recovery replay.

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

## Invariant Enforcement Parity

| Invariant | Normal | Recovery | Same Phase? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Unique and relation constraints | Yes | Yes (replayed ops) | Yes | Medium |
| Index/store coupling | Yes | Yes | Yes | Medium |
| Marker lifecycle semantics | Yes | Yes | Yes | Low |

## Verification Readout

- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
- `bash scripts/ci/check-memory-id-invariants.sh` -> PASS

## Overall Recovery Risk Index

**4/10**
