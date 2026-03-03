# Recovery Consistency & Replay Equivalence Audit - 2026-03-03

Scope: equivalence between normal commit-window execution and recovery replay.

## Mutation Inventory

| Mutation Type | Normal Entry | Recovery Entry |
| ---- | ---- | ---- |
| Marker lifecycle | `begin_commit` / `finish_commit` (`db/commit/guard.rs`) | `ensure_recovered` + replay (`db/commit/recovery.rs`) |
| Prepared row apply | `apply_prepared_row_ops` (`db/executor/mutation/commit_window.rs`) | `replay_commit_marker_row_ops` via recovery path |
| Reverse/index mutation effects | prepared row ops + relation reverse index planning | replayed from marker row ops + rebuild |

## Side-by-Side Flow Comparison

| Phase | Normal | Recovery | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Pre-mutation validation | preflight prepare | replay prepares from persisted row ops | Yes | Low |
| Marker authority | persisted before apply | respected during replay | Yes | Low |
| Failure handling | marker retained on apply error | replay retried until success | Yes | Medium |
| Post-success marker clear | clear after successful apply | clear after successful replay + rebuild | Yes | Low |

## Idempotence and Determinism

| Scenario | Evidence | Result |
| ---- | ---- | ---- |
| Replay repeated | `db::commit::tests::recovery_replay_is_idempotent` | PASS |
| Marker-present startup recovery | `ensure_recovered` logic in `db/commit/recovery.rs` | PASS |
| Deterministic rebuild ordering | explicit replay + rebuild sequence in recovery | PASS |

## Targeted Test Evidence

- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS

## Overall Recovery Risk Index

**4/10**
