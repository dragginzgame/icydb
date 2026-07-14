# Invariant Preservation Audit - 2026-03-08

Scope: identity, ordering, structural, mutation, and recovery invariants in `icydb-core`.

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

## Symmetry and Recovery

| Invariant | Normal Path | Recovery Path | Equivalent? |
| ---- | ---- | ---- | ---- |
| Marker lifecycle | `begin_commit`/`finish_commit` | replay + clear on success | Yes |
| Row-op application | commit-window apply | marker row-op replay | Yes |
| Index/store restoration | runtime apply | replay + rebuild | Yes |

## Verification Readout

- `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
- `bash scripts/ci/check-field-projection-invariants.sh` -> PASS
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS

## Overall Invariant Risk Index

**4/10**
