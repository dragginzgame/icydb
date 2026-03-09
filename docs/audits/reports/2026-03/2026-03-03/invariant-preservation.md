# Invariant Preservation Audit - 2026-03-03

Scope: identity, ordering, structural, mutation, and replay invariants in `icydb-core`.

## Invariant Registry Snapshot

| Invariant Category | Key Invariants Checked | Status | Risk |
| ---- | ---- | ---- | ---- |
| Identity | index id/namespace/arity checks for cursor anchors | PASS | Low |
| Ordering | strict continuation advancement and excluded-anchor resume bounds | PASS | Low |
| Structural | access-path shape and grouped-order policy checks | PASS | Medium |
| Mutation | preflight prepare + generation guard + marker authority | PASS | Medium |
| Recovery | replay + rebuild + marker-clear ordering | PASS | Medium |

## Boundary Mapping

| Boundary | Evidence | Result |
| ---- | ---- | ---- |
| cursor token decode -> plan/runtime validation | `db/cursor/{error,token,spine,anchor}.rs` | Preserved |
| semantic range -> raw key bounds | `db/index/range.rs` | Preserved |
| mutation planning -> apply | `db/executor/mutation/commit_window.rs` | Preserved |
| marker replay -> post-recovery structure | `db/commit/recovery.rs` | Preserved |

## Symmetry and Recovery

| Invariant | Forward Path | Replay Path | Equivalent? |
| ---- | ---- | ---- | ---- |
| Row-op prepare/apply order | commit window preflight + apply | replay marker row ops | Yes |
| Marker authority lifecycle | `begin_commit`/`finish_commit` | replay then clear marker | Yes |
| Index/store structural restoration | apply-time prepared ops | rebuild + replay path | Yes |

## Targeted Test Evidence

- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS

## Overall Invariant Risk Index

**4/10**
