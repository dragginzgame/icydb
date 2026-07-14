# Invariant Preservation Audit - 2026-02-24

Scope: invariant preservation in `icydb-core` with emphasis on projection follow-up (`0.28.1`) and existing cursor/recovery guarantees.

## 1. Invariant Registry

| Invariant | Enforced Where | Structural or Runtime | Risk |
| ---- | ---- | ---- | ---- |
| Projection runs after canonical effective row selection | `crates/icydb-core/src/db/executor/load/aggregate.rs:684`, `crates/icydb-core/src/db/executor/load/aggregate.rs:698` | Structural | Low |
| Query-level `DISTINCT` remains row-level; value-level distinct is terminal-only | `crates/icydb-core/src/db/executor/tests/aggregate.rs:1496`, `crates/icydb-core/src/db/executor/tests/aggregate.rs:1515` | Runtime + tests | Low |
| `distinct_values_by(field)` preserves first-observed order | `crates/icydb-core/src/db/executor/load/aggregate.rs:1123`, `crates/icydb-core/src/db/executor/tests/aggregate.rs:1550` | Structural + tests | Low |
| Unknown projection field fails before scan work | `crates/icydb-core/src/db/executor/load/aggregate.rs:519`, `crates/icydb-core/src/db/executor/tests/aggregate.rs:1607` | Runtime + tests | Low |
| Cursor continuation remains strict-after boundary | `crates/icydb-core/src/db/query/cursor/spine.rs:177`, `crates/icydb-core/src/db/index/store/lookup.rs:111` | Structural | Low |
| Commit durability authority remains marker + replay | `crates/icydb-core/src/db/commit/guard.rs:124`, `crates/icydb-core/src/db/commit/recovery.rs:94` | Structural | Medium |

## 2. Boundary Mapping

| Boundary | Upstream Contract | Downstream Enforcement | Drift Risk |
| ---- | ---- | ---- | ---- |
| Query fluent -> session -> executor projection terminals | field-name resolution once, slot-based projection | `crates/icydb-core/src/db/query/fluent/load.rs:374`, `crates/icydb-core/src/db/session.rs:392`, `crates/icydb-core/src/db/executor/load/aggregate.rs:684` | Low |
| Cursor token -> executable plan | signature/direction/window compatibility | `crates/icydb-core/src/db/query/cursor/spine.rs:249`, `crates/icydb-core/src/db/query/cursor/spine.rs:250`, `crates/icydb-core/src/db/query/cursor/spine.rs:251` | Medium |
| Commit window -> recovery replay | row-op preparation/apply symmetry | `crates/icydb-core/src/db/executor/mutation/commit_window.rs:155`, `crates/icydb-core/src/db/commit/recovery.rs:94` | Medium |

## 3. Invariant Enforcement Mapping

| Invariant | Primary Enforcement | Secondary Guard | Test Coverage | Risk |
| ---- | ---- | ---- | ---- | ---- |
| `values_by` parity with `execute()` | `aggregate.rs` projection helpers | session facade wiring | `aggregate.rs` tests near `3597` | Low |
| distinct dedup parity vs values dedup | `project_distinct_field_values_from_materialized` | session-level wrapper | `aggregate.rs:1550`, `aggregate.rs:3748` | Low |
| scan-budget parity for projection terminals | route-owned execution path reuse | no new planner branches | `aggregate.rs:3850`, `aggregate.rs:3875`, `aggregate.rs:3912` | Medium |
| cursor window-offset compatibility | `ContinuationCursorWindowMismatch` validation | decode compatibility for V1/V2 | `cursor_validation.rs:200` | Medium |

## 4. Symmetry and Recovery Audit

| Invariant | Normal Path | Recovery/Replay Path | Equivalent? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Row apply ordering | `apply_prepared_row_ops` | `replay_recovery_row_ops` | Yes | Low |
| Rollback on partial failure | rollback stack in commit window | reverse rollback in replay | Yes | Low |
| Marker lifecycle | `begin_commit` -> `finish_commit` | replay then clear marker | Yes | Low |
| Read-before-recover prevention | recovered context gate | `ensure_recovered` | Yes | Low |

## 5. High-Risk Focus Areas

### A. Cursor Envelope Safety
- Stable: envelope containment and strict-after checks remain centralized in `crates/icydb-core/src/db/query/cursor/spine.rs:187` and `crates/icydb-core/src/db/index/range.rs:197`.

### B. Index Key Ordering Guarantees
- Stable: canonical key encode/decode and ordered comparison remain in `crates/icydb-core/src/db/index/key/codec.rs:66` and `crates/icydb-core/src/db/index/key/ordered.rs:146`.

### C. Reverse Relation Index Correctness
- Stable with moderate sensitivity due multi-phase replay/mutation (`crates/icydb-core/src/db/relation/reverse_index.rs:198`, `crates/icydb-core/src/db/executor/tests/semantics.rs:1662`).

### D. Recovery Idempotence
- Stable and tested (`crates/icydb-core/src/db/commit/tests.rs:294`, `crates/icydb-core/src/db/commit/recovery.rs:49`).

### E. Projection Terminal Integrity
- New in this cycle: projection terminal family is additive and parity-locked (`crates/icydb-core/src/db/query/fluent/load.rs:386`, `crates/icydb-core/src/db/query/fluent/load.rs:398`, `crates/icydb-core/src/db/query/fluent/load.rs:413`, `crates/icydb-core/src/db/query/fluent/load.rs:425`).

## 6. Enforcement Quality Evaluation

### High Risk Invariants
- Commit marker + replay authority (cross-layer, must stay mechanically equivalent).
- Cursor compatibility gates (signature/direction/initial_offset) where token-shape drift is possible.

### Redundant Enforcement (Defensive and Acceptable)
- Cursor boundary checks exist in both plan validation and executable/cursor validation layers.
- Projection unknown-field handling is validated early and reasserted in terminal execution tests.

### Missing Enforcement
- No critical missing enforcement found for the `0.28.1` projection additions.

## 7. Drift Sensitivity Analysis

| Drift Vector | Current Signal | Sensitivity | Risk |
| ---- | ---- | ---- | ---- |
| Projection terminal surface growth | `values_by` + `distinct_values_by` + id/value + first/last | Moderate | Medium |
| Cursor token contract | `initial_offset` compatibility remains enforced | Moderate | Medium |
| Access-path fan-out | 17 non-test files, 163 token refs | High | Medium-High |
| Commit/replay invariants | stable; no topology change in this cycle | Moderate | Medium |

## 8. Findings Summary

- Projection invariants are structurally coherent and test-locked.
- No semantic drift was detected between `values_by` and `distinct_values_by` contracts.
- Core risk remains in pre-existing cursor/recovery surfaces rather than projection additions.

## 9. Overall Invariant Risk Index (1-10, lower is better)

**4/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

## 10. Cross-Layer Invariant Matrix (Execution-Contract Mapping)

Legend:
- `M` = enforced mechanically
- `C` = enforced by convention
- `N` = not applicable

| Operation | Row state | Secondary index | Unique index | Reverse index | Continuation | Replay equivalence | Isolation boundaries |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Insert | M | M | M | M | N | M | C |
| Update (indexed field change) | M | M | M | M | N | M | C |
| Delete | M | M | N | M | N | M | C |
| Replay insert/update/delete | M | M | M (except delete) | M | N | M | C |
| Cursor resume / range scan | M | M | N | N | M | N | M |
| Aggregate fast-path / fallback | M | M | M | N | N | N | M |
