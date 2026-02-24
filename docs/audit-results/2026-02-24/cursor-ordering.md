# Cursor + Ordering Correctness Audit - 2026-02-24

Scope: continuation semantics and ordering invariants only.

## 1. Invariant Table

| Area | Invariants Assumed | Verified? | Evidence | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Token decode boundary | malformed token rejected before planning | Yes | `crates/icydb-core/src/db/query/cursor/spine.rs:262`, `crates/icydb-core/src/db/query/cursor/continuation.rs:80` | Low |
| Signature compatibility | token signature must match executable signature | Yes | `crates/icydb-core/src/db/query/cursor/spine.rs:249`, `crates/icydb-core/src/db/query/cursor/spine.rs:269` | Low |
| Direction compatibility | token direction must match executable direction | Yes | `crates/icydb-core/src/db/query/cursor/spine.rs:250`, `crates/icydb-core/src/db/query/cursor/spine.rs:288` | Low |
| Window-shape compatibility | token `initial_offset` must match executable offset | Yes | `crates/icydb-core/src/db/query/cursor/spine.rs:251`, `crates/icydb-core/src/db/query/cursor/spine.rs:303` | Low |
| Envelope containment | anchor must remain inside planned range envelope | Yes | `crates/icydb-core/src/db/query/cursor/anchor.rs:90`, `crates/icydb-core/src/db/query/cursor/anchor.rs:105` | Low |
| Strict continuation advancement | candidate must be strictly after anchor by direction | Yes | `crates/icydb-core/src/db/query/cursor/spine.rs:202`, `crates/icydb-core/src/db/index/store/lookup.rs:111` | Low |
| Offset-one-time semantics | continuation requests consume effective offset once | Yes | `crates/icydb-core/src/db/query/plan/logical/mod.rs:455`, `crates/icydb-core/src/db/executor/load/mod.rs:373` | Low |

## 2. Failure Mode Classification Table

| Failure Type | Expected Error | Observed Mapping | Correct? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| Unknown token version | `ContinuationCursorVersionMismatch` | mapped in cursor spine | Yes | Low |
| Signature mismatch | `ContinuationCursorSignatureMismatch` | mapped in cursor spine | Yes | Low |
| Boundary arity mismatch | `ContinuationCursorBoundaryArityMismatch` | mapped in cursor spine | Yes | Low |
| Boundary type mismatch | `ContinuationCursorBoundaryTypeMismatch` | mapped in cursor spine | Yes | Low |
| PK slot mismatch | `ContinuationCursorPrimaryKeyTypeMismatch` | mapped in cursor spine | Yes | Low |
| Offset mismatch | `ContinuationCursorWindowMismatch` | mapped in cursor spine | Yes | Low |

## 3. Envelope Safety Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| ---- | ---- | ---- | ---- |
| Anchor before lower bound | No | `contains` guard rejects | Low |
| Anchor after upper bound | No | `contains` guard rejects | Low |
| Correct token bytes but wrong index id | No | anchor consistency validation rejects | Low |
| Correct token shape but wrong predicate signature | No | signature mismatch rejects | Medium |

## 4. Duplication/Omission Safety Table

| Mechanism | Duplication Risk | Omission Risk | Explanation | Risk |
| ---- | ---- | ---- | ---- | ---- |
| strict `apply_anchor` rewrite | Low | Low | continuation edge becomes excluded anchor | Low |
| store-side advancement gate | Low | Low | non-advancing candidate rejected | Low |
| post-access cursor boundary filter | Low | Low | strict boundary ordering retained | Low |
| effective offset computation | Low | Low | offset is zeroed for continuation pages | Low |

## 5. Structural Mutation Table

| Property | Can Change? | Protection Mechanism | Risk |
| ---- | ---- | ---- | ---- |
| Access-path variant | No | typed plan + signature validation | Low |
| Direction | No | explicit direction gate | Low |
| Initial offset | No | explicit offset gate | Low |
| Boundary width/type | No | arity/type checks | Low |
| Index-range anchor compatibility | No | boundary/anchor consistency checks | Low |

## 6. Overall Risk Assessment

Critical issues:
- None observed.

Medium-risk drift vectors:
- Cursor token shape growth remains sensitive (new fields require decode + validation + tests).
- Signature field composition remains a key safety gate and must stay stable.

Low-risk observations:
- `initial_offset` compatibility is explicit and tested.
- Envelope semantics remain centralized and deterministic.

## 7. Overall Cursor/Ordering Risk Index (1-10, lower is better)

**3/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability
