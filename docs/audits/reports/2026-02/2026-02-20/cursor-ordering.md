# Cursor + Ordering Correctness Audit - 2026-02-20

Scope: continuation semantics and ordering invariants only.

## 1. Invariant Table

| Area | Invariants Assumed | Verified? | Evidence | Risk |
| ---- | ------------------ | --------- | -------- | ---- |
| Cursor token decode boundary | malformed hex tokens rejected before planning | Yes | `crates/icydb-core/src/db/cursor.rs:38`, `crates/icydb-core/src/db/mod.rs:452` | Low |
| Signature + direction gate | token must match canonical plan signature and direction | Yes | `crates/icydb-core/src/db/query/plan/cursor_spine.rs:258`, `crates/icydb-core/src/db/query/plan/cursor_spine.rs:267` | Low |
| Window-shape compatibility | token `initial_offset` must match planned offset | Yes | `crates/icydb-core/src/db/query/plan/cursor_spine.rs:272`, `crates/icydb-core/src/db/query/plan/validate/mod.rs:196` | Low |
| Envelope containment | anchor must remain inside original range envelope | Yes | `crates/icydb-core/src/db/query/plan/cursor_spine.rs:454`, `crates/icydb-core/src/db/index/range.rs:122` | Low |
| Resume semantics | continuation edge is strict excluded anchor by direction | Yes | `crates/icydb-core/src/db/query/plan/cursor_spine.rs:143`, `crates/icydb-core/src/db/index/range.rs:103` | Low |
| Monotonic advancement | store traversal rejects non-advancing continuation candidate | Yes | `crates/icydb-core/src/db/index/store/lookup.rs:140` | Low |
| Post-access strict-after filter | rows retained only if strictly after boundary | Yes | `crates/icydb-core/src/db/query/plan/logical.rs:348` | Low |
| Offset one-time consumption | continuation requests use effective offset 0 | Yes | `crates/icydb-core/src/db/query/plan/logical.rs:429`, `crates/icydb-core/src/db/executor/load/page.rs:134` | Low |
| Continuation token carries original offset | cursor payload persists `initial_offset` | Yes | `crates/icydb-core/src/db/query/plan/continuation.rs:165`, `crates/icydb-core/src/db/query/plan/continuation.rs:226` | Low |

## 2. Failure Mode Classification Table

| Failure Type | Expected Error | Actual Error | Correct? | Risk |
| ------------ | -------------- | ------------ | -------- | ---- |
| Invalid hex token | `CursorPlanError::InvalidContinuationCursor` | matches | Yes | Low |
| Non-token payload | `CursorPlanError::InvalidContinuationCursorPayload` | matches | Yes | Low |
| Unsupported token version | `CursorPlanError::ContinuationCursorVersionMismatch` | matches | Yes | Low |
| Signature mismatch | `CursorPlanError::ContinuationCursorSignatureMismatch` | matches | Yes | Low |
| Boundary arity mismatch | `CursorPlanError::ContinuationCursorBoundaryArityMismatch` | matches | Yes | Low |
| Boundary type mismatch | `CursorPlanError::ContinuationCursorBoundaryTypeMismatch` | matches | Yes | Low |
| PK slot mismatch | `CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch` | matches | Yes | Low |
| Offset mismatch (new) | `CursorPlanError::ContinuationCursorWindowMismatch` | matches | Yes | Low |
| Legacy v1 token on non-zero offset | `CursorPlanError::ContinuationCursorWindowMismatch` (actual 0 vs expected >0) | matches | Yes | Low |

## 3. Envelope Safety Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| -------- | -------------------- | ------------- | ---- |
| Anchor exactly equal to upper bound | No | accepted only if bound form allows it; continuation becomes terminal due strict excluded resume | Low |
| Anchor exactly equal to lower bound | No | accepted only when inside envelope; resume remains strict-after | Low |
| Anchor between valid keys | No | envelope check + strict monotonic progression | Low |
| Correct bytes, wrong index id | No | explicit id mismatch rejection | Low |
| Valid hex, corrupted payload | No | decode path rejects before structured validation | Low |
| Correct arity, wrong namespace | No | explicit namespace check rejects non-user keys | Low |
| Anchor for different index | No | index id mismatch rejection | Low |
| Anchor outside envelope but lexicographically valid | No | `KeyEnvelope::contains` guard | Low |
| Cursor from different predicate/plan shape | No | continuation signature mismatch | Medium (signature drift-sensitive) |
| Cursor from composite path | No | unexpected anchor + signature incompatibility rejection | Low |
| Anchor before lower bound | No | envelope lower-bound guard | Low |
| Anchor after upper bound | No | envelope upper-bound guard | Low |

## 4. Duplication/Omission Safety Table

| Mechanism | Duplication Risk | Omission Risk | Explanation | Risk |
| --------- | ---------------- | ------------- | ----------- | ---- |
| strict excluded bound rewrite | Low | Low | single helper path in `KeyEnvelope::apply_anchor` | Low |
| store-side advancement check | Low | Low | rejects non-advancing continuation | Low |
| post-access strict boundary retain | Low | Low | continuation filter is strict `>` | Low |
| offset handling on continuation | Low | Low | `effective_page_offset` zeros offset when cursor present | Low |
| next-cursor generation threshold | Low | Low | page-window end uses effective offset | Low |

## 5. Structural Mutation Table

| Property | Can Change? | Protection Mechanism | Risk |
| -------- | ----------- | -------------------- | ---- |
| Index id | No | decoded `IndexId` must match planned index | Low |
| Key namespace | No | `IndexKeyKind::User` requirement | Low |
| Component count/arity | No | explicit component count guard | Low |
| Access path variant | No | signature + access-shape validation | Low |
| Order direction | No | token direction check + plan direction | Low |
| Predicate | No | continuation signature includes predicate surface | Medium |
| Initial offset | No | token offset must match plan offset | Low |

## 6. Overall Risk Assessment

Critical issues:
- None found.

Medium-risk drift:
- Continuation signature remains the main guard against cross-query cursor reuse; signature-field drift would be safety-sensitive.
- Future expansion of cursor payload fields can raise compatibility pressure if not mirrored in decode + validation.

Low-risk observations:
- Offset semantics are now explicit and verified at plan time (`ContinuationCursorWindowMismatch`).
- Continuation token versioning is backward-compatible (v1 decode remains supported with `initial_offset=0`).
- Store and planner both enforce strict continuation monotonicity.

Areas requiring additional tests:
- Explicit namespace-mismatch adversarial cursor fixture (if not already covered via generated raw key fixtures).
- Additional DESC matrix rows with non-zero initial offsets across secondary-index paths.

Overall Cursor/Ordering Risk Index (1-10, lower is better): **3/10**

Interpretation:
- 1-3 = Low risk / structurally healthy
- 4-6 = Moderate risk / manageable pressure
- 7-8 = High risk / requires monitoring
- 9-10 = Critical risk / structural instability

Targeted verification run during this audit:
- `cargo test -p icydb-core cursor_validation -- --nocapture`
- `cargo test -p icydb-core load_cursor_with_offset_applies_offset_once_across_pages -- --nocapture`
