# Cursor + Ordering Correctness Audit - 2026-02-18

Scope: continuation semantics and ordering invariants only.

## 1. Invariant Table

| Area | Invariants Assumed | Verified? | Evidence | Risk |
| ---- | ------------------ | --------- | -------- | ---- |
| Cursor token hex decode boundary | Invalid hex/shape tokens are rejected before planning and mapped to typed cursor errors | Yes | `crates/icydb-core/src/db/mod.rs:433`, `crates/icydb-core/src/db/mod.rs:435`, `crates/icydb-core/src/db/cursor.rs:13`, `crates/icydb-core/src/db/executor/tests/paged_builder.rs:53` | Low |
| Cursor payload decode + signature/direction gate | Token payload must decode, signature must match plan, direction must match executable plan | Yes | `crates/icydb-core/src/db/query/plan/continuation.rs:383`, `crates/icydb-core/src/db/query/plan/continuation.rs:392`, `crates/icydb-core/src/db/query/plan/continuation.rs:399` | Low |
| Plan cursor structural validation | `plan_cursor` enforces cursor order requirement, anchor envelope checks, and boundary/anchor consistency before execution | Yes | `crates/icydb-core/src/db/query/plan/executable.rs:108`, `crates/icydb-core/src/db/query/plan/executable.rs:129`, `crates/icydb-core/src/db/query/plan/executable.rs:138` | Low |
| IndexRange anchor structure | Anchor cannot change index id, key namespace, or component arity | Yes | `crates/icydb-core/src/db/query/plan/executable.rs:209`, `crates/icydb-core/src/db/query/plan/executable.rs:214`, `crates/icydb-core/src/db/query/plan/executable.rs:219` | Low |
| Envelope containment | Anchor must remain within original raw-key lower/upper bounds | Yes | `crates/icydb-core/src/db/query/plan/executable.rs:242`, `crates/icydb-core/src/db/index/range.rs:102`, `crates/icydb-core/src/db/index/range.rs:152` | Low |
| Resume semantics | Continuation rewrite uses strict `Bound::Excluded(anchor)` for ASC progression | Yes | `crates/icydb-core/src/db/index/range.rs:81`, `crates/icydb-core/src/db/index/range.rs:88`, `crates/icydb-core/src/db/index/store/lookup.rs:140` | Low |
| Raw-key monotonic advancement | Store scan checks that resumed raw key strictly advances beyond anchor | Yes | `crates/icydb-core/src/db/index/store/lookup.rs:153`, `crates/icydb-core/src/db/index/range.rs:120` | Low |
| Ordered boundary continuation | Post-access continuation boundary keeps only rows strictly after boundary (`>` under canonical order) | Yes | `crates/icydb-core/src/db/query/plan/logical.rs:511`, `crates/icydb-core/src/db/query/plan/logical.rs:523` | Low |
| Boundary/anchor agreement | IndexRange cursor boundary and raw-key anchor must resolve to same primary key | Yes | `crates/icydb-core/src/db/query/plan/executable.rs:261`, `crates/icydb-core/src/db/query/plan/executable.rs:291`, `crates/icydb-core/src/db/query/plan/executable.rs:573` | Low |
| Composite leakage prevention | Index-range anchor on non-IndexRange/composite paths is rejected | Yes | `crates/icydb-core/src/db/query/plan/executable.rs:177`, `crates/icydb-core/src/db/query/plan/executable.rs:249`, `crates/icydb-core/src/db/executor/context.rs:291` | Low |

## 2. Failure Mode Classification Table

| Failure Type | Expected Error | Actual Error | Correct? | Risk |
| ------------ | -------------- | ------------ | -------- | ---- |
| Hex decode failure | `PlanError::InvalidContinuationCursor { reason: CursorDecodeError::* }` | Matches (`InvalidHex`, `OddLength`, `Empty`) | Yes | Low |
| Payload decode failure (valid hex, invalid token payload) | `PlanError::InvalidContinuationCursorPayload` | Matches | Yes | Low |
| Boundary/anchor mismatch | `PlanError::InvalidContinuationCursorPayload` | Matches (`boundary/anchor mismatch`) | Yes | Low |
| Anchor index id mismatch | `PlanError::InvalidContinuationCursorPayload` | Matches (`index id mismatch`) | Yes | Low |
| Anchor component arity mismatch | `PlanError::InvalidContinuationCursorPayload` | Matches (`component arity mismatch`) | Yes | Low |
| Out-of-envelope anchor | `PlanError::InvalidContinuationCursorPayload` | Matches (`outside the original range envelope`) | Yes | Low |
| Anchor wrong namespace (`System` vs `User`) | `PlanError::InvalidContinuationCursorPayload` | Matches (`key namespace mismatch`) | Yes | Low |
| Unsupported token version | `PlanError::ContinuationCursorVersionMismatch` | Matches | Yes | Low |
| Cursor from different canonical query shape | `PlanError::ContinuationCursorSignatureMismatch` | Matches | Yes | Low |

Evidence: `crates/icydb-core/src/db/mod.rs:433`, `crates/icydb-core/src/db/query/plan/continuation.rs:383`, `crates/icydb-core/src/db/query/plan/validate/mod.rs:126`, `crates/icydb-core/src/db/query/plan/executable.rs:209`, `crates/icydb-core/src/db/query/plan/executable.rs:242`, `crates/icydb-core/src/db/executor/tests/paged_builder.rs:53`, `crates/icydb-core/src/db/executor/tests/cursor_validation.rs:5`, `crates/icydb-core/src/db/query/plan/executable.rs:573`, `crates/icydb-core/src/db/executor/tests/pagination.rs:1435`.

## 3. Envelope Safety Table

| Scenario | Can Escape Envelope? | Why / Why Not | Risk |
| -------- | -------------------- | ------------- | ---- |
| Anchor exactly equal to upper bound | No | Included upper is valid then continuation becomes terminal; excluded upper is rejected by envelope check | Low |
| Anchor exactly equal to lower bound | No | Included lower is valid; excluded lower is rejected by envelope check | Low |
| Anchor between two valid keys | No | Accepted only if within bounds; continuation resumes strictly after anchor | Low |
| Anchor with correct bytes but wrong index id | No | Rejected during anchor decode/identity validation | Low |
| Anchor with valid hex but corrupted payload | No | Payload decode fails before planning completes | Low |
| Anchor with correct arity but wrong namespace prefix | No | `key_kind` must be `User`; non-user rejected | Low |
| Anchor referencing different index entirely | No | `IndexId` mismatch rejected | Low |
| Anchor outside original envelope but lexicographically valid | No | `anchor_within_envelope` rejects it | Low |
| Cursor generated from different predicate | No (by signature) | Continuation signature includes predicate/access/order and is enforced | Medium (drift-sensitive if signature fields change) |
| Cursor generated from composite access path | No | Signature mismatch path, plus anchor is rejected when access path is not `IndexRange` | Low |
| Anchor sorts before lower bound | No | Envelope lower-bound check rejects | Low |
| Anchor sorts after upper bound | No | Envelope upper-bound check rejects | Low |

Evidence: `crates/icydb-core/src/db/query/plan/executable.rs:202`, `crates/icydb-core/src/db/query/plan/executable.rs:209`, `crates/icydb-core/src/db/query/plan/executable.rs:214`, `crates/icydb-core/src/db/query/plan/executable.rs:219`, `crates/icydb-core/src/db/query/plan/executable.rs:242`, `crates/icydb-core/src/db/index/range.rs:152`, `crates/icydb-core/src/db/query/plan/hash_parts.rs:360`, `crates/icydb-core/src/db/query/plan/continuation.rs:392`, `crates/icydb-core/src/db/query/plan/executable.rs:177`.

## 4. Duplication/Omission Safety Table

| Mechanism | Duplication Risk | Omission Risk | Explanation | Risk |
| --------- | ---------------- | ------------- | ----------- | ---- |
| Raw anchor -> resumed bounds rewrite | Low | Low | Resume rewrite is strict `Excluded(anchor)` in one helper | Low |
| Store-level continuation advancement guard | Low | Low | Scan aborts if traversal does not move strictly past anchor | Low |
| Post-access continuation boundary filter | Low | Low | Boundary filtering uses strict `>` under canonical order | Low |
| Boundary + anchor dual checks | Low | Medium | Boundary and anchor are both validated; consistency check currently keys on primary-key equivalence | Medium |
| Multi-page IndexRange parity and monotonicity tests | Low | Low | Tests assert strict anchor monotonicity and paged vs unbounded parity | Low |

Evidence: `crates/icydb-core/src/db/index/range.rs:81`, `crates/icydb-core/src/db/index/store/lookup.rs:153`, `crates/icydb-core/src/db/query/plan/logical.rs:511`, `crates/icydb-core/src/db/query/plan/executable.rs:261`, `crates/icydb-core/src/db/executor/tests/pagination.rs:2549`, `crates/icydb-core/src/db/executor/tests/pagination.rs:2659`.

## 5. Structural Mutation Table

| Property | Can Change? | Protection Mechanism | Risk |
| -------- | ----------- | -------------------- | ---- |
| Index id | No | Decoded anchor `index_id` must equal expected index id | Low |
| Key namespace | No | Decoded anchor `key_kind` must be `User` | Low |
| Component count (arity) | No | Decoded anchor component count must equal index field count | Low |
| Component ordering | Partially constrained | Raw anchor must decode as canonical `IndexKey` and stay in envelope; no explicit component-by-component equivalence test to boundary tuple | Medium |
| Index type / access class | No | Non-IndexRange access rejects anchor; access included in signature | Low |
| Escape `AccessPath::IndexRange` | No | Anchor rejected for non-index-range and composite access shapes | Low |
| Convert into composite access path | No | Composite path + anchor rejected; signature check also blocks plan-shape mismatch | Low |
| Modify predicate | No (by signature) | Predicate included in continuation signature profile | Medium (drift-sensitive if profile changes) |
| Modify order direction | No | Order direction in signature and explicit token direction check | Low |

Evidence: `crates/icydb-core/src/db/query/plan/executable.rs:209`, `crates/icydb-core/src/db/query/plan/executable.rs:214`, `crates/icydb-core/src/db/query/plan/executable.rs:219`, `crates/icydb-core/src/db/query/plan/executable.rs:249`, `crates/icydb-core/src/db/query/plan/continuation.rs:399`, `crates/icydb-core/src/db/query/plan/hash_parts.rs:360`, `crates/icydb-core/src/db/query/plan/hash_parts.rs:414`.

## 6. Overall Risk Assessment

Critical issues:
- None found in current cursor/order invariants.

Medium-risk drift:
- Continuation signature is the primary barrier against predicate/access/order mutation; any future profile drift in `ContinuationV1` fields is safety-sensitive.
- Boundary/anchor consistency is enforced by primary-key equivalence, not full slot equivalence, so token tampering space is reduced but not fully canonicalized at tuple level.

Low-risk observations:
- Structural checks for index id, namespace, arity, and envelope are explicit and fail closed.
- Resume semantics are centralized and strict (`Excluded`) for continuation.
- Existing cursor failure classifications are consistently typed and covered by tests.

Areas requiring additional tests:
- Explicit adversarial test for wrong namespace anchor (`key_kind != User`) at `plan_cursor` boundary.
- Explicit adversarial test for predicate-only signature mismatch (separate from order/entity mismatch).
- Explicit test for direction-mismatch payload rejection path in `decode_validated_cursor`.

Overall Cursor/Ordering Risk Index (1–10, lower is better): **3/10**

Interpretation:
1–3  = Low risk / structurally healthy
4–6  = Moderate risk / manageable pressure
7–8  = High risk / requires monitoring
9–10 = Critical risk / structural instability

Targeted verification executed during this run:
- `cargo test -p icydb-core cursor_validation -- --nocapture`
- `cargo test -p icydb-core plan_cursor_rejects_index_range_boundary_anchor_mismatch -- --nocapture`
- `cargo test -p icydb-core load_composite_range_cursor_pagination_matches_unbounded_and_anchor_is_strictly_monotonic -- --nocapture`
- `cargo test -p icydb-core load_unique_index_range_cursor_pagination_matches_unbounded_case_f -- --nocapture`
