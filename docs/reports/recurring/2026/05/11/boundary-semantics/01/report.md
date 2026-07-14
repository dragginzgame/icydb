# Boundary / Envelope Semantics Audit - 2026-05-11

## Run Metadata + Comparability Note

- scope: range boundary envelope semantics and continuation resume correctness
- recurring definition: `docs/audits/recurring/range/boundary-envelope-semantics.md`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-12/boundary-semantics.md`
- code snapshot identifier: `082b7d142`
- method tag/version: `Method V4`
- comparability status: `non-comparable` - the recurring definition now requires the expanded proof-table format, unbounded attack matrix, and drift analysis that the `2026-03-12` report did not include.

## Invariant Registry

| Invariant | Enforced Where | Structural or Implicit? |
| --- | --- | --- |
| Resume rewrites only the direction-owned edge and excludes the anchor | `crates/icydb-core/src/db/index/envelope/mod.rs::resume_bounds_from_refs` | Structural |
| Anchor must remain inside the original range envelope | `validate_index_scan_continuation_envelope`, `cursor::anchor::validate_anchor_in_envelope` | Runtime guard plus validated anchor type |
| Inclusive/exclusive semantics stay centralized | `KeyEnvelope::contains`, `envelope_is_empty` | Structural |
| Raw-key ordering is the canonical traversal order | `IndexStore::visit_raw_entries_in_range`, `RawIndexKey` BTree range traversal | Structural |
| Cursor anchor identity cannot change index id, key namespace, or arity | `cursor::anchor::validate_anchor_identity` | Runtime guard |
| Boundary slot and raw anchor must identify the same row position | `validate_index_range_boundary_anchor_consistency` | Runtime guard |
| Empty envelopes must not scan storage | `IndexStore::visit_raw_entries_in_range` checks `envelope_is_empty` before `BTreeMap::range` | Structural guard |

## Bound Transformation Proof Table

| Location | Transformation | Invariant Preserved | Enforcement Type | Risk |
| --- | --- | --- | --- | --- |
| `resume_bounds_from_refs` | ASC continuation becomes `(Excluded(anchor), upper)` | resume monotonicity, upper immutability | Structural | Low |
| `resume_bounds_from_refs` | DESC continuation becomes `(lower, Excluded(anchor))` | resume monotonicity, lower immutability | Structural | Low |
| `resume_bounds_for_continuation` | missing anchor preserves original bounds | initial scan semantics | Structural | Low |
| `validate_index_scan_continuation_envelope` | rejects out-of-envelope anchors before rewrite | envelope containment | Runtime guard | Low |
| `validate_index_scan_continuation_advancement` | rejects candidate equal to or behind anchor | duplicate prevention | Runtime guard | Low |
| `envelope_is_empty` | equal bounds are empty unless both edges are included | empty-envelope correctness | Structural | Low |
| `visit_raw_entries_in_range` | empty envelope returns before store traversal | no-scan empty continuation | Structural | Low |

## Envelope Attack Matrix

| Scenario | Lower=None? | Upper=None? | Structural Prevention? | Runtime Guard? | Test Only? | Risk |
| --- | --- | --- | --- | --- | --- | --- |
| Anchor == lower, lower included | No | Maybe | ASC rewrites to excluded anchor | envelope containment | No | Low |
| Anchor == lower, lower excluded | No | Maybe | anchor is outside envelope | containment rejection | No | Low |
| Anchor == upper, upper included | Maybe | No | ASC rewrite yields empty exclusive/included envelope | empty-envelope no-scan | No | Low |
| Anchor == upper, upper excluded | Maybe | No | anchor is outside envelope | containment rejection | No | Low |
| Anchor below lower | No | Maybe | none before validation | containment rejection | No | Low |
| Anchor above upper | Maybe | No | none before validation | containment rejection | No | Low |
| Empty range | No | No | `envelope_is_empty` short-circuits raw scan | access validation rejects inverted semantic ranges | No | Low |
| Single-element range | No | No | included/included is non-empty; exclusive edge collapses empty | envelope emptiness helper | No | Low |
| Unbounded lower, bounded upper | Yes | No | unbounded lower accepted; upper remains immutable | containment for bounded side | No | Low |
| Bounded lower, unbounded upper | No | Yes | unbounded upper accepted; lower remains immutable | containment for bounded side | No | Low |
| Continuation produces empty envelope | No | No | raw scan returns before traversal | covered by regression tests | No | Low |
| Composite or mutated access path | N/A | N/A | anchor validation rejects anchors without an index-range payload | cursor plan guard | No | Low |

## Upper Bound Immutability

| Code Path | Upper Modified? | Proven Immutable? | Risk |
| --- | --- | --- | --- |
| ASC `resume_bounds_from_refs` | No, clones `upper` unchanged | Yes | Low |
| DESC `resume_bounds_from_refs` | Yes, replaces upper with `Excluded(anchor)` by design | Yes, lower remains unchanged instead | Low |
| `resume_bounds_for_continuation(None, ...)` | No | Yes | Low |
| raw scan traversal | No | Yes, consumes already-derived bounds | Low |

## Ordering Alignment

| Layer | Ordering Source | Divergence Possible? | Risk |
| --- | --- | --- | --- |
| Raw index scan | `BTreeMap::range` over `RawIndexKey` | Low; one traversal primitive | Low |
| Envelope containment | `Ord` through `KeyEnvelope` | Low; same key domain | Low |
| Continuation advancement | directional strict `Ord` comparison | Low; property test covers ASC/DESC | Low |
| Composite value encoding | canonical encoded key construction | Medium if new value encodings are added without property coverage | Moderate |

## Logical -> Raw Bound Mapping Table

| Logical Operator | Raw Lower Bound | Raw Upper Bound | Enforced Where | Drift Risk |
| --- | --- | --- | --- | --- |
| `>` | `Excluded(v)` | N/A | planner range lowering and `KeyEnvelope` semantics | Low |
| `>=` | `Included(v)` | N/A | planner range lowering and `KeyEnvelope` semantics | Low |
| `<` | N/A | `Excluded(v)` | planner range lowering and `KeyEnvelope` semantics | Low |
| `<=` | N/A | `Included(v)` | planner range lowering and `KeyEnvelope` semantics | Low |

## Anchor / Boundary Consistency

| Issue | Structural? | Guarded? | Drift-Sensitive? | Risk Level |
| --- | --- | --- | --- | --- |
| Forged raw anchor with wrong index id | No | Yes, `validate_anchor_identity` | Low | Low |
| Wrong key namespace | No | Yes, `validate_anchor_identity` | Low | Low |
| Wrong component arity | No | Yes, `validate_anchor_identity` | Low | Low |
| Boundary primary key does not match raw anchor PK | No | Yes, `validate_index_range_boundary_anchor_consistency` | Medium | Low |
| Non-canonical raw anchor bytes | No | Yes, decode/re-encode equality check | Low | Low |

## Composite + Cursor / Plan Binding Containment

| Property | Mutable? | Prevention Mechanism | Risk |
| --- | --- | --- | --- |
| Cursor changes index id | No after validation | signature check plus anchor index-id check | Low |
| Cursor changes access path variant | No after validation | continuation signature and access payload matching | Low |
| Cursor introduces anchor for non-range plan | No | `unexpected_index_range_anchor_for_non_range_path` | Low |
| Cursor introduces anchor for composite plan without range payload | No | `unexpected_index_range_anchor_for_composite_plan` | Low |
| Cursor changes order direction | No | cursor direction validation | Low |
| Cursor changes page initial offset | No | cursor window validation | Low |

## Resume Monotonicity Proof

| Property | Mechanism | Structural? | Risk |
| --- | --- | --- | --- |
| ASC resume lower bound strictly increases | lower becomes `Excluded(anchor)` | Yes | Low |
| DESC resume upper bound strictly decreases | upper becomes `Excluded(anchor)` | Yes | Low |
| Anchor cannot reappear in resumed scan | exclusive bound plus strict advancement guard | Yes plus runtime guard | Low |
| Equal-bound collapse is deterministic | `envelope_is_empty` treats equal exclusive edge as empty | Yes | Low |

## Duplication / Omission Proof

| Mechanism | Duplication Possible? | Omission Possible? | Risk |
| --- | --- | --- | --- |
| Store range traversal with exclusive resume bound | No for anchor row | No beyond intentional anchor exclusion | Low |
| Cursor raw anchor emitted from last index-range row | Low | Low | Low |
| Post-access pagination boundary comparison | Low | Low | Low |
| Grouped resume boundary filtering | Low, but separate grouped domain | Low | Moderate if grouped ordering expands |

## Canonical Envelope Definition

The continuation envelope is:

`effective_envelope = (lower', upper)` for ASC scans, where `lower' = Bound::Excluded(anchor)`.

For DESC scans, the direction-owned edge is symmetric:

`effective_envelope = (lower, upper')`, where `upper' = Bound::Excluded(anchor)`.

Required definition coverage:

| Definition Element | Stated? | Verified In Code? | Risk |
| --- | --- | --- | --- |
| ASC `lower' = Excluded(anchor)` | Yes | Yes | Low |
| DESC `upper' = Excluded(anchor)` | Yes | Yes | Low |
| Upper immutable for ASC | Yes | Yes | Low |
| Lower immutable for DESC | Yes | Yes | Low |
| Empty envelope short-circuits scan | Yes | Yes | Low |

## Drift Sensitivity

| Drift Vector | Impacted Invariant | Risk |
| --- | --- | --- |
| Adding new `Value` encodings without raw-order property coverage | raw/logical ordering alignment | Moderate |
| Expanding DESC index-range pushdown paths | direction-owned resume edge | Moderate |
| Adding composite continuation forms outside `IndexRange` | plan binding containment | Moderate |
| Moving cursor anchor derivation away from last emitted raw key | duplication/omission guarantee | Moderate |
| Bypassing `AccessPlan::validate_runtime_invariants` for runtime plans | inverted range fail-closed behavior | Moderate |

## Overall Envelope Risk Index

**3/10**

The boundary remains structurally healthy. The main residual risk is future drift from new value encodings, DESC/composite expansion, or bypassing the accepted runtime validation path.

## Verification Readout

- `cargo test -p icydb-core anchor_equal_to_upper_resumes_to_empty_envelope --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core desc_anchor_equal_to_lower_resumes_to_empty_envelope --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core cross_layer_canonical_ordering_is_consistent --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core load_composite_range_cursor_pagination_matches_unbounded_and_anchor_is_strictly_monotonic --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core load_cursor_pagination_pk_order_inverted_key_range_returns_empty_without_scan --features sql -- --nocapture` -> PASS

## Follow-Up Actions

- No immediate follow-up required for this run.
- Next comparable boundary run should keep `Method V4` and track whether new schema mutation work adds any runtime path that bypasses `AccessPlan::validate_runtime_invariants`.
