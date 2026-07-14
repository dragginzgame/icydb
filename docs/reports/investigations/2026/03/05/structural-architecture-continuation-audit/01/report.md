# Structural Architecture Audit - Continuation Spread (2026-03-05)

Scope: `crates/icydb-core/src/db` non-test runtime modules.

## STEP 1 - Cross-Cutting Continuation Surface

Search terms:
`continuation`, `cursor`, `anchor`, `ExecutionShapeSignature`, `decode_cursor`, `validate_cursor`, `cursor_policy`, `anchor_validation`.

Raw matches: `1568` lines across `98` runtime files.

Category summary:
- `A semantic authority`: `7` files
- `B execution runtime`: `47` files
- `C structural plumbing`: `32` files
- `D accidental propagation`: `12` files

Layer summary:
- `intent`: 1
- `query/plan`: 24
- `access`: 4
- `executor` (includes `db/cursor` runtime): 60
- `index/storage`: 7
- `codec`: 2

### File-Level Classification Table

| file | layer | usage category | semantic or plumbing | hit count |
| --- | --- | --- | --- | --- |
| `crates/icydb-core/src/db/query/plan/continuation.rs` | query/plan | A semantic authority | semantic | 90 |
| `crates/icydb-core/src/db/index/envelope.rs` | index/storage | B execution runtime | semantic | 74 |
| `crates/icydb-core/src/db/cursor/anchor.rs` | executor | B execution runtime | semantic | 71 |
| `crates/icydb-core/src/db/executor/load/entrypoints.rs` | executor | D accidental propagation | semantic | 70 |
| `crates/icydb-core/src/db/cursor/mod.rs` | executor | B execution runtime | semantic | 70 |
| `crates/icydb-core/src/db/executor/executable_plan.rs` | executor | C structural plumbing | plumbing | 66 |
| `crates/icydb-core/src/db/cursor/spine.rs` | executor | B execution runtime | semantic | 64 |
| `crates/icydb-core/src/db/session.rs` | executor | D accidental propagation | semantic | 59 |
| `crates/icydb-core/src/db/index/scan.rs` | index/storage | B execution runtime | semantic | 59 |
| `crates/icydb-core/src/db/executor/stream/access/mod.rs` | executor | D accidental propagation | semantic | 58 |
| `crates/icydb-core/src/db/executor/continuation/mod.rs` | executor | B execution runtime | semantic | 55 |
| `crates/icydb-core/src/db/executor/load/mod.rs` | executor | D accidental propagation | semantic | 47 |
| `crates/icydb-core/src/db/cursor/error.rs` | executor | B execution runtime | semantic | 47 |
| `crates/icydb-core/src/db/cursor/boundary.rs` | executor | B execution runtime | semantic | 38 |
| `crates/icydb-core/src/db/executor/kernel/post_access/mod.rs` | executor | B execution runtime | semantic | 37 |
| `crates/icydb-core/src/db/cursor/token/scalar.rs` | executor | B execution runtime | semantic | 34 |
| `crates/icydb-core/src/db/codec/cursor.rs` | codec | B execution runtime | semantic | 31 |
| `crates/icydb-core/src/db/cursor/continuation.rs` | executor | B execution runtime | semantic | 29 |
| `crates/icydb-core/src/db/query/plan/model.rs` | query/plan | A semantic authority | semantic | 28 |
| `crates/icydb-core/src/db/executor/window.rs` | executor | D accidental propagation | semantic | 28 |
| `crates/icydb-core/src/db/cursor/token/wire.rs` | executor | B execution runtime | semantic | 28 |
| `crates/icydb-core/src/db/cursor/grouped_validate.rs` | executor | B execution runtime | semantic | 27 |
| `crates/icydb-core/src/db/cursor/token/grouped.rs` | executor | B execution runtime | semantic | 20 |
| `crates/icydb-core/src/db/response/paged.rs` | executor | C structural plumbing | plumbing | 19 |
| `crates/icydb-core/src/db/response/grouped.rs` | executor | C structural plumbing | plumbing | 19 |
| `crates/icydb-core/src/db/executor/route/planner/feasibility.rs` | executor | D accidental propagation | semantic | 19 |
| `crates/icydb-core/src/db/executor/load/page.rs` | executor | B execution runtime | semantic | 18 |
| `crates/icydb-core/src/db/cursor/validation.rs` | executor | B execution runtime | semantic | 17 |
| `crates/icydb-core/src/db/executor/route/planner/mod.rs` | executor | D accidental propagation | semantic | 16 |
| `crates/icydb-core/src/db/cursor/range_token.rs` | executor | B execution runtime | semantic | 16 |
| `crates/icydb-core/src/db/query/fluent/load/pagination.rs` | query/plan | C structural plumbing | plumbing | 15 |
| `crates/icydb-core/src/db/executor/route/mode.rs` | executor | D accidental propagation | semantic | 14 |
| `crates/icydb-core/src/db/query/fingerprint/shape_signature/mod.rs` | query/plan | C structural plumbing | plumbing | 13 |
| `crates/icydb-core/src/db/query/plan/mod.rs` | query/plan | C structural plumbing | plumbing | 12 |
| `crates/icydb-core/src/db/executor/load/grouped_route.rs` | executor | B execution runtime | semantic | 12 |
| `crates/icydb-core/src/db/executor/stream/access/physical.rs` | executor | D accidental propagation | semantic | 11 |
| `crates/icydb-core/src/db/cursor/planned.rs` | executor | B execution runtime | semantic | 11 |
| `crates/icydb-core/src/db/cursor/order.rs` | executor | B execution runtime | semantic | 11 |
| `crates/icydb-core/src/db/executor/kernel/mod.rs` | executor | B execution runtime | semantic | 10 |
| `crates/icydb-core/src/db/query/plan/validate/mod.rs` | query/plan | C structural plumbing | plumbing | 9 |
| `crates/icydb-core/src/db/query/plan/semantics/group_having.rs` | query/plan | A semantic authority | semantic | 9 |
| `crates/icydb-core/src/db/executor/route/contracts.rs` | executor | D accidental propagation | semantic | 9 |
| `crates/icydb-core/src/db/executor/kernel/reducer.rs` | executor | B execution runtime | semantic | 9 |
| `crates/icydb-core/src/db/executor/kernel/post_access/order_cursor.rs` | executor | B execution runtime | semantic | 9 |
| `crates/icydb-core/src/db/query/plan/semantics/logical.rs` | query/plan | C structural plumbing | plumbing | 8 |
| `crates/icydb-core/src/db/executor/traversal.rs` | executor | B execution runtime | semantic | 8 |
| `crates/icydb-core/src/db/query/plan/order_contract.rs` | query/plan | A semantic authority | semantic | 7 |
| `crates/icydb-core/src/db/query/fluent/load/builder.rs` | query/plan | C structural plumbing | plumbing | 7 |
| `crates/icydb-core/src/db/index/pk_equivalence.rs` | index/storage | B execution runtime | semantic | 6 |
| `crates/icydb-core/src/db/executor/route/hints.rs` | executor | D accidental propagation | semantic | 6 |
| `crates/icydb-core/src/db/executor/mod.rs` | executor | B execution runtime | semantic | 6 |
| `crates/icydb-core/src/db/cursor/token/error.rs` | executor | B execution runtime | semantic | 6 |
| `crates/icydb-core/src/db/query/intent/errors.rs` | intent | C structural plumbing | plumbing | 5 |
| `crates/icydb-core/src/db/query/fingerprint/hash_parts.rs` | query/plan | C structural plumbing | plumbing | 5 |
| `crates/icydb-core/src/db/executor/stream/access/scan.rs` | executor | D accidental propagation | semantic | 5 |
| `crates/icydb-core/src/db/access/lowering.rs` | access | C structural plumbing | plumbing | 5 |
| `crates/icydb-core/src/db/query/policy.rs` | query/plan | A semantic authority | semantic | 4 |
| `crates/icydb-core/src/db/query/plan/validate/fluent_policy.rs` | query/plan | C structural plumbing | plumbing | 4 |
| `crates/icydb-core/src/db/query/plan/validate/cursor_policy.rs` | query/plan | A semantic authority | semantic | 4 |
| `crates/icydb-core/src/db/query/fingerprint/fingerprint.rs` | query/plan | C structural plumbing | plumbing | 4 |
| `crates/icydb-core/src/db/executor/load/index_range_limit.rs` | executor | B execution runtime | semantic | 4 |
| `crates/icydb-core/src/db/executor/load/grouped_fold/page_finalize.rs` | executor | B execution runtime | semantic | 4 |
| `crates/icydb-core/src/db/executor/aggregate/distinct.rs` | executor | B execution runtime | semantic | 4 |
| `crates/icydb-core/src/db/query/plan/validate/grouped/cursor.rs` | query/plan | A semantic authority | semantic | 3 |
| `crates/icydb-core/src/db/query/fluent/load/validation.rs` | query/plan | C structural plumbing | plumbing | 3 |
| `crates/icydb-core/src/db/executor/load/grouped_fold/mod.rs` | executor | B execution runtime | semantic | 3 |
| `crates/icydb-core/src/db/diagnostics/execution_trace.rs` | executor | C structural plumbing | plumbing | 3 |
| `crates/icydb-core/src/db/cursor/token/mod.rs` | executor | B execution runtime | semantic | 3 |
| `crates/icydb-core/src/db/cursor/signature.rs` | executor | B execution runtime | semantic | 3 |
| `crates/icydb-core/src/db/contracts/semantics.rs` | executor | C structural plumbing | plumbing | 3 |
| `crates/icydb-core/src/db/query/plan/validate/grouped/mod.rs` | query/plan | C structural plumbing | plumbing | 2 |
| `crates/icydb-core/src/db/query/plan/validate/core.rs` | query/plan | C structural plumbing | plumbing | 2 |
| `crates/icydb-core/src/db/mod.rs` | executor | C structural plumbing | plumbing | 2 |
| `crates/icydb-core/src/db/index/range.rs` | index/storage | B execution runtime | semantic | 2 |
| `crates/icydb-core/src/db/index/mod.rs` | index/storage | B execution runtime | semantic | 2 |
| `crates/icydb-core/src/db/executor/load/execute.rs` | executor | B execution runtime | semantic | 2 |
| `crates/icydb-core/src/db/executor/access_dispatcher.rs` | executor | B execution runtime | semantic | 2 |
| `crates/icydb-core/src/db/codec/mod.rs` | codec | C structural plumbing | plumbing | 2 |
| `crates/icydb-core/src/db/access/execution_contract.rs` | access | C structural plumbing | plumbing | 2 |
| `crates/icydb-core/src/db/response/mod.rs` | executor | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/query/plan/validate/grouped/structure.rs` | query/plan | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/query/plan/semantics/mod.rs` | query/plan | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/query/plan/expr/mod.rs` | query/plan | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/query/fingerprint/projection_hash.rs` | query/plan | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/query/explain/mod.rs` | query/plan | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/predicate/fingerprint.rs` | executor | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/predicate/coercion.rs` | executor | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/index/store.rs` | index/storage | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/index/key/codec/tuple.rs` | index/storage | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/executor/load/secondary_index.rs` | executor | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/executor/load/grouped_fold/global_distinct.rs` | executor | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/executor/load/grouped_fold/candidate_rows.rs` | executor | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/executor/load/fast_stream.rs` | executor | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/executor/delete/mod.rs` | executor | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/executor/aggregate/mod.rs` | executor | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/executor/aggregate/field_extrema.rs` | executor | B execution runtime | semantic | 1 |
| `crates/icydb-core/src/db/access/mod.rs` | access | C structural plumbing | plumbing | 1 |
| `crates/icydb-core/src/db/access/canonical.rs` | access | C structural plumbing | plumbing | 1 |

## STEP 2 - Cross-Cutting Spread Metrics

- Total `continuation` mentions: **620**
- Files containing `continuation`: **71**
- Files containing continuation/cursor/anchor semantic terms (full pattern set): **98**
- Files containing cursor decode/validation logic: **22**

Files with highest continuation/cursor/anchor pressure **outside** planner authority (`query/plan`) and cursor runtime authority (`db/cursor`, `db/executor/continuation`):

| file | hit count |
| --- | --- |
| `crates/icydb-core/src/db/index/envelope.rs` | 74 |
| `crates/icydb-core/src/db/executor/load/entrypoints.rs` | 70 |
| `crates/icydb-core/src/db/executor/executable_plan.rs` | 66 |
| `crates/icydb-core/src/db/index/scan.rs` | 59 |
| `crates/icydb-core/src/db/session.rs` | 59 |
| `crates/icydb-core/src/db/executor/stream/access/mod.rs` | 58 |
| `crates/icydb-core/src/db/executor/load/mod.rs` | 47 |
| `crates/icydb-core/src/db/executor/kernel/post_access/mod.rs` | 37 |
| `crates/icydb-core/src/db/codec/cursor.rs` | 31 |
| `crates/icydb-core/src/db/executor/window.rs` | 28 |
| `crates/icydb-core/src/db/executor/route/planner/feasibility.rs` | 19 |
| `crates/icydb-core/src/db/response/grouped.rs` | 19 |
| `crates/icydb-core/src/db/response/paged.rs` | 19 |
| `crates/icydb-core/src/db/executor/load/page.rs` | 18 |
| `crates/icydb-core/src/db/executor/route/planner/mod.rs` | 16 |
| `crates/icydb-core/src/db/query/fluent/load/pagination.rs` | 15 |
| `crates/icydb-core/src/db/executor/route/mode.rs` | 14 |
| `crates/icydb-core/src/db/query/fingerprint/shape_signature/mod.rs` | 13 |
| `crates/icydb-core/src/db/executor/load/grouped_route.rs` | 12 |
| `crates/icydb-core/src/db/executor/stream/access/physical.rs` | 11 |

Interpretation:
- Spread is still broad across executor/index/session modules.
- The refactor reduced route-local branching, but continuation vocabulary remains structurally cross-cutting.

## STEP 3 - Execution Hubs (>600 LOC)

| module | LOC | cursor decode | continuation/anchor checks | paging logic | continuation distribution hub |
| --- | ---: | --- | --- | --- | --- |
| `executor/load/mod.rs` | 864 | yes (`decode/validate` refs) | yes | yes | **YES** |
| `executor/load/entrypoints.rs` | 667 | yes | yes | yes | **YES** |
| `session.rs` | 659 | yes (`decode_cursor`) | yes | yes | **YES** |
| `executor/stream/access/mod.rs` | 658 | no | yes (anchor/continuation routing) | minimal | **YES** |
| `access/execution_contract.rs` | 732 | no | minor continuation contract refs | minimal | no (policy contract hub) |
| `executor/aggregate/contracts/grouped.rs` | 687 | no | no | grouped paging contracts only | no |
| `query/plan/expr/type_inference.rs` | 670 | no | no | no | no |
| `query/fingerprint/fingerprint.rs` | 654 | no | signature/plumbing refs | no | no |
| `query/explain/mod.rs` | 640 | no | no | no | no |
| `query/fingerprint/hash_parts.rs` | 633 | no | signature/plumbing refs | no | no |
| `data/storage_key.rs` | 613 | no | no | no | no |

## STEP 4 - Incomplete Authority Collapse Signals

Searched for: `decode_cursor(`, `validate_cursor(`, `anchor_validation(`, `cursor_position(`, `ExecutionShapeSignature`.

Findings:
- `decode_cursor(`
  - Defined in `codec/cursor.rs`
  - Called directly in `session.rs` (outside cursor runtime authority)
- `validate_cursor(` exact symbol: not present as a distributed helper.
- `anchor_validation(` exact symbol: not present.
- `cursor_position(` exact symbol: not present.
- `ExecutionShapeSignature` appears in both:
  - planner authority (`query/plan/model.rs`, `query/plan/continuation.rs`, logical semantics)
  - executor bridge (`executor/executable_plan.rs`)

This indicates partial collapse: authority is centralized for policy, but signature/decode usage still leaks into outer runtime boundaries.

## STEP 5 - Ranked Complexity Drivers (Mention Count)

Counts from runtime non-test modules:

1. `continuation|cursor|anchor` -> **1556**
2. `PlanError|validate_` -> **683**
3. `LoadExecutor|executor/load|load_scan|load_` -> **218**
4. `type_inference|ExpressionType|AggregateExpr|ProjectionExpr` -> **114**
5. `AccessPath::|AccessPath<` -> **94**

Dominant driver remains continuation/cursor/anchor spread by a large margin.

## STEP 6 - Refactor Leverage Points (Top Driver: Continuation)

### Modules implementing continuation semantics (keep as authorities)
- `query/plan/continuation.rs`
- `query/plan/validate/cursor_policy.rs`
- `cursor/*`
- `executor/continuation/mod.rs`
- `index/envelope.rs` + `index/scan.rs` (runtime scan-boundary semantics)

### Modules mostly transporting continuation data (candidate plumbing-only)
- `executor/executable_plan.rs`
- `response/paged.rs`, `response/grouped.rs`
- `query/fingerprint/*` (signature/fingerprint transport)

### Modules still interpreting continuation semantics outside intended authority
- `session.rs` (direct `decode_cursor` call)
- `executor/load/entrypoints.rs` (cursor/continuation orchestration logic still partially local)
- `executor/stream/access/mod.rs` (anchor/continuation interpretation at stream layer)
- `executor/route/*` (now reduced, but still consumes continuation policy gates in multiple files)

## STEP 7 - Actionable Conclusion

### 1) What changed structurally in this refactor
- Continuation policy gates were collapsed under `RouteContinuationPlan` methods.
- Free gate wrappers were removed.
- Router branch pressure decreased (`executor/route if: 82 -> 56`).
- Several continuation DTO boundaries were introduced (`AccessScanContinuationInput`, `IndexScanContinuationInput`, grouped/runtime contexts).

### 2) Why complexity score did not decrease
- The highest-weight cross-cutting metric (continuation/cursor/anchor spread) remains high:
  - `1556` mentions
  - `98` files touched by continuation-related vocabulary
  - `71` files with direct `continuation` references
- Complexity reduction in one hotspot (route branching) was offset by broad continuation vocabulary propagation across executor/index/session/fingerprint/response.

### 3) Continuation propagation hubs
- `executor/load/mod.rs`
- `executor/load/entrypoints.rs`
- `session.rs`
- `executor/stream/access/mod.rs`

### 4) Minimal architectural change likely to reduce continuation spread
Introduce one **opaque runtime continuation context** (single public type) returned by cursor/runtime authority and consumed by load/stream/index/session boundaries without exposing token/signature/anchor semantics.

Concretely:
- Move raw token decode calls (`decode_cursor`) behind cursor runtime authority API (remove direct session usage).
- Keep `ExecutionShapeSignature` planner-owned and hand executor an opaque compatibility handle, not the signature struct.
- Restrict continuation semantics methods to one runtime facade module; other modules consume only pre-resolved booleans/bounds.

### 5) Expected impact (smallest high-leverage slice)
- Continuation/cursor/anchor mentions: **~15-20% reduction** (`1556` -> ~`1240-1320`)
- Files touching continuation terms: **~12-18 file reduction** (`98` -> ~`80-86`)
- Complexity index: **6/10 -> ~5-5.5/10** (assuming no new cross-layer continuation terms are introduced)
