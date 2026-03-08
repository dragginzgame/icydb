# DB Type Audit Report (2026-03-08)

Scope: `crates/icydb-core/src/db/**` (non-test top-level type declarations)

## 1. Type inventory summary

- Total types: **521**
- By kind:
  - `struct`: 283
  - `enum`: 194
  - `type`: 22
  - `trait`: 22
- By visibility:
  - `pub`: 114
  - `pub(crate)`: 105
  - `pub(in crate::db::executor)`: 87
  - `pub(in crate::db)`: 80
  - `private`: 90
  - other scoped visibilities: 45
- By primary layer ownership:
  - `executor`: 181
  - `query/plan`: 148
  - `other`: 116
  - `index`: 30
  - `cursor`: 24
  - `predicate`: 21
  - `codec`: 1
- Cross-layer types (referenced in >1 layer): **177**
- Types with 0 references outside defining module file: **138**
- Types with exactly 1 reference outside defining module file: **48**
- Proven dead top-level types (single token occurrence across `db/`): **none**

Full inventory (every type, with path/module/visibility/layer/reference counts):
- `/tmp/db_type_inventory_layers.tsv`

Reference subsets:
- one-reference types: `/tmp/db_types_one_ref.tsv`
- zero-reference-outside-module types: `/tmp/db_types_zero_ref.tsv`

## 2. Visibility issues

### Fixed in this audit

- Tightened index-plan internals from `pub(crate)` to `pub(in crate::db)`:
  - `IndexApplyPlan` (`db/index/plan/mod.rs`)
  - `IndexMutationPlan` (`db/index/plan/mod.rs`)
- Tightened executor-internal kernel contracts to executor scope:
  - `PlanRow` (`db/executor/kernel/post_access/mod.rs`)
  - `PostAccessStats` and its fields (`db/executor/kernel/post_access/mod.rs`)
  - `ExecutionKernel::apply_post_access_*` helper visibilities (`db/executor/kernel/post_access/mod.rs`)
- Tightened storage-local fingerprint type to private:
  - `RawIndexFingerprint` (`db/index/store.rs`)

### Remaining candidates (not applied)

Executor-only `pub(crate)` items with zero non-executor references (candidate for `pub(in crate::db::executor)`):
- `BudgetSafetyMetadata`
- `CursorPage`
- `ExecutorError`
- `SaveMode`
- `MergeOrderedKeyStream`
- `IntersectOrderedKeyStream`
- `OrderedKeyStream`
- `OrderedKeyStreamBox`
- `VecOrderedKeyStream`
- `BudgetedOrderedKeyStream`
- `KeyOrderComparator`

These were left unchanged where they appear in broader signatures/re-export boundaries (to avoid accidental interface breakage).

## 3. Redundant or consolidatable types

Candidates (recommendation only; no behavior changes applied):

- `ScalarAccessWindowPlan` (planner) and `AccessWindow` (route/runtime) both model offset/limit/fetch window contracts with highly overlapping fields and transforms.
- `GroupedContinuationWindow` (planner) and `GroupedPaginationWindow` (runtime) are near 1:1 field mirrors with `from_contract(...)` copying all fields.
- `RouteContinuationPlan` stores paired `AccessWindow` instances (`keep` and `fetch`) that may be representable as one window + explicit lookahead policy.

## 4. Stale or incorrect comments

- Searched for stale concepts: `keep_count`, `fetch_count`, old continuation wording, old execution-path wording.
- No stale references to removed semantics were found.
- One redundant comment line was removed in `db/executor/window.rs` to avoid duplicate wording around `accept_existing_row`.

## 5. Naming inconsistencies

Inconsistencies observed:

- Window-size naming across layers mixes `limit`, `page_limit`, `fetch_limit`, `keep_count`, and `fetch_count`.
- Same concept appears with different labels in planner and route/runtime DTOs (`limit` vs `page_limit`).
- Reused type names across layers (`FieldSlot`, `ExecutionMode`, `ExecutionError`) increase cognitive ambiguity during boundary reviews.

No field renames were applied in this pass.

## 6. Enum simplification candidates

Candidates (recommendation only):

- `GroupedRouteRejectionReason` currently has a single variant (`CapabilityMismatch`), suggesting either expansion is pending or it can be folded into outcome metadata until more reasons exist.
- Duplicate high-level enum names across layers (`ExecutionMode`, `ExecutionError`) suggest namespace-level simplification/renaming for clearer ownership.

No enum shape changes were applied.

## 7. Safe cleanups (no behavior change)

Applied:

- Visibility tightening only (no runtime logic changes).
- One local doc-comment improvement on `RawIndexFingerprint` ownership boundary.
- One redundant comment cleanup in executor window helper.

Validation:

- `cargo fmt --all`: passed
- `cargo check -p icydb-core`: passed
- `cargo test -p icydb-core`: failed in existing `compile_fail` harness (`tests/compile_fail.rs`) with `E0599` in `db/executor/load/entrypoints/scalar.rs` about `RouteContinuationPlan::fetch_count_for` (outside this patch scope; file already had concurrent changes)
