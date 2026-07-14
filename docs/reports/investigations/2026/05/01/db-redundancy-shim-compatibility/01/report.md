# DB Redundancy, Shim, and Backwards-Compatibility Audit - 2026-05-01

## Report Preamble

- scope: `crates/icydb-core/src/db/`
- code snapshot identifier: `c88e171f43` (`dirty` working tree)
- audit focus:
  - redundant production code
  - transitional shims / bridge modules
  - backwards-compatibility or multi-version support paths
  - test-only legacy helpers that can hide architectural debt
- non-goal: this report does not classify normal route fallback behavior as
  backwards compatibility

## Executive Summary

The production `db/` tree does **not** show a major backwards-compatibility
problem. Persisted row, commit marker, and commit control-slot formats are
versioned but fail closed against unsupported versions. That matches the
current hard-cut policy better than decode fallback support would.

The real redundancy/shim pressure is architectural:

- grouped aggregate expression access still has a compatibility re-export
  module
- structural codecs have short alias namespaces while the original
  `structural_field` functions remain the real owner
- predicate/expression canonicalization still carries legacy plain-`Expr` and
  runtime-`Predicate` bridge wrappers
- `db::scalar_expr` remains a separate scalar micro-engine for index and
  predicate transforms, overlapping a narrow subset of the compiled expression
  function surface
- some test support still names and preserves old helper contracts, especially
  session SQL helpers

No finding below requires an emergency fix before normal development resumes.
The best cleanup order is to remove the true shims first, then tighten stage
artifacts and test support once grouped/expression consolidation settles.

## Method

Commands and searches used:

- `find crates/icydb-core/src/db -type f | sort`
- `find crates/icydb-core/src/db -type f -name '*.rs' | wc -l`
- `find crates/icydb-core/src/db -name '*.rs' -print0 | xargs -0 wc -l | sort -nr | head -60`
- `rg -n -i "\b(shim|compat|compatibility|legacy|deprecated|backward|backwards|fallback|transitional|temporary|TODO|FIXME|remove after|old|obsolete|adapter|bridge|re-export|reexport)\b" crates/icydb-core/src/db`
- `rg -n -i "compatibility re-export|legacy|deprecated|obsolete|old wrapper|old shared|old executor|old entity-row|old grouped|test-only fallback|compatibility lane|compatibility contract|old-version|versioned|format_version|schema_version|migration|decode fallback|fallback decode|v1|v2" crates/icydb-core/src/db --glob '!**/tests/**' --glob '!**/tests.rs'`
- `rg -n "pub\(in crate::db[^)]*\) use crate::db|pub\(crate\) use crate::db|pub use crate::db" crates/icydb-core/src/db`
- duplicate-name scans over non-test functions, structs, enums, and traits
- targeted reads of the files listed in each finding

Scale:

- Rust files under `db/`: 834
- Rust lines under `db/`: about 276k, including tests

## Findings

### 1. True Shim: Grouped Expression Compatibility Re-Export

Priority: `P1 cleanup`

Cleanup status: `completed in the follow-up cleanup pass`

Files:

- `crates/icydb-core/src/db/executor/aggregate/contracts/state/grouped_expr.rs`
- `crates/icydb-core/src/db/executor/aggregate/contracts/state/mod.rs`
- `crates/icydb-core/src/db/executor/aggregate/contracts/state/grouped.rs`
- `crates/icydb-core/src/db/executor/pipeline/runtime/grouped.rs`

Read:

`grouped_expr.rs` is explicitly documented as a compatibility re-export for
grouped aggregate expression programs. It both:

- implements `CompiledExprValueReader` for `RowView`
- re-exports `CompiledExpr` back into aggregate state

That is a real shim. It exists only because aggregate reducer code still imports
the compiled expression type through the aggregate-state boundary even though
the expression implementation now lives in `query::plan::expr`.

Risk:

- medium architectural drift risk
- low immediate runtime risk
- this can obscure the intended owner of `RowView` expression reading

Recommended action:

Completed action:

1. Moved the `CompiledExprValueReader for RowView` impl to the `RowView` owner
   boundary under `executor::pipeline::runtime`.
2. Imported `CompiledExpr` directly from `query::plan::expr` in aggregate
   modules that need it.
3. Deleted `executor::aggregate::contracts::state::grouped_expr`.
4. Ran `cargo check -p icydb-core` after the cleanup.

### 2. Alias Namespace Redundancy: Structural Codec Shims

Priority: `P2 cleanup`

Cleanup status: `partially completed in the follow-up cleanup pass`

Files:

- `crates/icydb-core/src/db/data/storage.rs`
- `crates/icydb-core/src/db/data/by_kind.rs`
- `crates/icydb-core/src/db/data/collection.rs`
- `crates/icydb-core/src/db/data/storage_key.rs`
- `crates/icydb-core/src/db/data/structural_field/mod.rs`
- `crates/icydb-core/src/db/data/persisted_row/codec/mod.rs`
- `crates/icydb-core/src/db/data/persisted_row/codec/traversal.rs`

Read:

These modules are short semantic aliases over `data::structural_field::*`.
Their headers say they preserve the original structural-field functions while
giving callers a semantic namespace.

At audit time, production use appeared narrow:

- `persisted_row/codec/mod.rs` imports `data::storage::{decode, encode}`
- `persisted_row/codec/traversal.rs` imports `data::collection::{decode, encode}`
- direct production imports of original `structural_field::decode_*` /
  `encode_*` functions were not found outside the alias modules

After the follow-up cleanup, the `data::collection`, `data::by_kind`, and
`data::storage_key` alias modules are gone. `data::storage` remains because it
is used more broadly by persisted-row structured and by-kind codecs.

Risk:

- medium redundancy risk
- low runtime risk
- API ownership is split: the alias modules are caller-friendly, but the
  implementation owner remains named as `structural_field`

Recommended action:

Choose one direction:

- preferred: make the alias namespaces the public db-local codec boundary and
  move the implementations behind them over time, or
- simpler: inline the two production alias users back to `structural_field` and
  delete the alias namespace modules

Do not keep both as long-term peers.

Follow-up cleanup note:

- Deleted `data::collection`, `data::by_kind`, and `data::storage_key` after
  inlining their narrow persisted-row codec users to the `structural_field`
  owner.
- Left `data::storage` for a separate pass because it has a broader footprint
  across structured and by-kind slot codecs.

### 3. Legacy Predicate/Expression Bridge Still Carries Transitional Shape

Priority: `P2 cleanup`

Files:

- `crates/icydb-core/src/db/query/plan/expr/predicate_bridge.rs`
- `crates/icydb-core/src/db/query/plan/expr/predicate_compile.rs`
- `crates/icydb-core/src/db/query/plan/expr/canonicalize/mod.rs`
- `crates/icydb-core/src/db/query/plan/expr/type_inference/mod.rs`

Read:

The bridge is production-relevant, not dead code. It converts runtime
`Predicate` trees into planner-owned boolean `Expr`, canonicalizes them, then
compiles canonical boolean expressions back to runtime predicates where a
predicate subset is useful.

The transitional part is visible in comments and APIs:

- `CanonicalExpr` keeps a stage artifact while downstream surfaces still expose
  plain `Expr`
- `PredicateCompilation` wraps a compiled predicate while legacy callers still
  receive the underlying `Predicate`
- `derive_normalized_bool_expr_predicate_subset` supports legacy normalized
  `Expr` inputs
- type inference exposes `TypedExpr::into_expr_type` for callers that still
  consume the stage as a plain type

Risk:

- medium semantic drift risk if new callers bypass the artifact boundary
- medium redundancy risk because planner boolean IR and runtime predicate IR
  still round-trip through explicit bridge code
- low immediate correctness risk because the bridge is well-documented and
  heavily tested

Recommended action:

1. Convert production callers from plain `Expr` returns to `CanonicalExpr`
   where possible.
2. Convert predicate-subset derivation to accept only canonical artifacts at
   production boundaries.
3. Move legacy plain-`Expr` adapters behind `#[cfg(test)]` or delete them once
   production callers are typed.
4. Keep runtime `Predicate` as a distinct filter engine unless the product goal
   explicitly expands to a full predicate/expression VM unification.

### 4. Separate Scalar Micro-Engine Overlaps Function Semantics

Priority: `P2 watch / consolidate metadata`

Files:

- `crates/icydb-core/src/db/scalar_expr.rs`
- `crates/icydb-core/src/db/query/plan/expr/function_semantics.rs`
- `crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs`
- `crates/icydb-core/src/db/index/key/build.rs`
- `crates/icydb-core/src/db/index/key/expression.rs`
- `crates/icydb-core/src/db/query/plan/expr/scalar.rs`

Read:

`db::scalar_expr` is a separate scalar-only expression program for index and
predicate scalar work. It is not the row/grouped scalar projection evaluator,
but it does overlap with part of the expression function surface:

- `Lower`
- `Upper`
- `Trim`
- `LowerTrim`
- `Date`
- `Year`
- `Month`
- `Day`

This appears intentional for hot index/predicate scalar transforms, but it is
still a second semantic surface for a small set of functions.

Risk:

- medium semantic drift risk if function semantics expand in one place but not
  the other
- low immediate runtime risk because the surface is narrow and used by index
  key/predicate paths

Recommended action:

1. Keep `ScalarValueProgram` for now if index/predicate hot paths depend on
   scalar-only execution.
2. Add a clear invariant comment that it is an index/predicate scalar kernel,
   not a projection expression engine.
3. Share function metadata from `function_semantics.rs` where practical,
   especially labels and type expectations.
4. Add or keep convergence tests for overlapping operations against
   `CompiledExpr` where both surfaces admit the same expression.

### 5. Versioned Persistence Envelopes Are Strict, Not Backcompat Branches

Priority: `keep`

Files:

- `crates/icydb-core/src/db/codec/mod.rs`
- `crates/icydb-core/src/db/commit/store/control_slot.rs`
- `crates/icydb-core/src/db/commit/store/marker_envelope.rs`
- `crates/icydb-core/src/db/commit/store/tests.rs`

Read:

The db has versioned persisted envelopes:

- row envelope: `ROW_FORMAT_VERSION_CURRENT = 2`
- commit marker envelope: `COMMIT_MARKER_FORMAT_VERSION_CURRENT = 1`
- commit control slot: `COMMIT_CONTROL_STATE_VERSION_CURRENT = 1`

These do not decode old versions. They validate the version and return
incompatible persisted-format errors for unsupported versions. Tests can encode
older versions to assert fail-closed behavior.

Risk:

- low redundancy risk
- low backwards-compat risk
- this is aligned with hard-cut internal protocol policy

Recommended action:

Keep these strict version checks. Do not remove the version fields unless the
storage format itself is hard-cut in a separate migration. Do not add decode
fallbacks.

### 6. `*_v1` Names Mostly Mark Stable Hash/Capability Contracts

Priority: `keep / rename only with care`

Files:

- `crates/icydb-core/src/db/query/fingerprint/hash_parts/grouping.rs`
- `crates/icydb-core/src/db/query/plan/model.rs`
- `crates/icydb-core/src/db/query/plan/projection.rs`

Read:

The search found several `v1` names:

- `hash_grouping_shape_v1`
- `hash_projection_spec_v1`
- `supports_field_target_v1`
- `supports_grouped_distinct_v1`
- `supports_grouped_streaming_v1`
- `__icydb_scalar_projection_default_v1__`

These are not decode fallbacks or old-version branches. They are stable
semantic/hash/capability labels.

Risk:

- low runtime risk
- low redundancy risk
- naming can look like backwards compatibility if read out of context

Recommended action:

Keep these unless a larger fingerprint/capability rename pass happens. If they
are renamed, treat that as a semantic cache/fingerprint decision, not routine
cleanup.

### 7. Barrel Re-Exports Are Heavy But Mostly Boundary-Owned

Priority: `P3 cleanup`

Files:

- `crates/icydb-core/src/db/executor/mod.rs`
- `crates/icydb-core/src/db/executor/projection/mod.rs`
- `crates/icydb-core/src/db/executor/diagnostics/mod.rs`
- `crates/icydb-core/src/db/executor/pipeline/contracts/execution/mod.rs`
- `crates/icydb-core/src/db/executor/aggregate/contracts/spec.rs`
- `crates/icydb-core/src/db/session/sql/projection/mod.rs`
- `crates/icydb-core/src/db/session/sql/projection/runtime/mod.rs`

Read:

The db tree has many module-root re-exports. Most follow the repository module
boundary rule and are not shims by themselves. A few are cross-owner projections
that can feel like compatibility surfaces:

- executor diagnostics re-exporting db diagnostics types
- aggregate contracts re-exporting planner `AggregateKind`
- pipeline execution contracts re-exporting runtime `ExecutionRuntimeAdapter`
- projection modules re-exporting grouped compile/evaluate helpers

Risk:

- low immediate risk
- medium auditability cost because imports can obscure the owner

Recommended action:

Do not do a sweeping re-export deletion. Instead, remove re-exports only when a
module is already being touched for ownership cleanup. The grouped expression
shim is the best first target.

### 8. Test-Only Legacy Helpers Are Concentrated But Worth Isolating

Priority: `P3 test cleanup`

Files:

- `crates/icydb-core/src/db/session/tests/mod.rs`
- `crates/icydb-core/src/db/session/tests/execution_spine_guard.rs`
- `crates/icydb-core/src/db/executor/tests/pagination.rs`
- `crates/icydb-core/src/db/executor/tests/aggregate_core.rs`
- `crates/icydb-core/src/db/identity/tests/mod.rs`
- `crates/icydb-core/src/db/commit/store/tests.rs`

Read:

The test tree deliberately preserves some old contracts:

- old entity-row scalar SELECT helper wording
- old grouped helper additive-key path
- guards that legacy authority labels do not reappear
- older-version commit marker fail-closed tests
- legacy identity delimiter-collision proof

These are mostly good regression tests, not production shims. The concern is
organization: some helpers live in broad `tests/mod.rs` files and read like
product surfaces.

Risk:

- low runtime risk
- medium maintenance cost

Recommended action:

1. Move legacy helper/proof utilities into named test-support modules when
   those files are next touched.
2. Rename comments that say "old helper" when the behavior is now canonical
   test support.
3. Keep the negative guards that prevent legacy production labels and APIs from
   returning.

### 9. Large Files Are Mostly Tests, With A Few Production Pressure Points

Priority: `P3 incremental`

Largest non-test production pressure points observed:

- `crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs`
- `crates/icydb-core/src/db/session/sql/mod.rs`
- `crates/icydb-core/src/db/query/plan/group.rs`
- `crates/icydb-core/src/db/query/plan/expr/function_semantics.rs`
- `crates/icydb-core/src/db/sql/parser/projection.rs`
- `crates/icydb-core/src/db/query/fluent/load/terminals.rs`
- `crates/icydb-core/src/db/query/expr.rs`
- `crates/icydb-core/src/db/predicate/runtime/mod.rs`

Read:

Large size alone did not prove redundancy. The main production redundancy
inside these large files is already covered above:

- expression/function scalar overlap
- stage-artifact/plain-`Expr` transitional wrappers
- fluent terminal repetition, recently reduced

Recommended action:

Handle these through focused ownership slices rather than line-count-driven
splits.

## Not Findings

### Planner Fallback Reasons

Many hits for `fallback(...)` are planner route diagnostics or tests comparing
optimized and fallback execution. These are not backwards compatibility.

Examples include:

- predicate pushdown fallback reasons
- materialized order fallback
- grouped planner fallback reason labels
- full-scan fallback parity tests

### Prepared Execution Plan Shells

`PreparedExecutionPlan<E>` and `SharedPreparedExecutionPlan` are not classified
as redundant in this audit. The typed shell is still used by fluent/typed
execution, while the shared shell is used by SQL/cache/generic-free execution.
There is some structural duplication in handoff bundles, but the code comments
show a live split rather than a stale shim.

### Public Query Wire Types

`query::expr` is large and schema-agnostic, but it appears to be the public-ish
frontend/fluent filter/order expression surface. It is not redundant with
planner `Expr`; it lowers into planner-owned expressions at the intent boundary.

## Recommended Cleanup Order

1. Delete `executor::aggregate::contracts::state::grouped_expr`. `Done.`
2. Decide whether structural codec alias modules become the real owner or go
   away. `Partially done: collection/by_kind/storage_key removed; storage
   remains.`
3. Tighten predicate canonicalization APIs around `CanonicalExpr` and
   `PredicateCompilation`, then delete plain-`Expr` production adapters.
4. Document and test the `db::scalar_expr` overlap with compiled expressions.
5. Move test-only legacy helpers into named support modules.
6. Opportunistically shrink barrel re-exports during related ownership work.

## Verification

This report began as an audit-only pass. The follow-up cleanup removed the
grouped expression shim and three narrow structural codec alias modules.

Post-report sanity checks:

- `git diff --check` passed
- `cargo check -p icydb-core` passed after the grouped expression cleanup
- `cargo check -p icydb-core` passed after the structural alias cleanup
