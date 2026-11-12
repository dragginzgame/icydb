# Query admission removal map

Source audit, 2026-11-12, continuing authorized 0.310 work. This records removal
boundaries and qualification obligations, not completed refactoring or measured
performance gains. [Programme](query-admission-design.md).

## 1. Remove unreachable intent state, preserve executable predicates

**Confirmed dead production route.** Every constructor of
[LoweredSqlFilter](../../../crates/icydb-core/src/db/sql/lowering/select/mod.rs)
sets visible_expr to Some. There is no mutation that clears it. Its `(None,
Some(predicate))` dispatch is the only non-test caller of StructuralQuery's
filter_normalized_predicate. The preceding compiler experiment established that
removing that branch leaves the intent-only constructor/append chain unused.

The coherent removal includes the optional SQL expression states,
FilterSemanticAuthority::PredicateOnly, NormalizedFilter's predicate-only
constructor/append operations, QueryIntent::append_predicate and the QueryModel /
StructuralQuery forwarding methods. Keep the required SQL predicate mirror until
its admission/derivation owners are consolidated: discarding it now adds work.

Do not remove [executable PredicateOnly filter selection](../../../crates/icydb-core/src/db/query/plan/access_choice/model.rs).
It describes a legitimate execution result after access/residual planning, not
the unreachable authoring mode. Similarly, PreparedQueryParameterContract's
from_normalized_predicate is a live cache contract, not the retired constructor.
A symbol-name search alone would conflate these responsibilities.

## 2. Migrate behavioral tests; delete representation-only assertions

The [intent state tests](../../../crates/icydb-core/src/db/query/intent/state/tests.rs)
include an assertion whose entire purpose is preserving predicate-only authority
without a visible expression. Retire it with that mode. Other fixtures exercise
real semantics and need replacement input, not deletion:

| Boundary | Preserve in maintained fixtures |
| --- | --- |
| Intent append | Failed extraction retains existing intent; AND composition retains uncovered clauses |
| Structural cache key | Both key scope and residual expression affect identity, in either append order |
| Exact access / residual planning | Proving the indexable subset does not prove the complete expression |
| Session residual execution | NULL/unknown rows, limits, offsets, cursor lookahead and changed scopes across cache hits |
| Mutation selection | UPDATE/DELETE never widen their selected rows after exact-key simplification |
| Exact-count and planner accounting | Original key-list facts and failure-before-cache-publication remain intact |

The [session residual fixture](../../../crates/icydb-core/src/db/session/tests/cardinality_tiebreak/residual_filters.rs)
currently builds mixed filters through the retired predicate-only append helper.
Its ten tests cover the behaviors above, including SQL writes and aggregate
results. Do not replace those tests with a test-only compatibility API or a
placeholder `TRUE` expression paired with an unrelated predicate; that would
retain an invalid synchronization contract.

A second trap is extraction granularity. The
[predicate compiler](../../../crates/icydb-core/src/db/query/plan/expr/predicate/compile.rs)
first requires the supplied expression to be runtime-predicate-admissible.
Intent can retain a subset from independently appended clauses even when another
clause is unextractable. Combining all clauses and deriving once is therefore
not automatically equivalent to the existing append contract. A new owner must
preserve useful restrictions without falsely claiming full coverage. This is a
migration risk, not a newly established user-visible defect.

## 3. Remove semantic-to-syntax writeback, including membership work

**Confirmed repeated transformation.** In
[bindings](../../../crates/icydb-core/src/db/sql/lowering/bindings.rs),
admit_compare_or_expression lowers a SQL subtree, compiles/adopts a predicate,
normalizes and validates its operands, then writes the admitted value back into
SqlExpr::Literal. bind_prepared_where lowers the whole expression again for truth
shape; [parameterized lowering](../../../crates/icydb-core/src/db/sql/lowering/parameterized.rs)
then constructs LoweredSqlFilter through another lowering pass.

Membership has an additional concrete copy site: for each relevant bound item,
bind_expr clones the membership target into a temporary equality AST, validates
it, and moves the canonical right-hand literal back into the membership list.
If the target itself was bound, that happens for every item. This is static
source evidence of repeated construction, not an IC-cycle measurement or a claim
that all binding paths are quadratic.

The replacement should retain admitted semantic operands and derive the filter
from them, while visiting invalid operands before destructive folds. Keep exact
parameter arity/placement, SQL literal policy, NULL comparison handling, typed
error field roles, first-call error precedence and current-schema rebinding.
The existing infer_here suppression avoids some redundant nested inference;
do not describe every AST node as independently re-inferred.

## 4. Keep exact-count admission independent of full planning

The [exact-count session route](../../../crates/icydb-core/src/db/session/query/exact_count.rs)
constructs accepted-schema structural intent and asks for prefix keys directly.
[try_build_count_cardinality_prefix_access_from_query_model](../../../crates/icydb-core/src/db/query/plan/pipeline.rs)
consumes the intent predicate without going through
prepare_query_model_scalar_planning_state_with_schema_info.

Consequently, PreparedScalarPlanningState cannot become the only place where
admitted filters exist unless this shortcut is redesigned. Forcing exact-count
through full planning would add stages rather than consolidate them. Keep one
lightweight accepted-schema admission result usable before access selection;
scalar planning can borrow/retain that result with its existing schema handle.
Preserve exact-count's deliberately narrow supported shapes and typed refusal.
This qualifies the earlier suggestion to extend the existing planning-state
owner: reuse it for planning, not as a mandatory entrypoint for every consumer.

## Qualification and delta

Ran the existing compiled Core test binary for the inspected boundaries:
10 session residual-filter tests and 4 SQL binding tests passed. These establish
a baseline only; no candidate implementation was tested in this continuation.
Documentation references and diff hygiene are checked separately.

Net retained production code: 0 lines. Net retained test code: 0 lines. No
complexity reduction has landed in this audit turn. The proposed reduction is
one unreachable intent mode plus the SQL semantic-to-syntax round trip, without
removing residual execution or the exact-count shortcut. Wasm module/code-section
bytes, IC cycles and instruction effects remain unmeasured. Broad/release gates
were not requested and remain skipped user-owned validation.

## Implementation follow-through

The first removal is implemented: SQL filters always contain an expression, and
NormalizedFilter directly owns that expression without an authority enum.
The predicate-only intent constructor and append chain are gone. SQL's existing
required-predicate route retains its already-derived value; the rejected
mirror-discard/rederive design was not introduced.

Two obsolete predicate-append-only tests were removed. Remaining fixtures use
expression-backed clauses or accepted-schema filter builders. The covering
matrix intentionally qualifies strict planner contracts: duration/timestamp
widening in public frontend lowering must not silently replace those fixtures'
strict semantics. Its paired expression/predicate inputs now explicitly carry
matching semantics through the maintained handoff, rather than constructing a
predicate-only intent or a placeholder expression.

Measured delta against the pre-removal working tree: -79 production / +37 test
Rust code lines, net -42. One intent mode and its dispatch were removed; no generic
adapter, compatibility alias or test-only legacy constructor was added. Exact-count
still consumes admitted intent without a full planning detour. The parameter
writeback removal and the rest of the bound-filter programme remain pending.

Focused qualification passed: 503 query tests, 298 cardinality/covering session
tests, 99 ordering/cache session tests and 66 SQL-lowering tests (966 total).
Strict Core Clippy with SQL/migration/metrics and documentation checks pass.
Broad/release gates remain skipped user-owned validation; no package versions
or deployment changed. This qualifies the intent removal, not the whole programme.

## SQL binding follow-through

Canonical operands are no longer written from temporary predicates back into
SqlExpr literals, including membership items. Early admission now consumes
borrowed semantic operands; membership shares one lowered target rather than
constructing and lowering one SQL equality tree per relevant item. For complex
targets, the maintained expression inference gate still receives a comparison.
No legacy/new switch, generic AST adapter or additional persisted representation
is introduced. This adds 22 production / 48 test code lines (net +70); mechanism
reduction is writeback removal and fewer target lowerings, not fewer Rust lines.

This is narrower than complete direct semantic binding. Templates still make an
execution-local SQL copy and substitute parameters; whole-query lowering follows
admission. Those remaining stages must be consolidated with the accepted-schema
filter owner, without moving exact-count through full planning. Wasm code/module
bytes, cycles and instruction impact are unmeasured.

Final focused qualification passed: 67 SQL-lowering tests, 99 parameter/cache
ordering tests and 298 cardinality/covering/residual session tests (464 total).
Strict Core Clippy with SQL/migration/metrics, make fmt-check, documentation links
and diff hygiene pass. Broad/release gates remain skipped user-owned validation.
No package versions or deployment changed.

## Admitted WHERE handoff

Parameter admission now returns the semantic expression already produced for its
truth-shape check. Scalar/grouped template binding and global-aggregate binding
consume it directly through shared boolean normalization, removing one repeated
whole-WHERE lowering pass. Cold bound SELECTs use the same checked-template
compiler as cached parameterized calls; the separate bound-statement preparation
function is removed. No current-schema or pre-fold gate is intentionally skipped.

Delta: +2 production / -23 test Rust code lines (net -21). Tests now inspect the
maintained semantic output. There is no additional semantic representation or
legacy mode. This does not eliminate the execution-local SQL copy for substitution,
nor all leaf admission lowering; wider direct binding and bound-filter authority
consolidation remain pending. Cold compilation now constructs a template as well;
Wasm module/code-section bytes and IC cycles/instructions remain unmeasured.

Qualification for this handoff: 67 SQL-lowering, 99 parameter/cache ordering and
298 cardinality/covering/residual session tests pass (464 total), together with
strict Core Clippy under SQL/migration/metrics and documentation/diff checks.
Broad/release gates remain skipped user-owned validation. No package versions
or deployment changed; this is not a complete programme closeout.

## Slice 3 audit: remove syntax substitution, preserve literal rules

Source inspection on 2026-11-12, after the accepted-schema filter owner landed,
confirms a remaining independent traversal in `sql/lowering/bindings.rs`.
`CheckedSqlSelectTemplate::compile` clones the extracted WHERE predicate, and
`bind` clones its retained predicate on every invocation. `bind_expr` mutates
that copy from parameter slots to SQL literals. Early operand admission lowers
selected subtrees, then `bind_prepared_where` lowers the complete result. These
are actual source paths, not a measured allocation or instruction count.

The removal target is the execution-local SQL copy and substitution walker,
including membership-slot mutation. Keep the immutable parsed template, binding
arity/payload admission, semantic expression, accepted-schema filter authority
and value-dependent planning. The planning template's current-value memo is a
separate responsibility, not another SQL binding implementation to remove.

Two syntax-sensitive rules need explicit propagation into direct binding:

- `lower_sql_binary_expr` checks a field/arithmetic/literal pair with
  `validate_numeric_literal`. Today substitution turns a parameter into precisely
  that literal shape. Resolving parameters only after this check would bypass it.
- `lower_sql_numeric_scale_function_call` recognizes a literal scale, validates
  its supported unsigned range, and canonicalizes it to `Value::Nat64`. Today a
  scale parameter takes this branch after substitution. Direct binding must use
  the resolved literal before deciding the scale rule, rather than treating it
  as an arbitrary expression.

These are migration hazards found by source inspection, not confirmed defects
in the maintained implementation. In particular, the binding test module covers
nested functions, dead CASE branches and comparison admission but contains no
named scale-parameter control; inspect the wider SQL tests before adding one.

Implement parameter resolution inside the existing SQL expression lowering
owner, with a concrete invocation context for optional bindings/current schema.
Do not introduce a second bound-expression AST, generic visitor framework or
fallback binder. Preserve the distinction between subtrees containing slots and
literal-only subtrees while enforcing pre-fold operand admission. Child results
should flow directly into their parent semantic expression, avoiding another
lowering of the same subtree just to validate it. Normalization must not erase
invalid dead operands before admission; ILIKE already invokes Boolean
normalization within the lowerer, so moving all validation to a final pass is
not a sufficient design.

Complete the change across scalar, grouped and global-aggregate WHERE templates
in one slice. Qualify cold/warm changed values; field arithmetic; numeric scales;
NULL/identity coercion; membership with bound targets/items; nested CASE/function
and unreachable-branch refusal; exact/+1 input budgets; accepted-schema renewal;
and failure leaving the previous cache resident usable. Preserve the first-call
WHERE-error precedence over projection/lane lowering. The existing direct
mutation, HAVING and aggregate FILTER restrictions remain explicit consumers
for slice 4, not new parameter placements.

This audit changes no runtime code: production/test Rust delta is zero. Expected
complexity reduction is one mutable syntax representation per invocation and one
independent substitution traversal. Net implementation LOC and Wasm module/code
bytes, IC cycles and instruction effects remain unmeasured. No feature or
performance reduction is inferred from deleting the walker alone.

## Slice 3 implementation outcome

The mutable substitution route described above is removed. Both template compile
and warm bind pass borrowed WHERE syntax to the existing expression lowerer.
Bound operands enter the semantic tree directly; early comparison/inference
admission consumes those already-lowered children. The invocation context is
stack-local and contains no second AST or cached bound-value representation.
Membership retains its explicit per-item comparison checks and shared target.

The numeric-scale audit hazard applies to programmatically authored WHERE
syntax. Text parsing already requires a literal scale argument; its grammar was
not widened. Regression controls compare supported bound scales to literal
semantic output and retain typed refusals for negative, oversized and text scales,
plus field arithmetic refusal inside a dead Boolean branch.

466 focused SQL/session tests and strict Core Clippy pass. Production/test code
delta is +49/+65 lines (net +114), measured against the pre-slice affected files.
The removed mechanism is an entire syntax substitution traversal and per-call
WHERE copy; this does not establish smaller Wasm or lower IC costs. Formatting,
selected documentation and diff checks pass; broad/release gates remain unrun.
See the [current programme status](../../design/0.310-query-admission/status.md#direct-semantic-parameter-binding)
for evidence limits and the remaining consumer closeout.

## Global aggregate template ownership

The slice-4 consumer trace found a redundant execution stage in
`sql/lowering/aggregate/command/binding.rs`: template creation cloned its base
shape, constructed a complete unfiltered command, then retained both. Each bind
cloned that command and replaced its structural query and eligibility facts.
Those discarded facts included extrema ordering and metadata-count eligibility,
which must be derived from the actual bound filter.

The template now retains only base shape, accepted aggregate strategies,
projection and HAVING. One consuming command constructor binds the actual query
and derives filter-sensitive facts for both ordinary and parameterized calls.
The obsolete forwarding constructor is removed. Current-schema order capability
checks remain explicit, including the original template-shape diagnostic order.
No new type, cache, executor variant or compatibility branch is introduced.

This removes one retained complete command and its throwaway query/fact
construction, rather than merging specialized aggregate executors. Task-relative
cloc is -4 production / +4 test lines, net zero. The existing cached SELECT
matrix now also covers aggregate FILTER, HAVING, LIMIT 0 and OFFSET 1 with changed
WHERE bindings, literal-query controls, exact cache capacity and failed binding.
This is not a measured retained-byte, Wasm or IC cost result.

Closeout: 466 distinct focused tests pass (ordering/cache, SQL lowering and
cardinality/covering/residual), plus strict Core SQL/migration/metrics Clippy,
formatting and documentation/diff checks. The corrected pagination fixtures
retain required ordering. Current checks use the preserved Wire 0.21.3 lock;
older retained-binary output is not counted. The [programme closeout](../../design/0.310-query-admission/status.md#native-consumer-closeout-and-aggregate-template-contraction)
records the limits; no broad or installed qualification is claimed.

## Post-programme closeout: next independent boundary

The four-slice query-admission programme has a native-focused-ready verdict.
Further continuation is a closeout audit, not automatic authorization for an
independent executor redesign or another minor line. No new runtime edits or
checks are implied by this source inspection.

MA-04 remains the next substantial mechanism candidate. The maintained physical
binding contract crosses these concrete owners:

- `access/lowering.rs::lower_index_specs_for_path` emits prefix/range specs.
- `PreparedExecutionPlanResidents` retains the semantic `AccessPlannedQuery`
  alongside two independently typed `Arc` slices of physical specs.
- `AccessStreamBindings` forwards those slices and continuation policy.
- `stream/access/traversal.rs::produce_key_stream` consumes the slices in tree
  order, derives required counts from leaf shape, and validates prefix/range
  alignment before opening physical streams. `AccessSpecCursor` also validates
  complete consumption. Union/intersection traversal shares that protocol.
- Covering projection and `exact_count_cardinality_prefixes_for_plan` consume
  lowered specs without necessarily opening the ordinary row stream. These
  consumers prevent treating the arrays as traversal-only scratch.

The proposed outcome is one prepared binding authority whose borrowed leaf
views already identify their physical specs, while covering and metadata paths
reuse those same admitted specs. Preserve the semantic plan as the continuation
and diagnostic authority. The removal target is repeated tree/array agreement
and positional stream binding, not specialized physical scanning or exact-count
shortcuts. A replacement must show how it identifies leaves without reconstructing
an owned execution tree or adding a parallel lookup/cache with greater retained
state. No such design has been qualified yet; moving the cursor behind another
wrapper or replacing two offsets with one is not this outcome.

A bounded investigation should first inventory leaf identity/order and admission
ownership; then prototype a borrowed prepared binding against direct prefix,
multi-prefix, range, union and intersection paths; finally propagate covering,
metadata count and continuation consumers as one end-to-end implementation if the
prototype demonstrates fewer mechanisms. Refusal timing, missing-row provenance,
ASC/DESC order, resumed pages, resource exhaustion and retained-cache accounting
are acceptance conditions. Net code and retained-state estimates are unknown.
Wasm/IC effects cannot currently be qualified through the maintained actors due
to the known Canic graph conflict; do not use native timing or assume a win.

This closes the present audit pass with zero production/test Rust changes. It
identifies independent future scope, rather than declaring another implementation
slice complete or silently widening the finished query-admission programme.

## Authorized access-binding hard cut

The maintainer authorized this outcome as slice 5 of the existing pending 0.310
line. Implementation and native focused qualification are complete.

The existing `AccessPlan` tree now retains each index leaf's physical specs in a
shared owner. Its executable projection borrows the semantic payload and binding
together. Primary leaves retain no physical owner. Semantic equality, canonical
ordering and fingerprint projections exclude bindings. Semantic rewrites that
rebuild leaves discard preparation; execution rejects an unprepared index leaf
instead of reconstructing bounds or accepting a parallel spec array.

Removed mechanisms: `AccessSpecCursor`, its speculative copies and complete-
consumption protocol, repeated prefix/range index-name alignment, prepared-core
parallel spec arrays, `IndexSpecBundle`, and the now-redundant grouped access
handoff wrapper. Scalar and grouped execution retain continuation/expansion
policy independently of immutable physical binding ownership.

`PreparedScalarRuntimeHandoff::into_scalar_page_suppressed` shares leaf owners
when copying the semantic query without pagination. Exact-count admission reads
bindings from its plan. Covering admission accepts single leaves, including
multi-lookup and composite-index branch sets; it does not require a flattened
copy of union/intersection specs. Covering prefix expansion remains local to the
invocation, without mutating the prepared tree or continuation identity.

Transient cardinality-candidate probes and grouped resume-bound construction
remain live consumers of the same encoding primitives. They do not retain an
execution binding tree. Physical collection reservation is shared at the leaf
encoder; retained-owner construction has an additional admitted allocation.

The tradeoff is explicit: index leaves now each have a shared owner instead of
two global arrays. This removes traversal synchronization and separate handoffs,
but changes allocation granularity for composite plans. Retained accounting
visits those owners and cloned plans share them. No memory/Wasm/cycle/instruction
improvement is claimed; the maintained Wasm measurement remains blocked by the
known Canic graph conflict.

New focused cases cover nested child reordering, prefix/multi-prefix/range
binding identity, semantic equality, clone sharing, retained accounting,
unprepared-index rejection and failure to allocate a retained binding. Existing
session tests qualify covering, metadata, DISTINCT, ordering and continuation.
Package versions and deployment remain unchanged.


Closeout: 813 distinct focused tests and strict Core Clippy pass. The
[0.310 tracker](../../design/0.310-query-admission/status.md#access-binding-hard-cut-closeout)
records the selected families and corrected rejection fixtures. Task-relative
cloc is -123 production / +122 test code lines (net -1); this is a mechanism
reduction, not a substantial net source-size reduction. Broad/release gates and
macOS/installed qualification remain unrun.


## Physical scan handoff follow-through

The authorized follow-through removes `IndexStreamConstraints`,
`StructuralPhysicalStreamRequest` and traversal's physical forwarding method.
Physical dispatch accepts the borrowed executable leaf and derives its private
prefix/range references at that boundary. Prefix arity is checked once before
specialized scan dispatch instead of in four helper paths; redundant count
arguments are removed. Private scan bindings remain invocation-local, with no
second retained owner or new generic abstraction. Prefix expansion and physical
storage decoding keep their own responsibilities.

The primary-key resume matrix now exercises this entrypoint in both directions
with scalar and composite keys. A direct composite-plan rejection case checks
that the physical leaf boundary remains fail-closed. All 439 selected stream,
cardinality/covering and ordering/cache tests pass, together with strict Core
Clippy and formatting/documentation checks. Task-relative cloc is -55 production /
+12 test code lines (net -43). Wasm/IC effects remain unmeasured; broad/release
gates were not run. Per-leaf storage representation is unchanged in this slice.
