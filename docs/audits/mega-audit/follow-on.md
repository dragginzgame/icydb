# Write, migration and aggregate follow-on investigation

Requested after the initial cleanup closeout on 2026-11-11. This is source-backed
investigation evidence, not a separate issue queue or an implementation claim.
The investigation initially changed no production code. The maintainer later
authorized candidates 1–2; their implementation evidence is recorded in
[the implementation report](implementation.md#follow-on-migration-and-aggregate-reductions).
Candidate 3 remains a proposal.

## Ranked candidates

### 1. Move completed migration working state into fallback candidates

Owner: [migration planner](../../../crates/icydb-core/src/db/schema/migration_planner.rs),
`WorkingStore::candidate` and the final assembly in `plan_schema_migration`.

`WorkingStore::candidate(&self)` clones the enum catalogue, composite catalogue,
source bindings and entity snapshots into a new accepted bundle. Its only call
is the final loop over `working.values()`, after `reconcile_rekeyed_view` has
finished borrowing that map. Later program compilation uses original `stores`
and assembled candidates, not `working`. Reconciled candidates take precedence;
untouched stores produce no candidate.

**Smallest change:** consume `working.into_values()` and make the fallback
candidate constructor consume its working store. Move its four owned fields
into the accepted bundle. Preserve checked revision advancement, store order,
touched-store omission, reconciled-candidate precedence and typed errors.
No borrowed overlay, lazy catalogue or new working-state representation is needed.

**Do not conflate:** `exact_bundle` creates the rekeyed view used by the ordinary
lowerer while the working state is still needed. Eliminating that copy would be
a different ownership redesign. Migration already delegates remaining-change
semantics to `lower_existing_schema_proposal`; it is not an independent ordinary
schema implementation.

**Expected complexity delta:** four final copy operations removed, one explicit
ownership transfer, no new type or mechanism. Net code delta is unmeasured and
likely small; copied catalogue volume, Wasm bytes and IC effects are unmeasured.
**Qualification:** no-op/adoption, rename-only fallback, rename with reconciled
changes, shared named types across stores, physical transforms and preparation
error classification. Existing planner tests cover these boundary families.

### 2. Unify owned and borrowed value-reducer ingestion

Owner: [value reducer](../../../crates/icydb-core/src/db/executor/aggregate/value_reducer.rs),
`ingest`, `ingest_owned` and `ingest_cow`.

Borrowed and owned ingestion independently repeat NULL admission and the same
COUNT/SUM/AVG/MIN/MAX match. Their useful difference is ownership of a selected
MIN/MAX candidate: borrowed input clones only when selected, owned input moves.
`ingest_cow` already carries this distinction but immediately dispatches back
into the two duplicated implementations.

**Smallest change:** make the existing Cow entrypoint own the semantic match;
retain thin borrowed/owned adapters where callers need them. Compare using a
borrow and call `into_owned` only when an extremum is selected. Preserve all
numeric coercion, U256 SUM, typed overflow and mixed-domain refusal behavior.
Do not merge canonical ordered extrema with numeric-or-strict comparison:
those are distinct contracts used by grouped FIRST/LAST versus value extrema.

**Expected complexity delta:** two semantic ingestion implementations become
one using an existing ownership representation. No new trait, generic reducer
framework or cache. Net code delta is unmeasured until implemented; no Wasm/IC
speedup is claimed. **Qualification:** NULL/empty input, COUNT, Decimal bounds,
U256 SUM overflow, incomparable values, and owned/borrowed selected and rejected
extrema; direct scalar and grouped callers must preserve results and ownership.

### 3. Share grouped HAVING/resume eligibility, retain specialized page storage

Owners: [COUNT window](../../../crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/count/window.rs)
`row_matches_window` and [generic page selection](../../../crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/generic/page_finalize.rs)
`GroupedPageCandidate::matches_window`.

Both construct a borrowed `GroupedRowView`, evaluate the same compiled HAVING
expression, then apply the same resume-boundary predicate to the canonical group
key. The generic selection wrapper merely delegates and is not a third policy.
The shared low-level expression and boundary evaluators already exist.

**Smallest change to evaluate:** one borrowed eligibility helper owning the
HAVING-before-resume policy, fed the caller's group key, aggregate-value slice,
direction and prepared expression/boundary. COUNT can keep its one local count
value; generic rows keep their existing slice. Do not allocate a generic
candidate merely to call the shared policy, and reject the helper if its argument
plumbing outweighs the removed duplication.

**Expected complexity delta:** one eligibility policy instead of two; one local
helper added, no new payload representation. Net code and Wasm/IC delta remain
unmeasured. **Qualification:** HAVING true/false/NULL/errors, ASC/DESC continuation,
COUNT and generic aggregate pagination, offset, zero limit and lookahead.

## Larger duplication that is not yet a deletion proposal

COUNT and general grouped finalization both maintain bounded selection heaps,
unbounded ordering, offset/limit and cursor output. However, COUNT stores
`(GroupKey, u32)` and can avoid the generic aggregate-value/candidate envelope;
the generic path also supports compiled ordering and richer projections.
`OrderedGroupedPageSelection` already reuses general finalization for ordered
streams. A single generic heap/page engine could add allocations, generic
instantiations or dynamic branches. Keep these paths until matched uncompressed
Wasm total/code-section and IC instruction/cycle evidence supports consolidation.
The entire window module is not a defensible deletable-line estimate.

Scalar DISTINCT and grouped hash buckets both use inline-single/collision-vector
storage. Scalar buckets own Values with exact equality; grouped buckets carry
indexes into separately owned groups. A generic bucket would save similar
mechanics but introduce another abstraction and alter ownership/reservation
plumbing. This is lower priority than the existing-Cow ingestion consolidation.

## Write-flow verdict and review limits

[Structural mutation](../../../crates/icydb-core/src/db/executor/mutation/structural.rs)
already owns identity materialization, staging, constraint scheduling and atomic
publication. Session write callers enter `execute_accepted_structural_mutation_batch`;
the bounded-update entrypoint adapts its iterator and delegates to that same owner.
`ExpectedLoaded` retains a synchronously validated row to avoid a second point
read. Complete versus bounded-prefix packing and mutation-progress publication
have different atomicity/progress obligations. No duplicate write coordinator was
found in the inspected paths; collapsing those contracts is not justified.

This pass inspected the named migration assembly, structural coordinator and
aggregate reducer/finalization boundaries. It is not an exhaustive audit of all
write frontends, relation actions, SQL lowering or every aggregate route.

**Investigation-only delta:** 0 non-test and 0 test code lines; production
complexity was unchanged at the investigation boundary.
No code tests were selected for this investigation. Documentation/link and diff
checks qualify the report only. The first two candidates are the recommended
implementation order; the narrow third candidate needs a size/argument-cost
check before committing to a shared helper. No new minor or delivery is selected.

## Deeper grouped-page and DISTINCT audit

Further source tracing after the tooling refresh narrows candidate 3 and identifies
one additional ownership-local candidate. This remains investigation, not authority
to implement a larger grouped executor redesign.

### Eligibility sharing must preserve validation timing

COUNT's `row_matches_window` constructs its count value and requires a list-shaped
group key before checking HAVING, even when no HAVING expression is present.
Generic `GroupedPageCandidate::matches_window` asks for list-shaped key values
only when evaluating HAVING; key projection or emitted cursor construction may
validate them later. `GroupKey` also supports DISTINCT scalar keys, so its type
alone does not establish the grouped list-shape invariant.

A helper that eagerly builds every generic `GroupedRowView` would therefore
change error timing for malformed internal candidates. No user-facing defect is
established; this is a constraint on refactoring. Preserve the current caller
validation boundaries and share only the already-borrowed eligibility operation,
or first prove that every relevant production constructor guarantees the shape.
Introducing a closure/proof wrapper merely to save these short branches could
increase complexity. Candidate 3 is conditional, not yet a ready deletion.

The bounded generic path qualifies candidates before heap retention, then calls
`finalize_rows_from_candidates` with an always-true filter. It does **not** repeat
HAVING or resume evaluation during final pagination. The unbounded path filters
while producing its page. COUNT also filters before selection and does not filter
again in its output loop. No redundant second evaluation pass was found.

### Retain specialized heap and output payloads

Both paths use a max-heap with root replacement for the retained window, but their
payloads and obligations differ. COUNT carries `(GroupKey, u32)`; general candidates
carry aggregate values and canonical or compiled top-k ranking. Both charge their
actual candidate type before retention, and comparison can follow different
ordering contracts. General output can compile projection before HAVING setup;
COUNT projects only its selected output rows. A shared heap/page framework must
account for these ordering, admission and allocation differences. No safe whole-
module deletion or defensible large LOC estimate follows from similar loops.

### 4. Remove the repeated grouped DISTINCT membership probe

Subsequently authorized for implementation; see the
[implementation evidence](implementation.md#grouped-distinct-membership-preflight).

Owners: [DISTINCT admission](../../../crates/icydb-core/src/db/executor/aggregate/contracts/grouped/context.rs)
`ExecutionContext::admit_distinct_value` and
[grouped key set](../../../crates/icydb-core/src/db/executor/group/key.rs)
`GroupKeySet::insert_key`.

The sole production insertion caller validates and hashes the borrowed value,
then returns immediately if `contains_value` finds it. It next checks total and
per-group caps, constructs the retained key and charges it before calling
`insert_key`. The latter performs another `contains` before reservation and
`HashSet::insert`. No call between the two probes mutates the key set; the caller
holds exclusive access. Existing collision/parity tests compare borrowed lookup
against retained canonical equality, including forced hash collisions and maps.

**Smallest candidate:** let the insertion operation consume the caller's existing
absence proof by contract, reserve fallibly, and retain the actual insert result
for the existing invariant assertion. Remove the redundant pre-insert `contains`;
do not replace the HashSet, weaken collision equality, or remove the initial
borrowed probe that avoids copying/charging duplicates. Tests directly exercising
unconditional insertion must be reviewed against the narrowed caller contract.
Do not keep a second production insertion API solely for old test convenience.

**Expected complexity delta:** one duplicate lookup removed, with no new type,
trait or collection. Net code and IC/Wasm impact are unmeasured. This is a small
cleanup, not evidence of a second DISTINCT engine. Qualification must preserve
duplicates at full budget, total-before-per-group refusal, unchanged sets on
failure, scalar/map/Decimal canonical equality, and forced hash collisions.
Fallible reservation must still happen before mutation.

Borrowed canonical validation, retained canonical construction and allocation-free
lookup remain distinct: validation lets duplicates fail closed without allocating
a new retained tree, while construction is needed only for new keys. Their shared
map-normalization owner already exists. Merging them by cloning inputs first
would defeat the maintained ownership boundary.

**Review delta:** 0 non-test and 0 test code lines. No runtime tests or benchmarks
were run for inspection-only work. Documentation and diff checks passed. The next
small implementation candidate is the DISTINCT probe; broader page consolidation
remains conditional on error-order proof and matched Wasm/IC evidence.
