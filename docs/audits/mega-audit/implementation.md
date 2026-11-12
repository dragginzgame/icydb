# Simplification implementation evidence

The maintainer authorized working through the audit proposals on 2026-11-11.
This records the resulting evidence, not a second follow-up queue. Existing
0.310 work, dependency selections and package versions are preserved.

## First bounded batch

- **MA-02:** actual publication head is computed once and reused for ordinary
  receipts. Deferred activation still computes its separate future receipt
  head; no-op retains its admitted target. No new abstraction or persisted bytes.
- **MA-01:** remove `ExecutorErrorDetail`, its `ErrorDetail` variant and mapping.
  Move the remaining constraint-program-corruption constructor to the existing
  canonical diagnostic representation. The public Rust enum removal is a hard
  cut; public diagnostic code/class/origin/facts remain qualified independently.
  Explicit constructor searches found no other repository caller or caller in
  the available Canic/Storage sibling sources. This is not a claim about all
  unknown external Rust consumers; they must rebuild against the hard cut.
- **MA-03:** preflight keeps a vector of borrowed bundle selections instead of
  cloning accepted/candidate bundles. The target resolver consumes borrowed
  optional bundles at the same positions; removal callers adapt their existing
  slice iterators. There is no new overlay type, cache or persistence format.

## Code and complexity delta

Cloc, affected files before versus after, excluding comments/blanks:

| Owner | Non-test Rust code delta | Test Rust code delta | Complexity delta |
| --- | ---: | ---: | --- |
| Schema application | +5 | 0 | Remove one duplicate ordinary-head derivation and owned bundle copies; retain borrowed selection vector |
| Core errors | -76 | +12 | Remove one enum, one outer variant and mapping; add one typed diagnostic regression test |
| Total | **-71** | **+12** | Net **59 code lines removed**, no new module, trait, cache or dependency |

The resolver accepts an iterator rather than an owned-bundle slice. Count this
signature adaptation in the result; do not describe the borrowed vector itself
as eliminated. The small schema code increase buys simpler ownership and less
copying. No IC instruction/cycle or total/code-section Wasm result is claimed.

## Remaining proposals: evidence before edits

- **MA-06:** implemented after the additional caller proof below; the initial
  caution is resolved for current production constructors, not waived.
- **MA-07:** retain slot sorting for now. `SchemaInfo` stores a name-sorted field
  vector and does not retain the original accepted layout handle. A second slot
  vector/cache would add retained state; replacing name lookup with linear scans
  would trade one cost for another. No net simplification is established yet.
- **MA-04:** the owned executable tree is replaced with borrowed projections as
  described below. The subsequently authorized 0.310 hard cut also removes
  positional spec binding; see the current query-admission removal map.
- **MA-05:** coverage bookkeeping is simplified as described below. Expression
  authority and predicate subsets retain their distinct semantic roles; this
  does not claim that the complete expression and predicate representations can
  safely be merged.
- **MA-08:** provenance is not a disposable cache. Exact/superseded retirement
  and allocation identity keep old journal batches from retiring newer effects.
  A combined entry needs a traversal/recovery comparison before replacing the
  existing shared merge and position owners.
- **MA-09:** no adapter change without linked Wasm/IC evidence. The maintained
  reachable-operation actors use the deferred Canic adapter; no substitute
  native timing or speculative code-size result is supplied.
- **MA-10:** keep the small fixture duplication for this batch. The operation
  macro is already shared; changing helper reachability without qualifying both
  measurement actors risks changing the measurement subject for a small saving.

These are limits on what is proven, not completion of every proposal.
Filter representations and the remaining positional binding protocol still
need deeper evidence. This does not claim completion of the entire programme.

## Validation

Evidence directory: `target/evidence/mega-audit-simplification-20261111/`.
Strict Core library/test Clippy with SQL, migration and metrics passes.
All 48 selected diagnostic tests and 47 schema creation/lineage tests pass,
including typed corruption diagnostics, ordinary/replayed application, no-op
lineage repair, relation-domain refusal and deferred activation/abort/completion.
Final documentation/format verification is recorded with the batch evidence.
Broad workspace/release gates were not selected. No macOS or new installed
Canic qualification, deployment or network lifecycle action is claimed.

## Canonical predicate hashing follow-through

The production constructor search found one `ScalarPlan` assembly owner:
`build_logical_plan` in `query/plan/logical_builder.rs`. Its two production callers
are the general planning pipeline and the trivial no-filter path. General
planning carries `normalize_query_predicate` output through
`strip_redundant_primary_key_predicate_for_exact_access`, which only retains or
removes the whole predicate. Trivial planning supplies none. No production
assignment of raw syntax into the scalar predicate was found. Cache parameter
contracts do not create a second scalar-plan constructor. This closes the
caller-tracing gap recorded in the original audit without adding a marker type.

`hash_canonical_predicate` now hashes the carried tree directly. The shared
value encoder still canonicalizes membership/coercion bytes, and capacity and
budget admission remain before any hash-stream update. No identity domain,
serialization tag, digest algorithm or continuation format changes. Raw-syntax
test helpers explicitly enter the normalization owner before hashing.

Cloc delta for this slice: **-9 non-test code lines, +30 test code lines**
(net +21). One production wrapper and its copy/normalization pass disappear;
no type, cache, trait or dependency is added. The additional regression proves
canonical hashing fits an encoding-only temporary-memory budget. Combined with
the initial batch: -80 non-test, +42 test, net -38 code lines.

Evidence: `target/evidence/mega-audit-hashing-20261111/`. Strict affected Core
library/test Clippy passes. Seventeen existing predicate fingerprint cases pass,
as does the new encoding-only budget regression after correcting its fixture to
use the encoder's conservative reserved capacity rather than final byte length.
The original failure is retained; no production admission was weakened.
All 60 query fingerprint cases and the compact-membership normalization
idempotence case pass (79 focused passes in total). Formatting, documentation
and diff checks pass. IC cycles, instructions and raw Wasm total/code-section
deltas remain unmeasured; no broad gate or deployment was selected.

## Borrowed executable access projection

MA-04 removes the second owned union/intersection tree. `ExecutableAccessPlan`
is now the mechanical enum itself; its composite variants hold a private
borrowed child view. The view exposes only executable child projections and
length, so executor consumers do not acquire raw semantic path matching.
Projection no longer recursively collects `Vec<ExecutableAccessPlan>`.

Traversal and shape analysis project direct children on demand. Exact
intersection fallback still consumes rejected siblings' lowered bindings;
accepted-index unions retain missing-row filtering; nested streams retain
ordering, limits and continuation behavior. The existing prefix/range arrays,
two spec offsets and alignment/end-consumption checks are not removed.

Net cloc: **-13 non-test code lines, 0 test code lines**. Two internal types
remain: a plan enum and a borrowed child view replace a node enum and plan
wrapper. No new trait, cache, dependency or dynamic-dispatch layer. The benefit
is one fewer owned representation and no eager recursive projection pass, not
an assertion that every projection instruction disappears. Leaf projections
can now be evaluated again by separate readers; IC instruction/cycle and raw
Wasm total/code-section impact remain unmeasured.

Evidence: `target/evidence/mega-audit-access-20261111/`. Strict affected Core
library/test Clippy and all 52 selected tests pass: 40 access-stream boundary,
scan and polling cases; two semantic/executable shape cases; eight live/general
intersection cases; branch-resume coverage; and mixed primary/index union
missing-row/corruption coverage. Formatting, documentation and diff checks pass.
Existing behavior tests required no source changes. Broad workspace/release
gates, new macOS qualification and deployment were not selected.
The three audit batches total **-93 non-test, +42 test, net -51 code lines**.

## Filter completeness bookkeeping

MA-05 removes the three-state coverage enum and its combination/getter helpers.
Production consumers only ask whether the subset is complete. The existing
`Option<Predicate>` already distinguishes no subset from a partial subset; a
single completeness flag retains the additional proof. Constructors establish
that completeness implies a present subset, and conjunction preserves that
proof only when both inputs were complete. Appending an indexable predicate
cannot make a previously uncovered expression disappear.

Expression-backed versus predicate-only authority remains separate. No SQL
expression lowering, runtime predicate semantics, exact-key pruning rule or
persisted contract changes. This removes bookkeeping, not a semantic stage.

Cloc delta: **-41 non-test code lines, -13 test code lines**, net -54. One enum
and its derived-state transitions disappear; no new abstraction, cache, trait
or dependency is added. Tests retain their semantic assertions while removing
duplicate enum assertions. All four audit batches total **-134 non-test, +29
test, net -105 code lines**. IC instructions/cycles and raw Wasm total/code-section
changes remain unmeasured.

Evidence: `target/evidence/mega-audit-filter-20261111/`. Strict affected Core
library/test Clippy passes. All 22 selected tests pass: nine intent-state cases,
three planner residual-filter cases and ten session residual-filter cases.
They retain both append orders, full/partial exact-key pruning, accepted reads,
counts, windows/continuations, cache reuse, and mutation/SQL unknown-row behavior.
Formatting, documentation and diff checks pass. Broad workspace/release gates
and new macOS qualification were not selected; nothing was pushed or deployed.

## Overlay classification deletion

Further MA-08 tracing found that both production calls to the overlay classifiers
bind their result to an unused local. The journal classifier is immediately
followed by the real exhaustive `JournalRecord` match in
`prepare_online_batch_retirement`; derived-index retirement collects every
prepared key regardless of whether its value is present. Neither classifier
validates, mutates, or supplies a decision to those owners.

Delete the six-variant decision enum, both classifiers, discarded calls and two
tests that only mirrored those mappings. Keep the exhaustive recovery match,
position publication/admission, exact-versus-superseded retirement and existing
store/live merge behavior. No new dispatch owner, behavior axis, persisted
format or abstraction is introduced. This is a concrete deletion within MA-08,
not acceptance of the proposed multi-container redesign.

Cloc delta: **-48 non-test, -94 test code lines**, net -142. All five audit batches
total **-182 non-test, -65 test, net -247 code lines**. The classifiers may already
have been optimized away; no IC instruction/cycle or raw Wasm total/code-section
saving is claimed. Schema slot sorting remains unchanged: the current name-sorted
catalogue does not provide a borrowed slot-order view, and another cache would
add maintained state without an established net simplification.

Evidence: `target/evidence/mega-audit-overlay-20261111/`. Strict affected Core
library/test Clippy and 22 focused tests pass: four positioned-overlay cases,
four online-fold cases, thirteen recovery cases and the interrupted startup-fold
unique-key case. These retain exact/superseded positions, allocation refusals,
row/index reuse, rename and validation staging, stale catalog rejection,
migration-plan admission and replay/fold corruption boundaries. Formatting,
documentation and diff checks pass. Broad workspace/release gates and macOS
qualification were not selected; no network lifecycle action or delivery occurred.

## Remaining-slice access cursor

Further MA-04 tracing retains positional binding: executable traversal still
needs the independently lowered prefix/range specifications. However, its cursor
need not carry both the original slices and two offsets. `AccessSpecCursor`
now consumes checked prefixes and first elements from its remaining slices.
Failed reads leave the slices unchanged; copied cursors still provide independent
speculative traversal for exact-intersection admission and fallback.

One shared exhaustion check rejects either remaining slice. Remove the now-unused
count-comparison helper from `IndexRangeTraversalContract`; its missing-spec and
path-alignment checks remain. No new wrapper, cache, trait or behavior axis is
introduced. This simplifies the cursor representation without claiming to remove
the positional binding protocol or to change continuation identity.

Cloc delta: **-18 non-test code lines, 0 test code lines**. All six batches total
**-200 non-test, -65 test, net -265 code lines**. Two stored counters and their
update arithmetic disappear. IC instructions/cycles and raw Wasm total/code-section
effects remain unmeasured.

Evidence: `target/evidence/mega-audit-cursor-20261111/`. Strict affected Core
library/test Clippy and all 50 selected tests pass: 40 access-stream cases,
eight live/general intersection cases, secondary numeric range pagination,
and branch-distinct continuation coverage. Existing tests need no changes.
Formatting, documentation and diff checks pass. Broad workspace/release gates,
new macOS qualification and delivery were not selected.

## Reviewed disposition of the initial ten findings

The six implementation batches above are ready under their focused checks.
This is a closeout of the proven cleanup work, not exhaustive repository review,
release qualification or proof that the target architecture should be built.

| Finding | Current disposition | Evidence needed before further change |
| --- | --- | --- |
| MA-01 | Complete: obsolete diagnostic representation removed | New concrete consumer requirement |
| MA-02 | Complete: ordinary head derivation shared | None for this outcome |
| MA-03 | Complete: preflight topology borrows bundles | None for this outcome |
| MA-04 | Owned execution tree and positional binding protocol removed; leaf-owned bounds are native-focused-ready in 0.310 | Wasm/IC effects and allocation tradeoffs remain unmeasured |
| MA-05 | Query-admission ownership and direct SQL binding are native-focused-ready in 0.310; full expression and derived predicate retain distinct roles | See the current query-admission tracker; Wasm/IC effects remain unmeasured |
| MA-06 | Complete: canonical predicate hashing skips repeated normalization | A new raw predicate constructor would require rechecking the caller proof |
| MA-07 | Retain current slot ordering | An authoritative borrowed slot-order view, or measured evidence justifying retained indexing state |
| MA-08 | Unused classification removed; retain live/tombstone/provenance containers | A unified-entry traversal that preserves allocation-free merging and exact/superseded retirement with less machinery |
| MA-09 | Defer adapter specialization changes | Qualified linked one/ten-entity artifacts and IC instruction/cycle evidence |
| MA-10 | Retain fixture duplication for now | Qualification of both measurement subjects with identical inputs and accounted-for reachability changes |

MA-07's construction sorts `SchemaInfo.fields` by name for binary lookup;
`field_names_in_slot_order` derives a different order from the accepted slots.
Those orders serve distinct consumers. Replacing one with the other is not a
redundant-sort deletion, and adding a second stored vector is not established
as a net simplification.

MA-08's `OrderedOverlay` already shares the canonical/live merge across stores.
Its iterators borrow their inputs and the tombstone set, compare keys in either
direction, and choose live entries for equal keys. A unified entry would change
that iterator contract as well as provenance publication/retirement. No proposed
container merger has yet demonstrated lower total complexity.

MA-09/10's maintained actors remain members of `testing/canic-adapter`, and the
measurement policy selects that exact pair. The fixtures use different generated
profile types, not one interchangeable shared input type. Moving input setup to
a macro or generic helper would change the measurement subject without qualified
linked-artifact evidence. Existing Canic deferral remains with #373/#596; no new
build failure or fresh runtime qualification is claimed here.

This disposition review changes **0 non-test and 0 test code lines**. Source
checks were not rerun because no source changed. Documentation and diff checks
cover the updated map and disposition records. The next independent investigation
should target the inventory-only write/migration or aggregate/grouping owners,
rather than treating these conditional findings as mandatory deletion quotas.

## Follow-on migration and aggregate reductions

The maintainer authorized follow-on candidates 1–2. Migration final assembly
consumes `WorkingStore` after reconciliation finishes borrowing it. Its fallback
candidate moves four owned catalogue/snapshot fields instead of cloning them.
The ordered store iteration, untouched-store omission, reconciled-candidate
precedence, checked revision and typed errors remain. The earlier rekeyed-view
copies are retained because their inputs still have live consumers.

Aggregate `ingest_cow` now owns NULL, numeric and extrema ingestion. Borrowed
calls delegate to it, and ownership is acquired only for selected extrema.
Strict lint exposed that the owned adapter had no remaining production caller;
it was removed, with tests using the same Cow entrypoint as production. Numeric
coercion and canonical-order FIRST/LAST retain their existing separate owners.
New checks verify borrowed selection independence, moved string allocation and
owned/borrowed NULL exclusion from COUNT.

| Outcome | Non-test code delta | Test code delta | Complexity delta |
| --- | ---: | ---: | --- |
| Migration candidate ownership | +2 | 0 | Four final catalogue copies removed; no new representation |
| Aggregate ingestion | -31 | +39 | Two ingestion matches become one; unused owned adapter removed |
| Combined | **-29** | **+39** | Net +10 code lines; no new trait, cache, dependency or behavior axis |

Across all audit implementation: **-229 non-test, -26 test, net -255 code lines**.
These are cloc code counts excluding comments/blanks against each slice's
pre-edit working tree. IC cycles/instructions and raw Wasm total/code-section
effects remain unmeasured. The small test increase qualifies ownership behavior,
not a larger replacement abstraction.

Evidence: `target/evidence/mega-audit-follow-on-20261111/`. Strict affected Core
library/test Clippy passes after removing the unused owned adapter; the initial
failure log is retained. All 30 focused tests pass: 12 migration-planner cases,
six value-reducer cases, two grouped-reducer caller cases and ten scalar-terminal
preparation/DISTINCT cases. The migration cases cover rename identity, shared
named types, reconciliation and physical transforms; reducer cases cover ownership,
NULLs, numeric domain/overflow and typed variant refusal. Formatting,
documentation and diff checks pass. Broad gates, new macOS qualification,
network lifecycle actions and deployment were not selected.

## Grouped DISTINCT membership preflight

The sole production `insert_key` caller already probes borrowed canonical input
before budget checks and retained-key construction. Nothing between that probe
and insertion mutates the exclusively borrowed set. Remove the insertion helper's
second `contains` lookup, keeping fallible reservation before mutation and the
actual HashSet insertion/equality result with its existing caller invariant.

No new insertion API or proof type is necessary. Direct key-set tests may still
insert duplicates: the underlying HashSet returns false and retains its equality
contract. Those direct calls can now reserve capacity before reporting a duplicate;
production duplicates exit at the earlier borrowed probe before charging or
allocation. The existing direct tests remain valid and need no mechanical rewrite.

Cloc delta: **-3 non-test code lines, 0 test code lines**. One duplicate membership
preflight disappears, with no new behavior axis, collection, trait or abstraction.
All audit implementation totals: **-232 non-test, -26 test, net -258 code lines**.
IC instructions/cycles and raw Wasm total/code-section changes are unmeasured.

Evidence: `target/evidence/mega-audit-distinct-20261111/`. Strict affected Core
library/test Clippy and all 20 selected tests pass: fourteen key/equality/hash
cases and six DISTINCT admission/budget cases. They retain canonical Decimal/map
and U256 identity, forced collision equality, duplicates at full budget, total-
before-per-group refusal, and unchanged keys/usage on refusal. Formatting,
documentation and diff checks pass. No test source changed; broad gates, new
macOS qualification, network lifecycle changes and deployment were not selected.

## Schema lifecycle: ordered installation and borrowed catalogues

The two authorized candidates in [schema lifecycle](schema-lifecycle.md) are
implemented in the shared application lowering owner. Installation consumes
source-ordered store subsequences without sorting them again (-5 non-test lines);
its new multi-store permutation regression adds 68 test code lines. Borrowing
enum/composite catalogues removes two additive-construction copies and the owning
context constructor (-19 non-test lines, no test-line change). The source-binding
snapshot and existing-head sort retain their distinct validation/order purposes.

Combined delta: **-24 non-test, +68 test, net +44 code lines**. All audit
implementation totals: **-256 non-test, +42 test, net -214 code lines**.
Complexity decreases by one ordering pass, two catalogue copies and one helper;
there is no new representation, cache, generic framework or runtime fallback.
IC cycles/instructions and raw Wasm total/code-section changes are unmeasured.
Pre-edit source captures are under
`target/evidence/mega-audit-schema-lifecycle-20261111/`.

Strict Core lint with SQL/migration/metrics and 76 focused tests pass: 27 proposal
lowering, 47 additive entity creation/recovery and two collection-relation tests.
Formatting, documentation and diff checks pass. The initial offline lint attempt
found incoming locked Wire 0.21.2 absent from cache; fetching that exact selection
allowed locked validation without changing dependencies. Broad workspace/release
gates are skipped user-owned validation, not required by this continuation.

## Recovery: borrowed coalescing and admitted publication order

Both candidates in the [recovery review](recovery.md) are complete. Temporary
cardinality maps borrow prepared keys/values while preserving ordered
last-write-wins coalescing (zero code-line delta). Schema extraction uses the
admitted first publication instead of rescanning records and maintaining a
second optional candidate (-18 non-test code lines). A valid-checksum raw-byte
regression adds 38 test code lines for late/duplicate/mixed publications.

Combined delta: **-18 non-test, +38 test, net +20 code lines**. All audit totals:
**-274 non-test, +80 test, net -194 code lines**. Three cloning sites, a scan,
mutable optional state and repeated dispatch disappear; no new type, cache or
representation is added. IC cycles/instructions and uncompressed Wasm total/code
section changes remain unmeasured. Pre-edit source captures are under
`target/evidence/mega-audit-recovery-20261111/`.

Strict Core lib/tests Clippy with SQL/migration/metrics and 87 focused tests pass:
72 journal tests, nine schema-recovery tests, four Building-cardinality fold tests
and two Ready/source-drift tests. This includes the new authenticated raw-byte
regression and existing replay/fold contradiction tests. Formatting, documentation
and diff checks pass. Broad workspace/release gates remain skipped user-owned
validation. No package versions, dependencies, persisted formats or delivery
boundaries change in this cleanup.

## Relation preparation: one specification vector and borrowed validation

The [relation preparation reductions](relations.md) remove the duplicate field
specification vector/conversion and the descriptor wrapper/move-out method.
The existing component-spec type is reused; the validator borrows the existing
target contract. No new abstraction is added, and validation order remains intact.
Delta: **-10 non-test, 0 test code lines**. Cumulative audit delta: **-284 non-test,
+80 test, net -204 code lines**. Pre-edit sources are retained under
`target/evidence/mega-audit-relations-20261111/`.

Strict Core lib/tests Clippy with SQL/migration/metrics and 50 focused tests pass:
36 relation tests, ten nested write/recovery tests, two collection-relation tests
and two mixed-batch relation tests. Formatting, documentation and diff checks
pass. The initial offline lint attempt lacked incoming locked Runtime 0.21.2;
locked online validation fetched Runtime 0.21.2, Types 0.19.4 and Storage 0.73.4
without selecting upgrades. These incoming dependency edits are preserved.
Broad workspace/release gates remain skipped user-owned validation. IC cycles,
instructions and uncompressed Wasm total/code-section effects are unmeasured.

## Parameterized cache finalization

The [query-cache reduction](query-cache.md) shares final prepared-plan construction
and memo/cache publication between rebinding and first compilation. Logical
construction branches retain their allocation and charging order; exact-bound
reuse remains early. No new helper/type/cache is added. Delta: **-9 non-test,
0 test code lines**. Audit totals: **-293 non-test, +80 test, net -213**.
Wasm total/code-section and IC cycles/instructions remain unmeasured. Pre-edit
source is retained under `target/evidence/mega-audit-query-cache-20261111/`.

Strict Core lib/tests Clippy with SQL/migration/metrics and 18 focused tests pass:
eight planner-watermark, three template-accounting, three parameterized-query,
retained-cache accounting, bound-SQL reuse, verbose Hit/Miss and cardinality
lifecycle checks. Formatting, documentation and diff checks pass. Broad
workspace/release gates remain skipped user-owned validation. No dependency,
package-version or delivery changes are made by this cleanup.
