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
`build_logical_plan` in `query/plan/logical_builder/mod.rs`. Its two production callers
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

## DDL admission handoff reduction — 2026-11-12

Following the [schema lifecycle removal investigation](schema-lifecycle-removal.md),
SQL DDL publication consumes the admitted operation instead of deriving another
operation from snapshot differences. The delta and additive transition-plan enums
are removed. The surviving catalog mutation request admits supported additions
and historical fills once; index targets are constructed once per classification.
The version/fingerprint admission policy remains separate and intact.

Each derivation now retains its source schema version and existing runtime
fingerprint. Publication checks the supplied before snapshot and identity against
that proof before staging, and retains the live catalog identity check at commit.
Required-field addition retains the exact-empty-entity check. Persisted decoding,
physical index staging and interrupted publication recovery are unchanged.

Transition behavior tests moved to the mutation admission owner. New regressions
reject mismatched source shapes/versions and prove a rejected publication leaves
rows, indexes and accepted identity unchanged. The change removes **67 production
code lines** and adds **82 test code lines** (net **+15**), counted with the installed
cloc over all affected Rust files, including moved/untracked files, against the
pre-edit HEAD. Production Rust was clean at task start. The reduction is two
intermediate operation representations, one publication reclassification pass and
an unused exact-match request variant; no new framework or cache is added.

The 139 selected mutation, publication/recovery, SQL field, activation and historical
row tests, strict Core library Clippy with SQL/migration/metrics, formatting and
diff checks pass. Wasm total/code-section bytes and IC instructions/cycles are
unmeasured; fingerprint binding is additional work, so no runtime saving is claimed.
Broader tests, release gates and macOS qualification remain unselected user-owned
validation. No package version, commit or deployment is performed by this slice.

## Immutable candidate preparation reduction — 2026-11-12

Candidate preparation now trusts the immutable bundle admitted by its constructor
or bounded persisted decoder. The bundle fields are private, the two production
construction paths validate, and no mutable catalog/snapshot access escapes.
Encoding no longer invokes whole-bundle validation again. Preparation no longer
decodes and compares its freshly encoded bundle and root. Bounded encoding,
semantic and wire hashes, root validation, persisted reconstruction, live identity
checks and publication/recovery behavior remain in place.

Component encode/decode checks in the bundle constructor remain: this slice does
not establish that each can be removed without losing a component-level admission
rule. Candidate resource accounting drops the deleted decoder byte visits while
retaining the hash visit and operation-wide construction allowance. No cache,
new proof type, unchecked production constructor or format change is introduced.

Tests now feed invalid construction data through the maintained bundle constructor,
rather than mutating private fields and pretending the result is already accepted.
Malformed persisted inputs still reach the independent decoder. Projection expansion,
constructor/recovery equivalence, exact encoding and shared-budget refusal remain
covered. The 154 focused catalog, store, entity-creation and publication/recovery
tests pass. Strict Core library/test Clippy with SQL/migration/metrics and
formatting checks also pass.

Task-relative delta: **-16 production / +11 test code lines (net -5)**, measured
with installed cloc against `/tmp/icydb-candidate-before` and all three changed
Rust files. Complexity reduction: one repeated whole-bundle validation invocation
and two self-decoding passes removed, with no replacement mechanism. Wasm total
and code-section bytes and IC instruction/cycle effects remain unmeasured.
Broad workspace/release gates and macOS qualification are unselected user-owned
validation; no package version or delivery action is included.

## Borrowed migration reconciliation — 2026-11-12

Migration reconciliation no longer constructs temporary accepted bundles from
its unpublished working catalogs. A concrete borrowed `SchemaCatalogView` supplies
read-only input to ordinary lowering; accepted bundles lend the same view on the
ordinary path. It is neither runtime authority nor a mutable overlay/cache.
Final candidate construction and publication still require accepted bundles.

Catalog validation and semantic fingerprinting have one implementation over these
borrowed parts. Both immutable bundle construction and migration reconciliation
retain their validation boundary, and persisted decoding retains its component
and cross-component checks. The migration failure remains `RekeyedCatalogInvalid`
when rekeyed input is not closed. A regression removes a working snapshot while
retaining its source binding and verifies rejection before ordinary lowering.

Source-lineage planning now consumes source bindings directly. Migration passes
rekeyed bindings, preserving identity when a new entity reuses a predecessor's
name; installation passes an empty iterator. No intermediate owning bundle vector
or optional-bundle vector is needed for this handoff.

This slice adds **32 production / 17 test code lines (net +49)**, counted with
installed cloc against `/tmp/icydb-migration-before` across all 11 affected Rust
files. The explicit borrowed input adds one small type but removes a full owning
catalog-copy stage: enum catalog, composite catalog, source bindings, entity
snapshots and store-path ownership no longer have to be duplicated merely to
enter reconciliation. There is no new policy owner, durable state or persisted
format. This is a reduction in ownership/transformation work, not a LOC reduction.

The 258 selected migration, application, ordinary-lowering, catalog-publication
and interrupted-publication tests pass. Strict Core library/test Clippy with
SQL/migration/metrics, no-default-feature library compilation, formatting and diff
checks pass. The exercised application cases include predecessor-name reuse,
shared-type renames, staged activations, physical transforms and terminal replay.

Migration-specific target rebuilding, ordinary candidate assembly and their final
candidate selection remain distinct. This slice does not claim that those larger
mechanisms have been consolidated. Wasm total/code-section bytes and IC
instructions/cycles remain unmeasured. Broad workspace/release gates and macOS
qualification remain user-owned; no package version or delivery is selected.

## Migration finalization bookkeeping reduction — 2026-11-12

Migration finalization now consumes the ordinary lowerer's store-ordered candidates
directly. Both streams originate from store-path `BTreeMap` traversal, so rebuilding
a second map with copied path strings adds no ordering or lookup authority. The
ordered iterator preserves reconciled-candidate precedence, fallback finalization
and rejection of unmatched candidates.

Working stores now retain a `has_transitions` flag instead of a set of transitioned
entity IDs: its only consumer needs to distinguish an unchanged store from one
requiring a candidate. This does not change migration record/progress state.

The shared-type migration fixture now also creates a fresh entity in one store,
leaving the other on fallback finalization. Reversed input order must still produce
one candidate per store in canonical order with preserved type identities and
correct entity counts. The slice removes **1 production code line** and adds
**24 test code lines** (net +23), measured with installed cloc against
`/tmp/icydb-finalization-before`. The useful reduction is two owning bookkeeping
collections; no new helper, policy type or cache is introduced.

The 190 selected migration-planner and schema-application tests pass, including
the mixed-store regression. Strict Core library/test Clippy with SQL/migration/
metrics, no-default-feature library compilation, formatting and diff checks pass.

The two finalization paths remain because they retain distinct failure
classifications. Merging them without carrying that distinction would silently
change migration diagnostics; this cleanup does not add an error abstraction
merely to eliminate the branch. Wasm total/code-section bytes and IC
instructions/cycles remain unmeasured. Broad/release gates and macOS validation
remain unselected; package versions and delivery are unchanged.

## Shared schema draft rewrite — 2026-11-12

The maintainer authorized replacing the parallel construction paths in this
session. `SchemaDraft` now owns the only mutable store-local snapshots, value
catalogs and source bindings. Migration transforms and ordinary reconciliation
modify that same draft; one consuming finalizer creates the accepted candidate.
`WorkingStore`, `ExistingStoreCandidateState`, their second whole snapshot-map
copy and reconciled-versus-fallback candidate selection are removed.

Reconciliation first resolves immutable proposal routing, entity identities,
relation targets and activation provenance against the complete input catalog.
These inputs contain neither snapshots nor owned value catalogs. Named-type
validation now checks exact definitions without returning cloned catalogs.
Unchanged stores and stores changed only by migration still participate in
namespace validation and canonical publication order. Generated ingress keeps
explicit-removal code statically unreachable; explicit application and migration
retain that preparation step.

The two old finalizers' semantic error classifications are preserved using the
draft's ordinary-change flag, rather than keeping competing candidate builders.
Rekeyed catalog closure remains checked before reconciliation. Source-lineage
planning captures existing proposal source keys before an addition can reuse a
renamed predecessor's key. Activation fingerprints are pinned before mutation
for existing constraint-bearing declarations. Physical rewrite programs,
forward-reference passes, durable migration phases, publication and recovery
contracts are unchanged.

This supersedes the retained intermediate mechanisms described in the preceding
two implementation entries. Measured against the pre-rewrite source, installed
cloc reports **+107 production code lines and +9 test code lines (net +116)**.
This is not a net source deletion. The structural reduction is two mutable
construction representations to one, two finalizer implementations to one, and
one fewer whole snapshot/catalog copy in migration reconciliation. The added code
makes immutable reconciliation inputs explicit; it introduces no persisted state,
cache, compatibility fallback or general-purpose mutation framework.

Qualification: 237 selected tests pass across migration planning (13), schema
application (177), ordinary lowering (27), physical transform compilation (2),
catalog publication (12) and interrupted schema publication (6). Assertions now
also pin activation source fingerprints/epochs and mixed-store candidate revision
and persisted round-trip equality. Strict Core library/test Clippy with
SQL/migration/metrics, no-default-feature library compilation and formatting
checks pass. Full workspace/release gates
and macOS qualification were not selected. Wasm total/code-section bytes and IC
instructions/cycles remain unmeasured; no runtime improvement is claimed.
Package versions and deployment remain untouched.


## Replacement-boundary review against Toko Tooling 0.39.19

The [published replacement guide](https://github.com/dragginzgame/toko-tooling/blob/a6f3c63b9cf7a2547d3302369bce855676544722/docs/principles/subsystem-replacement.md)
clarifies completion of the preceding work. Its bounded responsibility was
store-local draft ownership and candidate finalization: both callers now use
`SchemaDraft`, and the two old mutable owners and candidate fallback are deleted.
The 237 focused tests qualify that boundary, not a wholesale schema-lifecycle
replacement. The +116 code-line result is not evidence of code reduction.

The original migration-target finding is broader. `rebuild_transitioned_snapshots`
still builds migration snapshots in field/index, relation and constraint passes;
ordinary `lower_existing_entity` still performs its own reconciliation assembly.
The shared draft removes duplicated ownership, but does not itself prove these
remaining constructors can be collapsed. A further replacement should establish
one target-snapshot construction responsibility and retire demonstrably equivalent
assembly paths together. Identity reservation and forward-reference resolution
remain required; migration policy, row-program compilation and durable recovery
are distinct responsibilities, not reasons to rewrite the entire schema directory.

This review adds guidance only. It does not initiate another rewrite, promise a
line/size reduction, or fold the persisted migration-phase hard cut into the
completed internal change. Any continued replacement should use the existing
finding's protected behavior and explicit deletion criteria, with one integrated
handoff rather than another sequence of helper extractions.

## Target-snapshot construction replacement — 2026-11-13

The maintainer authorized the next replacement after the 0.39.19 boundary review.
The coherent responsibility is materializing target entity snapshots from already
resolved field/object identities and source-bound constraint semantics. Its callers
are initial creation, existing-schema reconciliation and migration target planning.
Physical transformation admission, ordinary rejection of unexplained differences,
staged activation, row-program compilation and durable recovery remain distinct
policy owners. Merging those policies would change the protected contract.

The replacement uses the existing `PersistedSchemaSnapshot` payload owner for
structure changes and consumes snapshots through completion. Its structure update
copies only retained components when the accepted payload is shared; uniquely
owned drafts update in place. Reconciliation retains its accepted comparison but
constructs no detached snapshot for unchanged fields/names, no replacement field
or index vectors for unchanged facts, and no second index projection when fields
are unchanged. The mandatory accepted-index comparison remains.

Initial creation now completes its single snapshot map after reserving all field
identities. Migration establishes all renamed paths, field/index contracts and
relation identities first, then completes relations and row constraints together.
Row constraints depend on their own completed edges and shared value catalogs,
not on another entity's row constraints. This removes the third traversal while
retaining cross-entity forward references and canonical ordering.

Initial creation and physical migration also share row-constraint binding and the
catalog's active-row insertion boundary. The former migration-only compiler and
active check/targeted-rule constructors are deleted; their direct test callers now
use the maintained constructor with the same origins, kinds and error assertions.
SQL/generated ownership and allocator checks remain catalog-owned. Staged
activation constructors remain separate because their publication contract differs.

Deleted mechanisms: `rebuild_existing_entity_snapshot`, `rebuild_snapshot_shell`,
`RebuiltMigrationFields`, `install_reserved_migration_relation_ids`,
`compile_migration_constraint_kind`, `with_added_check`, `with_added_targeted_rule`,
the second initial snapshot map, and the standalone migration constraint pass.
There is no compatibility path, replacement framework or additional persistent
state. Remaining migration and reconciliation loops implement different admission
policies; the equivalent field/index/relation binders and row-constraint builder
are shared. This completes the bounded construction replacement, rather than
claiming that all schema-lifecycle policy or the deferred persisted migration
phase can disappear.

Installed cloc against the pre-replacement file snapshots reports **83 production
code lines removed and 180 test code lines added (net +97)**. Test growth includes
mechanical migration of the retired constructor callers, preservation of pending
owners/bytes for shared and uniquely owned snapshots, and mutual cross-store
references between renamed entities. No feature or behavioral assertion was
removed to obtain the reduction.

**Qualification:** 399 focused tests pass: migration planning (13), ordinary
lowering (27), schema application (177), snapshot behavior (33), constraint catalog
(7), check evaluation (79), schema codecs (41), targeted-rule writes (2), physical
transform compilation (2), catalog publication (12), and interrupted schema
publication (6). Strict Core library/test Clippy with SQL/migration/metrics,
no-default-feature library compilation, formatting and diff checks pass. The
expanded cross-store fixture initially reversed the migration API's current/prior
source keys; it was corrected without changing production admission or weakening
assertions. All listed results are from the final source.

Broad workspace/release gates and macOS qualification were not selected. Wasm
uncompressed/code-section bytes and IC cycles/instructions remain unmeasured;
structural copy/pass removal is not a measured runtime-performance claim. Package
versions, commits and deployment remain untouched.


## Direct physical-migration completion — 2026-11-13

The authorized 0.311 hard cut removes the accounting-only rebuild phase. Final
rewrite effects, completion counts and the cursor reset enter final validation
through one journaled record replacement. Update the public phase API, Core/CLI
callers and current record envelope together; no compatibility branch remains.
Keep pre-rewrite abortability, bounded cleanup and final proof distinct.

Net Rust source delta for this slice: non-test **-12**, test **+60** (net **+48**,
including comments/blank lines). Complexity falls by one public/persisted state,
one dispatch arm and one continuation/plan recompilation. No replacement type,
cache or stored representation is added. Counting still uses existing bounded
projection preparation; this is a state-machine simplification, not a large
source deletion. Wasm total/code-section bytes and IC cycles/instructions are
unmeasured. Focused qualification is recorded in the
[0.311 tracker](../../design/0.311-migration-completion/status.md).


## Metadata-only migration domains — 2026-11-13

The authorized second 0.311 slice shares candidate-generation selection between
row projection, completion counting and abort cleanup. A single domain visitor
streams logical counts or feeds cleanup's existing ordered maps/sets. It removes
the owned domain-record/vector representation and duplicate sorting/deduplication;
logical counts still precede physical namespace coalescing.

Counting and cleanup no longer construct row contracts, compiled constraints,
user-index projections or relation-row projections. Keep exact candidate/head
recompilation, snapshot admission, generation guards, live target/source authority,
capability checks and all-binding resolution before storage deletion. Row paths
retain full semantic projection. Remove the now-unused index identity field and
projection identity accessors. No public or persisted contract changes here.

This slice adds 16 non-test and 38 test Rust source lines (including comments and
blank lines), net +54. It reduces construction dependencies and temporary
representations rather than total source. No Wasm/IC performance benefit is claimed:
uncompressed total/code-section bytes, cycles and instructions are unmeasured.
Qualification is recorded in the [0.311 tracker](../../design/0.311-migration-completion/status.md).
