# Findings and architectural verdict

## Executive assessment

The current code has substantial complexity, but it is not accurate to describe
it as several wholly separate query or mutation engines. Important convergence
has already happened: structural writes share a coordinator, physical access
shares traversal, predicate normalization shares recursion, and schema proposal
lanes share candidate construction. Repeating those consolidation projects would
add churn without removing another owner.

The more credible remaining opportunities are **reducing carried representations
and repeated derived work**, then removing obsolete diagnostic machinery.
Two especially concrete observations are a duplicate accepted-head computation
and an executor detail enum with ten variants that have no explicit construction
sites in the searched repository. Larger proposals need semantic and measured
qualification before deletion claims.

This is a first investigation, not the exhaustive verdict requested by the
[brief](brief.md). [Coverage](inventory.md#coverage-limits) records the remaining
work. No tests or performance experiments were run. No issue is closed by this
report, and no independently verified whole-subsystem deletion is established.

## Ranked opportunities

Ranking combines benefit, evidence and practical next-step value, not file size.
IDs are references within this report, not a separate work-status queue.

| ID | Programme | Opportunity | Evidence / confidence | Risk |
| --- | --- | --- | --- | --- |
| MA-01 | A | Retire the parallel executor diagnostic-detail representation | Repository construction search; medium pending public-consumer review | Medium |
| MA-02 | A | Derive the ordinary schema application head once | Same function, same arguments, no intervening effects; high | Low |
| MA-03 | B | Use borrowed candidate topology during schema-domain preflight | Owned bundle copies are visible; medium | Medium |
| MA-04 | C | Remove the reconstructed executable access tree and positional spec coupling | Both representations and offset protocol exist; medium on opportunity, low on replacement | High |
| MA-05 | C | Narrow the dual expression/predicate filter representation | Coverage state and conversion path verified; low on safe elimination | High |
| MA-06 | B | Hash already-canonical predicates without copying and renormalizing | Extra pass exists; caller-wide canonical proof incomplete; medium | Medium |
| MA-07 | B | Avoid repeated field-slot ordering projections | Sorting method and multiple callers verified; medium | Medium |
| MA-08 | C | Challenge separate overlay value, tombstone and provenance containers | Three-container synchronization verified; low on beneficial replacement | Critical |
| MA-09 | C | Reduce generated adapter instantiations only where Wasm proves a cost | Generic generated context boundary verified; low on actual growth | High |
| MA-10 | A/B | Consolidate repeated measurement-fixture input setup | Text and call structure verified; medium; preserve independent subjects | Low semantic / medium measurement |

### MA-01 — Retire parallel executor diagnostic details

**Owners:** Core `error::ExecutorErrorDetail`, `ErrorDetail::Executor`, and the
diagnostic-code crate's `RuntimeBoundaryCode`.

Evidence:

- [Core error owner](../../../crates/icydb-core/src/error/mod.rs) declares eleven
  `ExecutorErrorDetail` variants and repeats their mapping to runtime boundary
  codes, Debug rendering and empty facts.
- Searching `ExecutorErrorDetail::` across `crates`, `qualification`, `testing`
  and `canisters` finds only the explicit construction of
  `AcceptedRowConstraintProgramCorrupt` in the owner. Self-qualified mappings
  inside the enum's implementation are not construction callers.
- `mutation_batch_empty`, duplicate-key and limit constructors already use
  `mutation_boundary_with_facts` with canonical codes from
  [the registry](../../../crates/icydb-diagnostic-code/src/lib.rs).
- Core exports `pub mod error`; absence of local callers does not prove external
  API deadness. Public manual construction and generated consumers remain to
  be checked before applying the hard cut.

**Proposal:** after that consumer check, move the one remaining internal
constructor onto the existing canonical diagnostic mechanism, preserving class,
origin, code and facts. Remove the executor enum and its `ErrorDetail` arm,
without aliases or a new registry framework. If a retained API use is found,
propagate it explicitly rather than silently dropping its diagnostics.

**Net code estimate:** 80–140 non-test physical lines removed after replacement;
test delta unknown until diagnostic qualification is selected. This is a range
from the enum, implementation and mapping blocks, not measured cloc.
**Complexity delta:** one enum and one outer detail branch removed; one
executor-to-registry mapping eliminated; no new trait/module/dependency.
**Effort:** one bounded slice. **Required proof:** typed public code/detail/facts,
Debug/error projection, class/origin preservation, and current constructor
coverage. Wasm and IC impact unknown; no persistence format change proposed.

### MA-02 — Compute the ordinary accepted head once

**Owner:** `apply_schema_with_contract` and `accepted_head_after_candidates` in
[schema application](../../../crates/icydb-core/src/db/schema/application.rs).

In the non-pending, non-empty-candidate branch, `accepted_head` calls
`accepted_head_after_candidates(authorities, candidates)`. Immediately afterward,
`lineage_head` calls the same function with the same arguments. The helper walks
stores, finds candidates, reads untouched store roots, allocates a vector and
hashes it. No publication occurs between these two calls.

**Proposal:** derive the actual lineage head once and reuse it for the ordinary
receipt head. Retain a distinct future head for deferred constraint activation;
those two heads intentionally differ. Preserve the empty-candidate contract and
fallible root checks.

**Net code estimate:** roughly 0–10 non-test physical lines removed, test code may
increase. **Complexity delta:** one duplicated derivation on the ordinary path;
zero types/modules/dependencies added. This is worthwhile despite a tiny LOC
delta. **Effort:** small bounded slice. **Required proof:** ordinary application,
no-op/replay, and pending activation where receipt and lineage heads differ.
No durable format change. Allocation/traversal removal is visible; instruction
and Wasm savings are unmeasured. This is the safest first implementation slice.

### MA-03 — Borrow the accepted-after topology in preflight

**Owner:** `preflight_new_entity_domains` in
[schema application](../../../crates/icydb-core/src/db/schema/application.rs).

The function copies `current_bundles` with `to_vec()`, then replaces matching
entries with cloned candidate bundles. It uses that assembled `after` collection
to find target stores for reverse-relation emptiness checks. The source bundles
and candidates remain borrowed inputs throughout the preflight.

**Proposal:** resolve candidate-over-current topology through borrowed bundles,
or a small local borrowed slice if that is simpler for the existing helper.
Do not add a general overlay framework. First inspect the full contract and all
callers of `accepted_entity_store_for_path`; absence and replacement semantics
must remain identical.

**Net code:** not yet estimated; this may reduce owned state rather than lines.
**Complexity delta target:** remove one owned snapshot projection and its
replacement loop; no new durable representation. Zero dependency change.
**Effort:** one bounded slice. **Required proof:** cross-store relation targets,
new entity collisions, missing targets, candidate replacement and reverse-index
domain rejection before publication. Risk is choosing the old target authority,
not merely allocating extra memory. Bundle-copy volume and IC/Wasm impact are
unmeasured. Depends on no other finding; overlaps MA-02 only in source file.

### MA-04 — Challenge the second access tree and positional binding protocol

**Owners:** [executable access](../../../crates/icydb-core/src/db/access/execution_contract/executable.rs),
[execution payload](../../../crates/icydb-core/src/db/access/execution_contract/types.rs),
[stream bindings](../../../crates/icydb-core/src/db/executor/stream/access/bindings.rs).

`ExecutableAccessPlan::from_access_plan` recursively builds another union/
intersection tree, allocating child vectors while borrowing leaf keys.
`ExecutionPathPayload` strips planner details. Separately, `AccessSpecCursor`
consumes lowered prefix/range arrays through two offsets and checks that every
spec was consumed. The tree and side arrays must agree on traversal order.

**Proposal to investigate:** lower once to executable leaves that directly own or
borrow their exact physical specs, or use a borrowed executable view where that
avoids the second tree. Keep planner-only semantics outside runtime execution.
These are alternatives, not a mandate to introduce both.

**Net code:** unknown until route and lifetime propagation is designed; the
whole access subsystem is not a deletable estimate. **Complexity delta target:**
one tree representation and/or one positional synchronization protocol removed,
including two cursor offsets and end-consumption checks. Count any added leaf
types and retained metadata against the reduction. **Effort:** multi-boundary
slice requiring further design. **Required proof:** union/intersection,
multi-prefix and branch ordering, empty ranges, ASC/DESC, resume boundaries,
covering projection, resource refusal, and missing-row behavior. Compare raw
Wasm total/code bytes and IC instructions before removing specialization.
No persistent data-format change intended; continuation identity is a contract
that must be explicitly held constant or deliberately cut. Risk high.

### MA-05 — Reduce filter representation synchronization

**Owners:** [intent state](../../../crates/icydb-core/src/db/query/intent/state.rs),
[expression-to-predicate compilation](../../../crates/icydb-core/src/db/query/plan/expr/predicate/compile.rs),
[planning pipeline](../../../crates/icydb-core/src/db/query/plan/pipeline.rs).

`NormalizedFilter` carries semantic authority (expression-backed or predicate-
only), an optional predicate subset, and Full/Partial/None coverage.
`derive_normalized_bool_expr_predicate_subset` can reject unsupported shapes
without rejecting the expression. Planner preparation then schema-normalizes
the predicate. This is real representation multiplicity, but the predicate can
be an access restriction rather than the full SQL truth condition.

**Proposal to investigate:** retain one full semantic filter and make the
indexable restriction a narrower planner result, with one owner for the coverage
proof. First trace predicate-only callers and append/composition semantics.
Do not replace SQL expressions with predicates: that would lose supported
expressions and three-valued semantics. Do not delete current admission before
proving that the earlier stage has equivalent accepted-schema authority.

**Net code:** unknown; no defensible large deletion estimate yet.
**Complexity delta target:** reduce independently synchronized semantic forms
and coverage transitions, without adding another general query IR.
**Effort:** substantial semantic slice after caller tracing. **Required proof:**
SQL/typed/dynamic parity, NULL/missing, enums/coercions, partial extraction,
non-indexable expressions, cache identity, parameter rebinding and continuations.
Overlaps MA-06; savings cannot be summed independently. IC/Wasm impact unknown.

### MA-06 — Reuse canonical predicate proof during hashing

**Owners:** [predicate fingerprint](../../../crates/icydb-core/src/db/predicate/fingerprint.rs),
[query hash sections](../../../crates/icydb-core/src/db/query/fingerprint/hash_sections/mod.rs),
[query cache](../../../crates/icydb-core/src/db/session/query/cache.rs).

`hash_predicate` copies and normalizes before structural encoding. Planning
already has a normalized predicate, and the cache uses the separate
`predicate_fingerprint_normalized` entrypoint. Query semantic hashing uses the
expression when present, with predicate hashing as its fallback. Therefore the
extra pass is not proven to occur on every query or cache hit.

**Proposal:** audit every fallback caller, then supply a canonical predicate to
the structural hash owner where that proof already exists. Preserve separate
BLAKE3 cache and SHA-256 continuation identities; different identity purposes
are not duplicate bugs. Avoid a publicly forgeable normalized marker.

**Net code estimate:** 0–30 non-test physical lines if existing proof suffices;
could grow if a new proof type is needed, in which case reassess benefit.
**Complexity delta:** remove one copy/normalization pass at proven callers,
not the shared normalizer or an identity domain. **Effort:** bounded after
caller proof. **Required proof:** identical canonical hashes, unordered input,
coercion distinctions, exhausted construction budget, predicate-only queries,
cache miss/hit and continuation mismatch. Runtime savings remain unmeasured.

### MA-07 — Reconsider repeated accepted-field slot ordering

**Owner:** `SchemaInfo::field_names_in_slot_order` and its admitted wrapper in
[schema info](../../../crates/icydb-core/src/db/schema/info.rs).

Every call collects `(slot, name)` pairs, sorts them and collects names. Current
callers include SQL wildcard projection, exact-key projection and covering
planning. The preparation wrapper correctly charges the shared implementation;
it is not a second sorting implementation.

**Proposal:** prefer borrowing the accepted layout's existing slot ordering if
available through the current authority. Only consider a cached slot index if
its retained-memory cost and invalidation are demonstrably better. Adding a
second warmed field catalogue would contradict the objective.

**Net code:** unknown until the owning layout API is traced. **Complexity delta
target:** remove one per-call derived ordering and its scratch-accounting path;
count any cache/index and invalidation state added. **Effort:** bounded
investigation then local slice. **Required proof:** dropped/noncontiguous slots,
renames, wildcard order, nested fields, physical layout evolution and budget
exhaustion. No persisted change proposed. IC/Wasm impact unknown.

### MA-08 — Overlay containers are a redesign hypothesis, not dead caches

**Owners:** [data store](../../../crates/icydb-core/src/db/data/store.rs),
[index store](../../../crates/icydb-core/src/db/index/store.rs),
[position metadata](../../../crates/icydb-core/src/db/positioned_overlay.rs),
[ordered merge](../../../crates/icydb-core/src/db/ordered_overlay.rs).

Journaled stores keep live values, tombstones and positions in separate
containers. Position metadata intentionally does not become a second read
authority; exact versus superseded retirement prevents an older batch deleting
a newer effect. `OrderedOverlay` already shares canonical/live merge behavior.

**Proposal to investigate:** a single overlay entry containing an upsert/delete
state and journal position could remove key duplication and synchronized map
updates. Reject it if it broadens reads, complicates streaming, or replaces an
allocation-free traversal with materialization. Do not remove provenance.

**Net code:** unknown. **Complexity delta target:** three synchronized containers
to one per family, offset by any new enum, iterator adaptation and proof states.
**Effort:** large recovery-sensitive design, not a cleanup patch. **Required
proof:** repeated writes/deletes, superseding batches, interrupted fold/replay,
allocation identity mismatch, exact retirement, forward/reverse traversal and
corrupt journals. Durable journal format should remain unchanged; any change
needs an explicit retained-data plan. Critical risk; IC/Wasm effects unknown.

### MA-09 — Generated adapters require reachability evidence

**Owners:** [typed adapter generator](../../../crates/icydb-model-macros/src/node/typed_adapter.rs),
[composite-key generator](../../../crates/icydb-model-macros/src/node/entity.rs),
[measurement policy](../../../qualification/build/src/wasm_measurement.rs).

`named_adapter_impl_tokens` generates input/output methods generic over
`TypedAdapterContext`; records, enums and transparent wrappers compose them.
This is a plausible instantiation multiplier, but it also preserves the
model/runtime boundary. Source genericity is not proof of linked Wasm growth.

**Proposal:** use the maintained one/ten reachable-entity pair to identify which
adapter instantiations survive linking. Move only repeated non-type-specific
work behind an existing concrete runtime owner. Retain per-type dispatch where
it is smaller or cheaper. Do not couple host-only schema generation back to
the runtime to remove a trait.

**Net code/complexity:** unestimated until linked symbols establish candidates;
success means fewer runtime instantiations/transformations without an extra
adapter layer. **Effort:** measurement first, then bounded generator slice.
**Required proof:** named enums/payloads, records, containers, composite keys,
typed binding failures, one/ten entity raw Wasm total/code-section deltas and
actual IC instructions. Existing composed fixtures depend on Canic; no fresh
measurement is claimed. Links to existing scope: [#19](https://github.com/dragginzgame/icydb/issues/19)
and [#112](https://github.com/dragginzgame/icydb/issues/112).

### MA-10 — Share fixture input definitions, retain separate measurements

**Owners:** [one-entity actor](../../../canisters/audit/one_entity_reachable_operations/src/lib.rs)
and [ten-entity actor](../../../canisters/audit/ten_entity_reachable_operations/src/lib.rs).

The pair repeats the nested structural input setup and unbound-input rejection
shape; operation execution already uses the shared
`execute_simple_reachable_entity_operation!` macro. The ten-entity dispatch is
intentional: it establishes additional reachable instantiations.

**Proposal:** consider sharing only identical input construction in the existing
fixture helper owner. Do not merge the canisters or abstract away entity
reachability. Prefer retaining this small duplication if the helper changes the
measurement subject or adds another generic dispatch mechanism.

**Net code estimate:** 10–30 test/fixture physical lines after helper overhead;
non-test product delta zero. **Complexity delta:** two fixture-data definitions
to one, at the cost of one helper; no product mechanism disappears.
**Effort:** small slice, but defer until the pair can be qualified together.
**Required proof:** same input values and reachable operations; matched artifact
comparison must account for any linking change. No test coverage deletion or
dependency removal is justified. Wasm/IC effects unknown.

## Apparent duplication to retain

1. [Storage opening policy](../../../crates/icydb-core/src/db/storage.rs): its
   short create/load branch enforces virgin-memory versus retained-data behavior.
   Inlining it everywhere would duplicate policy.
2. [Predicate normalization](../../../crates/icydb-core/src/db/predicate/normalize.rs):
   charged and retained paths share `normalize_with_work`; admission differences
   do not establish two normalization engines.
3. [Structural mutations](../../../crates/icydb-core/src/db/executor/mutation/structural.rs):
   accepted insert/replace/update/delete intent converges here. `ExpectedLoaded`
   deliberately avoids another backend read. Do not add a second generic write
   framework.
4. [Schema lowering](../../../crates/icydb-core/src/db/schema/application_lowering.rs):
   generated and explicit ingress share `lower_existing_store_candidates`.
   Generated ingress rejects removals and can avoid retaining removal machinery;
   collapsing the two entrypoints may increase linked Wasm.
5. [Fast-stream route](../../../crates/icydb-core/src/db/executor/scan/fast_stream_route/handlers.rs):
   it verifies eligibility then calls the canonical structural stream executor.
   It is not an independent traversal engine.
6. [Runtime root identity](../../../crates/icydb-core/src/db/schema/runtime_root.rs):
   incarnation, per-store roots and explicit absence protect authority across
   changes. A maximum revision alone is not equivalent.

## Dead-code and dependency verdict

MA-01 is **potentially externally consumed**, with locally obsolete variants;
it is not globally proven unreachable. MA-10 is **redundant fixture code**, not
obsolete fixture coverage. No other production deletion or unused dependency
is proven in this pass. The existing local Testkit isolation must not be counted
again as an audit saving. Deferred Canic fixtures remain live acceptance assets,
not abandoned code merely because their dependency graph is behind.

The requested five low-risk deletions cannot honestly be supplied from this
evidence. MA-02 is a high-confidence repeated-work removal; MA-01 and MA-10
remain conditional. Further semantic/consumer tracing is necessary.

## Architectural verdict

**Removable percentage:** not established. There is no defensible repository-wide
percentage from this pass. The inventory is physical text, several findings
overlap, and the large opportunities have not demonstrated behavioral or Wasm
equivalence. Do not turn the size of an affected module into a deletion budget.

**Five decisions most worth challenging:** duplicated access representation with
side arrays; dual filter authority/subset bookkeeping; separate overlay
containers; repeatedly projected schema metadata; generic typed adapter context
instantiations. These are ranked design questions, not five confirmed mistakes.

**Three fresh-design candidates:** executable access lowering, semantic filter
representation, and live overlay storage. Keep schema authority and commit/
recovery invariants fixed while testing alternatives.

**Retain:** accepted snapshots as authority; bounded fallible decoding; separate
proposal/runtime contracts; provenance-aware retirement; exact continuation
identity; specialized streaming and covering paths until measured otherwise;
independent tests at distinct trust and recovery boundaries.

**Smallest credible end state:** one semantic filter with planner-derived access
restrictions; one executable access contract feeding specialized shared streams;
one catalog-native schema publication owner; one structural mutation coordinator;
storage-owned collection formats with IcyDB-owned journal/authority policy;
minimal generated typed adapters. This is a direction, not a new framework.

The [programme](programme.md) gives the first slices and the measurements needed
to promote hypotheses into implementation proposals.
