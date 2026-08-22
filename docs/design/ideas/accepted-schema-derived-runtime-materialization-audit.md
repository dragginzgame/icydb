# Accepted-Schema Derived Runtime Materialization Audit

[Observed] Status: bounded architecture and measurement audit against restored
`v0.236.1` runtime behavior. This document is unnumbered, authorizes no
implementation or release, and treats the pre-existing compatible `Cargo.lock`
refresh as part of the local measurement environment rather than as runtime
source.

## Decision frame

[Observed] The question is not whether a per-entity cache can be written. The
question is whether current avoidable work is large enough to justify a new
memoized state while preserving one accepted-schema authority, eager global
validity, immutable request snapshots, and fail-closed corruption behavior.

[Measured] The audit temporarily varied only the placement of zero, one, or two
identical 64-index entities around the fixed `PerfAuditUser` target. It measured
the accepted-schema read boundary, retained one root through an update for the
true warm sample, published one target index before the publication-following
cold sample, and ran the same 2,048-row/three-index same-Wasm recovery. The
temporary endpoint, entity, and integration-test changes were removed after the
measurements.

[Observed] “Target only” below means no added 64-index matrix entity; the rest
of the production-shaped SQL audit schema remains constant. Every row used the
same target entity, target query, empty target data, generated surface, stable
layout, and lifecycle. Each row asserted target query result and physical work
equality before and after publication. Stable extent was 578,879,488 bytes in
all four rows.

## 1. Does the original scaling problem still exist?

### Historical reconciliation

[Observed] The 0.228 isolated result, 21,617,488,641 instructions, measured the
then-current complete recovery implementation for 2,048 rows with three
secondary indexes. It used two recovery work messages, repeated canonical
authority and row-transition preparation, and preceded the later converged
single-batch recovery flow.

[Observed] The 45,190,180,079-instruction 0.228 result was an exploratory
same-store placement of the 64-index entity. It was deliberately rejected as
acceptance evidence and the maintained fixture moved that entity to its own
journaled store. It established a real dependency in the old implementation,
but it did not isolate derived-runtime compilation from repeated recovery
preparation.

[Observed] The intervening 0.229 Patch 8 correction selected and retired one
complete batch per callback and shared one batch-resolved canonical commit
context plus one prepared row transition across validation, retirement, and
Apply. The same production commit then folded in 3,946,083,082 instructions.
The 0.229 closeout selector and staging corrections reduced the maintained
three-index callback again to 2,615,527,509 instructions. The 0.230 exact
cardinality line added maintained recovery work and measured 3,736,427,024
instructions for the shape.

[Measured] The restored current matrix measured maximum recovery callbacks of
3,735,180,290 instructions without a 64-index matrix entity and
3,950,432,378 with both 64-index entities. The latter agrees with the separately
frozen exact-lock `v0.236.1` result of 3,952,036,708 within 1,604,330
instructions, or about 0.041%.

[Inferred] The old 21.617B and 45.190B observations are not valid present-day
cost baselines. Row count and target index fanout are comparable, but recovery
execution shape, callback count, transition reuse, selector behavior,
cardinality maintenance, and unrelated-entity store placement are not. Most of
the old cost was removed by 0.229’s single selected batch, shared commit
context, and shared prepared-transition corrections, not by 0.237.

### Current-baseline matrix

| Basis | Shape | First cold read | True warm read | Target-publication cold read | Upgrade/recovery maximum |
| --- | --- | ---: | ---: | ---: | ---: |
| Measured | Target entity only | 34,460,297 | 727,245 | 24,916,864 | 3,735,180,290 |
| Measured | Target + same-store unrelated 64-index entity | 62,310,473 | 738,187 | 52,363,680 | 3,849,439,904 |
| Measured | Target + other-store unrelated 64-index entity | 62,304,496 | 746,365 | 34,917,107 | 3,822,421,173 |
| Measured | Target + both unrelated entities | 95,171,112 | 757,862 | 64,125,319 | 3,950,432,378 |

[Measured] One 64-index entity adds 27,850,176 first-read instructions in the
target store or 27,844,199 in another store. Both add 60,710,815 instructions;
the additional 5,016,440 above the sum of the individual deltas is interaction
or run-level variation and must not be assigned to a phase without narrower
instrumentation.

[Measured] Recovery inflation is now 114,259,614 instructions for the
same-store entity, 87,240,883 for the other-store entity, and 215,252,088 for
both. Those are real current deltas, but even the largest is 5.76% of the
target-only callback rather than the old 109% same-store increase.

[Measured] Warm deltas are small in absolute terms: +10,942, +19,120, and
+30,617 instructions. The current root still captures stable store roots before
its cache lookup, so these approximately 0.73M samples are the restored
predecessor behavior, not 0.237’s reverted approximately 0.54M witness-first
path.

### Work attribution

| Basis | Phase | Current owner and measured interpretation |
| --- | --- | --- |
| Observed | Target-owned query work | Executor and stores; excluded from the accepted-schema instruction counter, with result and physical work checked separately |
| Observed | Database-root observation | `DbSession::capture_accepted_runtime_store_roots`; reads both accepted-root slots for every registered store before cache comparison and repeats after compilation |
| Observed | Bundle selection and verification | `SchemaStore::current_accepted_schema_authority_ref`; selects the root, verifies bundle hash/format/store binding and constraint-job closure on a cache miss |
| Observed | Identity closure | `SchemaStore::accepted_schema_authority_ref_for_selection`; validates current identity state every time authority is borrowed |
| Observed | Per-entity projection | `current_accepted_runtime_entities` plus `current_accepted_catalog_selection`; builds runtime identity, raw accepted snapshot selection, and fingerprint |
| Observed | Runtime compilation | `AcceptedSchemaEntityRuntime::compile`; decodes the selected snapshot, compiles inspection/row/index/relation/identity contracts, builds `SchemaInfo`, then builds `EntityAuthority` |
| Observed | Recovery work outside root compilation | Journal selection, complete validation, prepared row transitions, canonical Apply, overlay retirement, cardinality maintenance, and watchdog accounting |
| Inferred | Matrix deltas | Upper bounds on all unrelated-schema work in the measured envelope; they do not isolate safe-to-defer derivation from mandatory admission checks |

### Phase-attributed follow-up

[Measured] A temporary exclusive phase probe repeated the target-only,
same-store and other-store cold reads. An uninstrumented control path inside
the probe Wasm measured 34,487,753 instructions for the target-only shape,
62,264,201 with the same-store 64-index entity, and 62,323,334 with the
other-store entity. Those single-entity deltas are 27,776,448 and 27,835,581
instructions. Enabling 273 or 289 exclusive phase samples added only 181,384
or 191,381 outer instructions. The attributed deltas were 27,761,572 and
27,821,933, within 14,876 instructions of their corresponding outer deltas.

| Basis | Increment from one unrelated 64-index entity | Same store | Other store | Semantic class |
| --- | --- | ---: | ---: | --- |
| Measured | Accepted bundle load and verification | 11,527,312 | 11,570,772 | Eager authority admission |
| Measured | Accepted entity-selection projection | 5,849,570 | 5,901,654 | Pins root-owned identity, raw snapshot and fingerprint |
| Measured | Verified snapshot decode | 8,393,178 | 8,453,431 | Eager entity validity |
| Measured | Complete inspection-plan compilation | 867,501 | 892,551 | Mixed validation and derivation |
| Measured | `SchemaInfo` plus `EntityAuthority` | 430,729 | 433,257 | Pure derivation after accepted inputs exist |
| Measured | Remaining root, closure, catalogue and map work | 693,282 | 570,268 | Mostly eager authority ownership |

[Measured] The complete post-snapshot runtime tail—inspection plan,
`SchemaInfo`, and `EntityAuthority`—was only 1,298,230 instructions for the
same-store entity and 1,325,808 for the other-store entity. Even that is an
overstatement of a safe lazy boundary: inspection compilation validates row
layout, constraints, indexes, relations and Identity. The mechanically pure
row-contract, inspection-fingerprint, `SchemaInfo`, and `EntityAuthority`
buckets total only 467,903 and 470,920 instructions, and the row contract is
currently an eager input to later validation.

[Inferred] Placement does not materially change the phase split. Roughly
25.8M of each 27.8M single-entity increment precedes or performs mandatory
accepted-entity validity. The scaling effect is real, but it is not primarily
lazy-derived-runtime construction.

## 2. What is the maximum possible gain?

[Measured] For the both-entity first read, the absolute upper bound obtained by
removing every cost correlated with the two 64-index entities is 60,710,815
instructions. The lower endpoint is the measured 34,460,297 target-only read.
No lazy design that retains the target work can save more on this fixture.

[Observed] `AcceptedSchemaEntityRuntime::compile` is fallible admission work as
well as allocation. `AcceptedSchemaSnapshot::try_new` and fingerprint checks
run during selection decode. Inspection-plan compilation then validates row
layout, compiles accepted constraints and predicates, validates index
projection identity, resolves relation targets, checks identity declarations,
and can return typed corruption or invariant errors. `SchemaInfo` and
`EntityAuthority` construction follow those checks.

[Observed] Relation compilation is not currently a pure function of one entity
selection. It resolves accepted runtime entities and accepted catalog
selections through the current `Db` and store registry. This is safe in the
eager builder because store roots are captured before and after the complete
build. It would be unsafe for an old pinned root to invoke after publication.

[Measured] The phase probe closes the missing split. Across the two isolated
64-index increments, every mixed post-snapshot runtime phase totals 2,624,038
instructions, while the mechanically pure subset totals 938,823. The
four-shape matrix had a further 5,016,440 instructions of two-entity
interaction. Even assigning all of that interaction to deferrable runtime work
would produce only a 7,640,478-instruction conservative ceiling; there is no
evidence supporting that assignment.

| Basis | Candidate boundary | Best-case absolute saving | Realistic saving now supportable | Recovery effect | Cost transfer / worst case |
| --- | --- | ---: | ---: | ---: | --- |
| Measured/Inferred | 1. Lazy expensive derivative only | 2,624,038 across the two isolated complete runtime tails; 7,640,478 only if all unexplained interaction were also credited | At most 938,823 before slot overhead, and no current independent lazy seam exposes all of it | Unmeasured; the 215,252,088 outer recovery delta is not phase attribution | About 0.47M mechanically pure work transferred to first access per 64-index entity; touching all entities saves no aggregate work and adds slot overhead |
| Inferred | 2. Boundary 1 plus cross-root reuse | Boundary 1 plus the unchanged target derivative on unrelated publication | Unmeasured | Unmeasured | Requires provenance comparison and transfer between roots; a failed proof pays comparison plus rebuild |
| Measured/Inferred | 3. Lazy entity validation/admission | At most the same 60,710,815 correlated delta | Rejected as a current semantic option | At most 215,252,088 | Delays unrelated corruption and makes accepted behavior access-order dependent |
| Measured/Inferred | 4. Persisted/background/prewarmed derivatives | First-read latency could move toward the 757,862 warm sample, shifting at most 94,413,250 instructions | No demonstrated net saving | Cannot remove more than the 215,252,088 unrelated-schema recovery delta | Moves work to install, upgrade, publication, a job, or stable decode and adds durable failure/retry state |

[Measured] If the full 938,823 mechanically pure two-entity subset were avoided
once per accepted root and only the target were accessed, its amortized effect
would be 938,823 instructions for one request, 93,882 over ten, 9,388 over one
hundred, and 939 over one thousand. Aggregate saving remains one event per
root; laziness does not improve current warm requests after relevant entities
have materialized.

[Inferred] As entity/index count grows, best-case avoidance grows roughly with
never-accessed entity compilation. The two single-entity results suggest about
27.85M instructions per 64-index entity in this fixture, but the both-entity
interaction prevents claiming a stable linear coefficient. Worst case grows
with the complete eager work plus one slot lookup and one initialization branch
per entity.

## 3. Current authority chain

| Basis | Component | May select accepted schema? | May prove freshness? | May compile derivatives? | May invalidate/replace authority? | Derived from |
| --- | --- | --- | --- | --- | --- | --- |
| Observed | Accepted stable root keys | Yes, through the two-slot selector | Yes, as persisted current-root evidence | No | Root publication writes the inactive slot | Checksummed stable root slots |
| Observed | `SchemaStore::accepted_bundle_cache` | No; it must match a selected root | Store-local only, because root-key mutation clears it | Yes: value catalog, cardinality domain, entity selections | Cleared on accepted-root mutation, projection reset, retention and recreation | Selected root plus verified immutable bundle |
| Observed | Verified accepted bundle | No | No independent freshness claim | Supplies accepted entity/catalog inputs | Replaced only through `SchemaStore` publication/fold | Root-bound hashed bundle bytes |
| Observed | Accepted catalog/entity selection | No | No | Decodes one accepted snapshot and value catalog | No | Verified bundle and entity identity |
| Observed | Identity closure | No | No | No | Admission failure blocks authority use | Bundle declarations plus effective identity-state records |
| Observed | `StoreHandle` | No | No | Routes access to the owning stores | No | Sealed `StoreRegistry` |
| Observed | `AcceptedSchemaRuntimeRoot` | No; it captures already selected store roots | Yes only by comparing a fresh full root capture | Eagerly compiles all entity runtimes | Whole root replaced in the thread-local map | Incarnation, ordered store roots, verified entity selections |
| Observed | `AcceptedSchemaEntityRuntime` | No | No | Owns inspection plan, row contract, `SchemaInfo`, `EntityAuthority` | No independent invalidation | One root identity plus one accepted entity selection |
| Observed | `current_accepted_schema_authority_matches` | No | Yes, store-locally via the owner-invalidated bundle witness or stable root selection | No | No | Expected accepted authority plus `SchemaStore` current state |
| Observed | `AcceptedSchemaCatalogContext` | No | No | No | No | `Rc` pins one root and one entity runtime |
| Observed | `RequestExecutionRoot` / `DbSession` | No | No | Session resolves a catalog context; request root owns budgets, not schema | SQL DDL explicitly drops the runtime-root cache after publication | Sealed registry and request counters |
| Observed | Publication | Yes, through expected-revision root preparation | Yes | No | Atomically replaces stable accepted root; root-key write clears store cache | Candidate bundle/root and current slots |
| Observed | Journal replay and fold | Applies the already journal-authorized candidate | Yes through replay/preflight/current-root checks | No | Replays live authority, folds canonical authority, retires overlays | Marker-bound journal plus canonical store |
| Observed | Recovery projection reset | No | No | No | Clears live projections and accepted bundle cache before replay | Canonical stable base and journal tail |
| Observed | Retirement / reinstall | Retirement removes exact overlays and invalidates on root keys; reinstall recreates state | No independent proof | No | Reinstall clears heap roots naturally and recreates stable authority | Positioned overlay ownership / fresh installation |

[Observed] The chain is singular: stable root selection chooses accepted state;
`SchemaStore` verifies and projects it; the database-wide runtime root captures
that state; contexts pin the root. A fingerprint, revision, or runtime-root
identity binds derivatives but cannot select a schema.

[Observed] Two ownership details are easy to misread. The bundle cache is both
a derivative cache and a store-local freshness witness, but it remains inside
the root-writing `SchemaStore` and is cleared by that owner. SQL DDL also drops
the database runtime-root cache, but correctness does not depend on that drop:
every lookup first captures and compares stable roots. The duplicate DDL
invalidation is a performance shortcut, not a second authority.

[Observed] The genuine lazy-design ambiguity is relation compilation. It reads
current database/catalog authority while constructing a derivative. The eager
before/after root check currently closes that race. A deferred old-root slot
would need all relation validation and binding inputs pinned inside its owning
authority snapshot; it may not reread current roots or catalog selections.

## Reference design stress test

[Proposed] If evidence later supports laziness, the least-complex shape is one
authority-owned root with monotonic per-entity slots and whole-root replacement.
The root must own accepted store/root identities, verified selections, global
tag/path/name uniqueness, relation target identities, and every result needed
to establish complete-schema validity before publication. A slot may then
derive only from those pinned inputs.

[Proposed] A slot hit must never prove freshness or choose a schema. A request
first resolves the current root, pins it for the complete execution, then reads
or initializes the root-owned derivative. Publication installs a new root;
existing contexts may retain the old root, and no new context may discover it.
Recovery clears or replaces the owning root as one operation. Cache loss must
change cost only.

[Observed] The illustrative `OnceCell<Result<...>>` adds a subtle failure axis.
Current compilation can encounter corruption, missing relation authority, and
borrow conflicts. Caching a transient borrow conflict would convert a
request-local condition into a permanent root failure; retrying selected
failures would make the cell more than one-shot. The initializer must first be
made pure over pinned, eagerly validated inputs, with a closed deterministic
error policy, before cached failure is coherent.

[Inferred] The reference design is technically plausible but is not a small
cache insertion. It requires an admission/derivation split, pinned relation
dependencies and deterministic failure classification. The phase evidence now
shows that the deferred pure part does not own a substantial fraction of the
60,710,815 correlated ceiling, so the design fails the build threshold.

## Alternatives and complexity

| Basis | Option | Expected instruction gain | Runtime-line delta | Test/evidence-line delta | Types/modules | Mutable owners / caches | Invalidation and failure states |
| --- | --- | ---: | ---: | ---: | --- | --- | --- |
| Observed | A. Current eager complete root | 0 | +0 | +0 | +0 | Existing root and bundle caches | Existing whole-root replacement and eager failure |
| Proposed | B. Root-owned lazy derivative slots | At most 938,823 realistically supportable across the two isolated unrelated entities; 2,624,038 before separating mandatory inspection validation | +180 to +300 | +500 to +900 | 1–3 types, preferably no module | One monotonic slot per entity, owned by root | Whole-root replacement plus empty/building/success/typed-failure slot states |
| Proposed | C. B plus cross-root reuse | B plus unmeasured unrelated-publication saving | +150 to +300 beyond B | +400 to +700 beyond B | Provenance/transfer types, possibly one module | Reuse donor/transfer state in addition to slots | Adds unchanged-entity proof, transfer rejection, old-root provenance failures |
| Proposed | D. Lazy validity or granular authority | At most 60,710,815 on this first read | +300 to +600 | +800 to +1,400 | Multiple authority/validity types | Multiple independently valid fragments | Adds access-order validity, partial publication and granular invalidation; violates the required singular semantics |
| Proposed | E. Persisted/background/prewarmed derivatives | Up to 94,413,250 latency shifted, no proven net saving | +500 to +900 | +1,000 to +1,800 | Codec/job/timer/state modules | Stable cache plus job/timer/retry owner | Adds format, recovery, corruption, retry, scheduling and upgrade states |

| Basis | Option | Raw/gzip Wasm risk | Heap/stable impact | Corruption and recovery burden | Maintenance verdict |
| --- | --- | --- | --- | --- | --- |
| Observed | A | None | Current eager heap; no derivative stable bytes | Eager complete failure; current recovery | Lowest burden |
| Proposed | B | Moderate from slot/error/split logic | Similar eventual heap, lower partial-use heap; no stable bytes | Must preserve eager admission and clear root on recovery | Not proportionate to the measured derivative gain |
| Proposed | C | Moderate-high | May retain old/new derivative graphs during publication | Cross-root provenance and rollback proof | Too much before B is proven |
| Proposed | D | High | Fragmented heap authority | Delayed unrelated corruption and partial recovery | Rejected |
| Proposed | E | Highest | Additional heap and stable records | New corruption taxonomy, codecs, jobs and recovery | Rejected without separate evidence and authorization |

## Required proof matrix for any future B design

| Basis | Proof | Current availability | Future requirement |
| --- | --- | --- | --- |
| Measured/Proposed | First target access with large unrelated entities | Temporary four-row and exclusive phase evidence; instrumentation removed | Retained phase-attributed test only if a future design is authorized |
| Observed/Proposed | Repeated warm access | Existing 1,000-request root-reuse test | Add initialized-slot hit counts and instruction gate |
| Proposed | Two entities materialized in both orders | None | New deterministic order-independence proof |
| Observed/Proposed | Target publication | Generic atomic root/context test exists | Add old-slot isolation and new empty-slot proof |
| Proposed | Unrelated same-store publication | Reverted 0.237 evidence only | New B-specific proof |
| Proposed | Unrelated other-store publication | Reverted 0.237 evidence only | New B-specific proof |
| Observed/Proposed | Publication failure and rollback | Generic commit/DDL rollback coverage exists | Prove no slot escapes failed root publication |
| Observed/Proposed | Old context across publication | Existing root-context identity proof | Prove old slot remains usable only through old context |
| Observed/Proposed | New context after publication | Existing replacement-root proof | Prove new context cannot reach old slot |
| Observed/Proposed | Journal replay and fold | Existing store replay/fold test | Add slot/root replacement assertions |
| Observed/Proposed | Projection reset | Existing cache-reset path and test | Add root/slot clearing assertion |
| Observed/Proposed | Same-Wasm upgrade | Existing recovery fixture | Add cold-slot and pinned-authority proof |
| Observed/Proposed | Reinstall | Existing clean recreation fixture | Add absence of derivative persistence proof |
| Observed/Proposed | Missing and mismatched root | Root selector/current-authority coverage exists | Add fail-closed slot non-use proof |
| Proposed | Borrow conflict | None for lazy initialization | Prove transient conflict is not cached as authority |
| Proposed | Corrupted target entity | Generic eager corruption paths exist | Prove failure occurs before root publication or explicitly reject design |
| Proposed | Corrupted unrelated entity | Global eager invariant exists | Prove it still blocks root publication before target access |
| Proposed | Cached success followed by publication | None | Prove whole-root replacement and old-context isolation |
| Proposed | Cached typed failure behavior | None | Define deterministic-only policy and prove replay behavior |
| Observed/Proposed | No-default, SQL-only, all-feature builds | Existing configurations | Rerun for the changed implementation |
| Observed/Proposed | Stable extent, Candid and exports | Existing audit machinery | Require equality and raw-Wasm-first report |
| Measured/Proposed | Deterministic result and physical work | Temporary matrix and existing query audits | Retain cross-shape, publication and upgrade equality gates |

## Complexity decision

[Measured] A current scaling effect exists for first accepted-root construction:
both unrelated 64-index entities correlate with 60,710,815 additional
instructions. The old recovery catastrophe does not exist; the corresponding
current recovery delta is at most 215,252,088 instructions against a roughly
3.74B target-owned callback.

[Measured] The removable phase required for safe Option B is now bounded and
is not substantial. Per 64-index entity, roughly 27.8M instructions are added,
but only 1.30M–1.33M occur in the complete mixed runtime tail and only
0.468M–0.471M are mechanically pure. Bundle verification, accepted selection
projection and verified snapshot decoding account for most of the increment
and remain eager under the required complete-schema validity semantics.

[Inferred] Option B would therefore add an admission/derivation split, pinned
relation inputs, slot state and a large proof matrix to move less than one
million demonstrated instructions across the two isolated unrelated entities.
Even the unjustified interaction-inclusive ceiling of 7,640,478 is far below
the 23,792,778 instructions that a 25% improvement on the 95,171,112
maximum-shape read would require. A smaller optimization to remove redundant
accepted-snapshot serialization or decoding would target the measured large
phases without adding lazy lifecycle state, but it requires separate evidence
and authorization.

[Proposed] Keep Option A. Do not prepare B1, add slots, reuse across roots,
delay validation, or introduce persisted/background materialization. New work
would require a new bottleneck and separately accepted evidence; the current
derived-runtime gain does not justify the ownership and proof burden.

DO NOT BUILD — CURRENT COST DOES NOT JUSTIFY COMPLEXITY
