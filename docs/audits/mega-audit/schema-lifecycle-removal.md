# Schema lifecycle removal investigation

Read-only source investigation, 2026-11-12, against the working tree based on
`c71f242833b2c490aaf11f68a62cb72c1b48e340`. Existing tooling/dependency and release
documentation edits were preserved. This report extends the earlier
[schema lifecycle audit](schema-lifecycle.md); its completed changes are not new
opportunities. This is evidence, not an implementation tracker or authorization
to change a release line.

## Verdict

The best large opportunity is to remove the migration planner's intermediate
accepted-bundle round trip and converge target construction. The clearest smaller
opportunities are repeated candidate validation/serialization, SQL DDL operation
rediscovery, and a migration phase that performs accounting rather than rebuilding.
These are four bounded proposals, not evidence that the entire schema lifecycle
or its recovery machinery can be deleted.

The existing source inventory counts `db/schema` at 49,425 non-test and 50,663 test
code lines. Those are scope sizes, not removable-line estimates. Moving the
authoring crates would not remove this runtime responsibility. No defensible
10,000-line deletion has been established by this investigation.

## Flow traced

| Surface | Current path | Distinct responsibility to retain |
| --- | --- | --- |
| Explicit/generated application | common application owner → initial/existing lowering → candidate → coordinated publication | Generated proposals cannot request explicit removals; exact replay and accepted-head checks |
| SQL DDL additions | accepted snapshot derivation + admission → publication envelope → snapshot-delta/request/transition rediscovery → candidate → publication | Current identity, physical index staging, historical-fill validation and exact empty-entity proof |
| Explicit migration | transition resolution → rename bindings → rebuilt snapshots → temporary accepted bundles → ordinary reconciliation → compiled row programs | Explicit identity changes, source lineage, SQL-owned objects, cross-entity references |
| Physical migration continuation | recompile against current authority → validation/staging → rewriting → domain count → final validation → publication | Bounded durable progress, pre-rewrite abort, same-contract retry/recovery |
| Candidate publication/recovery | validated bundle → encoded candidate/root → journal/live publication → canonical fold/reload | Atomic publication, checksummed roots, bounded persisted decode, distinct effective/canonical authority |

Inspection covered the application orchestrator, ordinary existing-entity
lowering, migration planner and continuation, SQL addition admission/publication,
bundle construction/encoding, selected store publication paths and relevant tests.
It did not exhaustively qualify every constraint activation, DDL mutation,
backend or public consumer. No tests or Wasm measurements were run for this
source-only investigation.

## 1. Converge migration target construction

**Classification:** late convergence; highest structural potential, conditional
on a successful design/prototype.

In [migration planning](../../../crates/icydb-core/src/db/schema/migration_planner/mod.rs),
`WorkingStore::new` copies the catalogs, bindings and entity snapshot map.
`rebuild_transitioned_snapshots` reconstructs fields/indexes, then relations, then
constraints. `WorkingStore::exact_bundle` clones that working state into a
validated bundle. `reconcile_rekeyed_view` wraps those bundles in
`ExistingProposalStore` and calls `lower_existing_schema_proposal`. The ordinary
lowerer then decides the remaining target changes and can return complete staged
activation candidates. The planner chooses those candidates or finalizes its
own working state.

This is two construction paths joined by an intermediate representation that
looks like accepted authority even though it has never been published. The
ordinary lowerer is already the authority for remaining changes; there is not a
second redundant invocation of it to simply delete.

**Proposed owner:** one concrete schema target assembler. Ordinary application
uses accepted identities; migration supplies resolved explicit identity changes
and physical transformation requirements. Both construct the final catalog once.
Retain migration-specific transition validation and row-program compilation.
Do not introduce a general overlay framework, trait hierarchy or second mutable
cache to achieve this.

**Deletion target:** `WorkingStore::exact_bundle`, the temporary bundle/store
adapters in `reconcile_rekeyed_view`, the choice between independently assembled
candidate paths, and overlapping field/index/constraint assembly where semantic
comparison proves equivalence. Do not count all of `migration_planner` or
`application_lowering` as removable: they contain substantial distinct policy.

**Required proof:** renamed identities retain tags and lineage, including reuse
of predecessor source names; SQL-owned objects survive reconciliation; generated
staged activations remain exact; same-proposal forward relations resolve after
identity reservation; unchanged entities do not advance versions; ordinary and
migration proposals produce equivalent intended candidates. Preserve rejection
of unexplained differences and active-work conflicts. Compare candidate roots
and physical programs as well as user-visible outcomes.

**Impact:** intended internal construction change, with no necessary persisted
format change. Candidate identity and allocation order must remain deliberate.
Expected complexity reduction: one intermediate accepted representation and one
round trip removed, fewer independently maintained assembly decisions. Net LOC
and runtime benefit remain unmeasured. This is the large follow-up, not a safe
delete-only patch.

## 2. Give immutable candidate construction one validation/encoding owner

**Classification:** repeated transformation and validation in the trusted
construction path; directly observed.

In [bundle publication](../../../crates/icydb-core/src/db/schema/enum_catalog/publication/mod.rs):

1. `AcceptedSchemaRevisionBundle::new_with_source_bindings` calls `validate`.
2. `validate` encodes/decodes enum, composite and source-binding catalogs,
   checks snapshots and runs cross-component semantic checks.
3. `CandidateSchemaRevision::prepare` calls the bundle encoder, which calls
   `bundle.validate()` again and then serializes its components again.
4. Candidate preparation decodes the complete newly encoded bundle and compares
   it with the input, hashes its bytes, then encodes/decodes the new root.

There is already an important optimization: persisted bundle decoding calls
`validate_decoded_semantics`, avoiding the full component round trip. Keep it;
do not report that earlier duplication as still present.

**Proposed owner:** finalize a candidate once from unpublished construction data,
performing all required semantic and encoding/bounds checks there. Alternatively,
preserve the immutable validated bundle as the proof and make its private encoder
avoid re-running the same validation. Choose the simpler design after auditing
all constructors and test-only mutation helpers. Do not create an unchecked
public constructor.

**Deletion target:** repeat full validation during encoding and construction-time
codec round trips that serve only to prove the encoder agrees with its decoder.
Move codec equivalence proof to tests only after confirming round trips are not
currently the sole enforcement of a bound or invariant. Keep encoding length
limits, canonical order, default payload checks, projection limits, hashes and
the actual persisted representation.

**Required proof:** existing malformed candidate rejection, nested projection
limits, resource diagnostics and constructed-versus-decoded root equality tests
must remain effective. Extend boundary cases where an encode/decode round trip
currently supplies missing validation. Keep `from_encoded`/`decode_contents`
fully checked: persisted journal bytes are a different trust boundary.

**Impact:** internal; preserve bytes and public behavior. Expected complexity
reduction: fewer validation passes and temporary representations, no additional
authority/cache. Likely more reduction in work than source lines. Resource
accounting must track retained work rather than leave charges for deleted passes.
Net LOC, IC instructions/cycles and Wasm effects remain unmeasured.

## 3. Stop rediscovering an already admitted SQL DDL operation

**Classification:** policy rediscovery; concrete, bounded consolidation.

[DDL admission](../../../crates/icydb-core/src/db/schema/mutation/ddl_admission.rs)
already returns `SchemaDdlAcceptedSnapshotDerivation` containing the accepted
after-snapshot and a typed mutation target. Nevertheless,
[publication](../../../crates/icydb-core/src/db/schema/publication/mod.rs)
calls `require_transition_plan`, which passes before/after snapshots through
`SchemaMutationDelta` → `SchemaMutationRequest` → `SchemaTransitionPlan`.
Index publication compares the rediscovered target with the admitted one.
The field-path index derivation itself also invokes the delta classifier;
expression-index derivation directly admits its target.

**Proposed owner:** catalog-native DDL derivation proves the supported change and
carries its operation/target to execution. Publication consumes that proof and
rechecks current authority and mutable physical preconditions.

**Deletion target:** the additive transition rediscovery layer and redundant
delta/request conversions once their callers are redirected. Consolidate the
initial admission classifier where needed; do not just remove publication's
check while leaving derivation capable of admitting a wider change.

**Required proof:** keep historical-fill payload decoding and nullable/history
floor rules at admission; retain the live exact-empty check for required-field
addition. Bind the proof to the accepted-before identity and the admitted after
snapshot. Reject unrelated simultaneous changes, wrong targets, stale identity,
malformed defaults and unsupported key shapes. Preserve physical index staging
and unique-constraint behavior. Relocate useful transition tests to admission;
remove only tests of representations that cease to exist.

**Impact:** intended internal change. Expected complexity reduction: fewer
operation representations and one authoritative admission decision. Small to
medium code opportunity, not thousands of lines. Exact net LOC is unmeasured.

## 4. Remove the accounting-only `RebuildingIndexes` phase

**Classification:** lifecycle state without corresponding physical work;
confirmed behavior, conditional hard-cut simplification.

In `advance_active_schema_migration` in
[application](../../../crates/icydb-core/src/db/schema/application/mod.rs),
`RebuildingIndexes` recompiles the plan, calls `migration_derived_domain_count`,
resets row progress and advances to `FinalValidation`. The called function in
[migration execution](../../../crates/icydb-core/src/db/schema/migration_execution/mod.rs)
prepares candidate entities and counts index/relation projections; it performs
no index writes. Candidate entries are staged during validation, and rewrite
pages carry physical effects. Final validation checks resulting rows and entries.

**Proposed change:** advance completed rewriting directly to final validation,
retaining any required domain count at the appropriate owner. Remove the extra
phase, dispatch branch and associated codec/public status cases. This does not
justify removing validation, rewriting or final validation themselves.

**Important exclusion:** `PersistedSchemaMigrationIndexCursor` is used by bounded
abort cleanup. It is not dead rebuild machinery and must remain. Likewise retain
abortability distinctions before and after row rewriting.

**Required proof:** interrupted final rewrite resumes correctly; journaled
progress and physical effects stay atomic; empty migrations and multi-page work
retain correct completion; final validation detects missing index/relation
entries; status counts remain defined; terminal replay and bounded abort retain
their behavior. Existing phase-sequence tests must change with the contract,
not be deleted wholesale.

**Impact:** public `SchemaMigrationPhase` and persisted phase encoding change.
Requires an explicitly selected hard-cut implementation boundary and retained-data
handling; this audit does not authorize it. Expected complexity reduction: one
persisted/public state and one continuation step, with no replacement state.
Source reduction likely modest. No timing or IC cost saving is claimed.

### Implementation — 0.311

The maintainer authorized the next minor on 2026-11-13. The accounting-only
state, dispatch branch, public variant and codec case are removed. The final
rewrite page now journals the completed domain count and reset validation cursor
with its physical effects, directly entering final validation. The independent
count helper retains bounded projection admission; one separate plan
recompilation and continuation disappear. Abort cleanup keeps its index cursor.

The new `ICYSMGC1` envelope rejects the old `ICYSMIG1` record intact. This includes
old terminal records: existing installations need explicit retirement/reset
before adoption, with any in-flight recovery completed on the original release.
There is no legacy reader or automatic deletion. Same-contract journal recovery
remains required. See the [0.311 tracker](../../design/0.311-migration-completion/status.md)
for qualification and release disposition.

## Boundaries deliberately retained

- Installation and additive creation already share new-entity lowering.
- Generated reconciliation already joins explicit application; its removal
  restrictions and source-ownership rules are product semantics.
- Forward-reference identity reservation is necessary even if intermediate
  snapshot construction is simplified.
- Effective live authority and canonical journal-fold authority describe
  different recovery positions. Their similar checks are not duplicate engines.
- Cached immutable bundles do not prove current mutable validation-job closure.
- Recompiling a retained migration against current authority protects plan/head
  agreement. A new persisted compiled-plan cache is not justified by this audit.
- Pre-rewrite validation protects abortability; post-rewrite validation observes
  changed durable contents. They are not interchangeable scans.
- Snapshot/bundle bytes, fingerprints and bounded recovery decoders remain.

## Recommended execution and validation

Start with DDL admission convergence: it has a clear deletion boundary and can
establish the carry-proof-to-publication pattern without redesigning migrations.
Then simplify candidate construction after enumerating its validation obligations.
Treat migration target convergence as a separate design/prototype with the
explicit requirement to delete the intermediate accepted view. Consider phase
removal only at an authorized hard-cut boundary.

For each implementation report actual net non-test/test code change and which
representations, passes or states disappeared. Reject a prototype that replaces
these with a larger generic framework. Qualify direct affected boundary families;
full workspace/release gates remain user-owned. Measure matched Wasm total and
code-section bytes and IC instructions/cycles if making runtime benefit claims.

This investigation changes zero Rust code and zero tests. Proposed deletion LOC,
Wasm and IC instruction/cycle deltas are unmeasured. Test inventory was inspected;
no test was proven obsolete merely because it covers a similar lifecycle stage.


## Implementation update — 2026-11-12

The authorized internal rewrite replaces the migration `WorkingStore` and ordinary
`ExistingStoreCandidateState` with one owned `SchemaDraft`. Migration rebuilding
and ordinary reconciliation now mutate the same catalog, and one finalizer
constructs accepted candidates. Immutable reconciliation inputs capture routing,
source identities, relation targets and activation provenance before mutation;
they contain no snapshot map. Exact named-type validation no longer returns
cloned value catalogs. Candidate precedence/fallback selection is deleted.

The required pre-reconciliation catalog check and public failure distinctions
remain. Migration rebuilding still reserves forward references in ordered passes;
durable publication, validation, recovery and migration phase contracts are not
rewritten. See the implementation record for qualification and measured deltas.
This supersedes the earlier incremental state described above without claiming
removal of the independently deferred migration progress phase.

## Construction replacement completion — 2026-11-13

The next authorized replacement moves target structure updates to the existing
snapshot payload owner, consumes initial and migration snapshots through their
completion phases, and shares active row-constraint binding/allocation. Separate
snapshot-shell builders, the temporary migration-field result, migration-only
constraint compiler and old active row constructors are removed. Migration's
relation and row-constraint completion now share a pass after identity reservation.

The remaining migration/reconciliation policy loops are deliberately distinct:
physical changes and immediate migration validation differ from ordinary rejection
and staged activation. Their equivalent value/index/relation binding operations
and active-row construction now have shared owners. The original finding does
not justify collapsing these distinct contracts or removing durable recovery.
The implementation record contains the final 399-test qualification and the
production/test delta; no Wasm or IC performance improvement is claimed.


## Domain-discovery follow-through — 2026-11-13

The maintainer approved the follow-on 0.311 domain-discovery cleanup. Counting and
abort inventory now share a metadata visitor instead of calling full candidate-row
preparation. Generation selection is common with row projection; relation target
binding retains the common capability check. Cleanup uses its existing ordered
sets without a separate domain vector or sorting pass. This supersedes the
retained projection-preparation detail in the initial phase-removal record above.
The implementation record reports the source increase and removed mechanisms;
the [0.311 tracker](../../design/0.311-migration-completion/status.md) owns closeout.
