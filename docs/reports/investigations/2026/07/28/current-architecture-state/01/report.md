# IcyDB Current Architecture State Audit

Date: 2026-07-28

Scope: present-state repository architecture, authority, frontend convergence,
validation, representation, recovery, and package boundaries

Method tag: `current-architecture-state-v1`

## Report preamble

- Baseline report: none. This is an independent first run, not a rerun of the
  0.213 closeout.
- Initial code snapshot:
  `defccc3ddc3dd969b41b862dbf944436c9b38c9f`.
- Final audited code snapshot:
  `634661d8cad8f09376dbc93944fbf7e55891300a`, tag `v0.213.37`.
- Snapshot movement: the repository advanced during the audit by the
  `Release 0.213.37` commit. That commit changes workspace release metadata and
  the README, not the architecture examined here.
- Current version: `0.213.37`.
- Branch: `main`, tracking `origin/main`.
- Initial and pre-report worktree: clean.
- Comparability: not directly comparable with the release closeout because this
  audit reconstructed the current system from dependency edges, construction
  sites, and runtime call sites before reading the closeout report.
- Correction posture: the initial audit was read-mostly. Subsequent bounded
  remediation has completed R1 through R7: the obsolete
  generated-versus-accepted schema-check surface and generated entity routing
  are deleted, planner index metadata now has one semantic key-item
  representation, and active/pending constraint failures preserve accepted
  identity through one diagnostic envelope. Mutation admission now also has
  one accepted-schema-owned scheduler over row-local and storage-backed
  constraints. Typed adapters now bind directly to accepted entity/field
  identities and slots. Dynamic and typed reads now use an engine-neutral
  feature and structural payload below sibling SQL and typed adapters.

## 1. Executive verdict

**Verdict: clean and coherent.**

There is one real accepted-schema-driven query planner, executor, structural
mutation pipeline, and physical row boundary. Generated Rust entity types do
not participate in core planning, expression evaluation, row validation, or
physical encoding. SQL, dynamic/structural, and typed writes converge on the
same accepted after-image and commit machinery. SQL and typed/dynamic reads
also use the same `StructuralQuery` planner/executor.

The initial audit also found a generated runtime entity route gate, an obsolete
schema-check CLI/API, parallel planner index metadata, lifecycle-dependent
constraint diagnostics, and row-order-dependent batch constraint admission.
R1 through R7 hard-deleted or consolidated those layers. `Db` now
contains only the sealed store registry,
`AcceptedRuntimeEntity` is projected from the current source-bound accepted
bundle, and every accepted secondary index lowers to one ordered semantic
key-item vector. One opaque mutation-constraint batch now carries all normal
save/delete admission into the commit window. The query feature owns accepted
planning, caching, execution, and structural projection; SQL owns only its
frontend semantics and response adaptation.

No P0 corruption or loss-of-atomicity defect was found. R5 corrected a P1
fail-closed defect in which valid unique swaps/cycles and self-referential
relation batches could be rejected according to input order.

### Direct answers

1. **Actual architecture:** schema/model crates construct proposals; core
   accepts and persists catalog snapshots; accepted projections drive a
   generic-free query engine and one structural mutation pipeline; typed APIs
   adapt source-bound Rust values at the facade; core owns physical rows,
   indexes, relations, commits, and recovery.
2. **Canonical/coherent parts:** proposal/acceptance separation, core package
   independence from model crates, engine-neutral query plans, shared write
   after-images, catalog-driven physical codecs, marker-owned publication, and
   fingerprint-bound query/inspection caches.
3. **Reachable old/parallel flows:** none found after R1-R7. Unique and relation
   evaluators remain deliberately specialized below one accepted mutation
   scheduler; SQL-specific lowering remains a frontend, not a second engine.
4. **Historically evolved abstractions:** the registration, schema-check,
   parallel index-key, name-routed typed-binding, and SQL-owned structural
   projection layers identified by the audit are no longer present.
5. **Repeated policy:** no duplicated durable mutation policy remains in the
   audited paths. R4 consolidated constraint identity/diagnostics, R5
   consolidated scheduling, and R6 removed typed name re-resolution.
6. **Durable validator unification:** row-local accepted constraints share one
   compiled row-constraint program. The accepted mutation scheduler now orders
   that program and specialized storage-backed unique/relation proofs over one
   complete batch overlay, with one diagnostic envelope.
7. **Concepts that must remain separate:** schema well-formedness, application
   normalizers/validators, database-owned defaults/timestamps, boundary and
   corruption validation, and cross-row physical proofs.
8. **Query engine independent of Rust structs:** yes below the typed boundary.
9. **Accepted schema without generated entities:** yes. Source-bound entity
   snapshots in current accepted bundles now provide deterministic runtime
   discovery and routing; generated canister wiring emits stores and frontend
   surfaces only.
10. **Influence of Rust types:** typed adapters supply compatibility requests
    and conversions only; no user `E`, `TypeId`, Rust default, derive validator,
    or generated physical codec was found in the core planner/executor. Sealed
    store registration supplies physical handles, while accepted bundles alone
    determine entity reachability.
11. **Delete/consolidate/move next:** no further deletion is justified by this
    audit. Nested/member durable constraints require a separate accepted-path
    design rather than extension of the completed remediation.
12. **Safe order:** R1 through R7 are complete. Preserve this boundary in later
    accepted/public contract work.

## 2. Current architecture

### 2.1 Authority diagram

```text
Rust declarations / model macros                 schema-only callers / SQL DDL
            |                                               |
            v                                               v
icydb-model Schema graph                         SchemaProposal / DDL intent
            |                                               |
            +----> bounded SchemaFragment / SchemaProposal <-+
                                      |
                                      v
                    database-scoped lowering and reconciliation
                                      |
                                      v
                    CandidateSchemaRevision + source bindings
                                      |
                                      v
                 marker-owned atomic catalog publication/receipt
                                      |
                                      v
                 durable accepted bundle/snapshot/catalog authority
                         |                         |
                         v                         v
          fingerprint-bound runtime projections   physical layout/contracts
          (SchemaInfo, inspection plans,          indexes, relations, rows,
           constraint programs, plan cache)       journal, recovery
                         |
          +--------------+----------------+
          |              |                |
          v              v                v
       SQL AST     StructuralQuery    typed adapter boundary
          \              |                /
           +------ logical/access plan ---+
                          |
                          v
                  generic-free executor
                          |
                          v
              engine-neutral values/results
```

Runtime entity selection now has the same authority direction:

```text
sealed StoreRegistry --selects physical store--+
                                                |
current accepted bundle/source binding --------+--> AcceptedRuntimeEntity
```

The route is reconstructed from the accepted head, and an unbound or ambiguous
accepted entity fails as catalog corruption.

### 2.2 Schema flow

| Transition | Actual owner |
| --- | --- |
| Rust declarations to authored graph | `icydb-model::Schema` and derive-generated model nodes |
| Host closure to fragment | `Schema::schema_fragment_for_canister` in `crates/icydb-model/src/fragment.rs` |
| Bounded proposal composition | `icydb_schema::SchemaProposal::try_compose` |
| Public application | `icydb::db::DbSession::apply_schema` in `crates/icydb/src/db/session/catalog.rs` |
| Database/store candidate composition | `db::schema::application::{apply_schema, lower_application_candidates}` |
| Initial/existing reconciliation | `db::schema::application_lowering` and catalog-native transition modules |
| Atomic publication | `db::commit::schema_publication::publish_accepted_schema_candidates_with_application_record` and related marker-owned variants |
| Durable authority | `AcceptedSchemaRevisionBundle`, `AcceptedSchemaSnapshot`, accepted source-binding and constraint catalogs in each schema store |
| Runtime projection | `AcceptedInspectionPlan::compile`, `AcceptedSchemaCatalogContext`, `SchemaInfo`, row/value contracts |

Source declarations and generated models are proposal inputs. There is no
accepted-schema-to-generated-model reconstruction in core. SQL DDL is a
frontend to catalog-native transition/publication logic rather than a second
durable owner.

### 2.3 Typed write flow

```text
typed Rust input
  -> facade typed write adapter and DynamicTypedEntityBinding compatibility
  -> source key to binding-owned accepted field ID + row slot
  -> DynamicTypedMutation / DynamicTypedStructuralPatch
  -> direct AcceptedMutationIntentPatch lowering with ID/slot recheck
  -> AcceptedStructuralMutation
  -> DbSession::execute_accepted_structural_save_batch_inner
  -> accepted defaults/generated/database-owned timestamp resolution
  -> canonical final after-image + write provenance
  -> AcceptedMutationConstraintScheduler row-local evaluation
  -> complete-batch unique/relation proof scheduling
  -> accepted physical row encoding
  -> atomic commit window
  -> engine-neutral result
  -> typed output conversion
```

The core flow is in `crates/icydb-core/src/db/session/write.rs`.
`AcceptedMutationIntentPatch` preserves omission, explicit default, null, and
value until accepted write policy resolves them. The database freezes a single
timestamp for a batch. Generated adapters do not apply defaults, timestamps,
normalizers, constraints, or physical codecs.

R6 removes the former source-to-display-name-to-slot round trip. Display names
remain result labels only.

### 2.4 Structural write flow

```text
DynamicMutation / field-name patch
  -> accepted entity and field-name resolution
  -> AcceptedMutationIntentPatch
  -> the same AcceptedStructuralMutation batch pipeline
  -> the same final after-image, constraint, relation/index, encoding, and commit
```

There is no structural-only persistence path.

### 2.5 SQL write flow

```text
SQL parser
  -> semantic compiler and accepted SchemaInfo binding
  -> SQL write candidate/patch lowering
  -> AcceptedMutationIntentPatch + AcceptedStructuralMutation
  -> the same accepted structural save batch
```

`INSERT` and `UPDATE` converge in
`crates/icydb-core/src/db/session/sql/execute/write/`.
SQL `DELETE` shares accepted structural selection, deletion preparation,
relation safety, and the commit boundary. SQL performs early frontend admission
for clearer SQL errors, but canonical mutation policy is enforced again at the
shared accepted patch/after-image boundary; SQL is not the authority.

### 2.6 Typed/fluent query flow

```text
Query<C, E> and generated field tokens in facade
  -> source-bound typed adapter compatibility
  -> binding-owned immutable entity source/tag catalog selection
  -> engine-neutral DynamicQueryRequest / StructuralQuery
  -> accepted EntityAuthority and SchemaInfo
  -> logical query plan
  -> accepted access lowering and prepared execution plan
  -> generic-free executor/terminals
  -> engine-neutral projection rows and OutputValue
  -> typed output adapter
```

The facade may use `PhantomData<E>` for compile-time ergonomics. Core query,
planner, access, executor, cursor, grouping, projection, ordering, prepared
plan, and cache types do not carry the user entity generic.

### 2.7 SQL query flow

```text
SQL AST
  -> accepted semantic binding/lowering
  -> StructuralQuery for scalar/select/delete/update selection
  -> shared planner, access paths, executor, terminals
  -> SQL response conversion
```

Global and grouped aggregates have SQL-specific semantic command/lowering
layers because SQL projection and aggregate semantics are richer than the
public dynamic request. They still use accepted `SchemaInfo`, shared expression
and access planning, and the common executor/terminal machinery. No second
physical query engine was found.

Dynamic queries and typed/fluent reads live behind the engine-neutral `query`
feature. SQL depends on that feature, reuses the same
`StructuralProjectionContract` and `StructuralProjectionPayload`, and creates
`SqlStatementResult` only at its outward response boundary.

### 2.8 Recovery flow

```text
durable store/catalog/journal/marker state
  -> Db::ensure_recovered_state / commit::recovery::ensure_recovered
  -> accepted bundle and row-layout verification
  -> marker/journal replay or derived-state rebuild under explicit prepare mode
  -> normal accepted commit preparation and structural physical boundaries
```

Accepted fingerprints, snapshots, layouts, and source bindings remain the
semantic authority. Recovery does not call model derives or proposal
validators. Store routing comes from the sealed registry; entity tag/path/name
routing comes from the restored current accepted bundle. Unpublished
marker-bound candidates resolve their own source-bound entity identities
during replay, so a newly introduced entity can recover before it becomes the
current head. The recovery domain key contains the store-registry identity and
commit allocation only. `CommitPrepareMode` makes the lifecycle distinction
explicit: normal writes perform admission and derive effects, durable replay
rebuilds recorded effects without re-running target admission, and
derived-state rebuild performs neither candidate admission nor candidate
effects.

## 3. Package and feature graph

### 3.1 Important dependency direction

```text
icydb-schema                         (public schema/proposal/value contracts)
      ^
      |
icydb-model-macros
      ^
      |
icydb-model                          (authored graph and typed adapter generation)

icydb-diagnostic-code <--- icydb-core ---> icydb-schema
                              ^
                              |
                            icydb          (public facade)
```

Important observations:

- `icydb-core` has no normal dependency on `icydb-model` or its proc macros.
- `icydb-model` depends on schema contracts, not core persistence.
- The facade's non-wasm build/config path can depend on model generation, but
  the database runtime does not.
- Schema-only and model-only fixture packages exist.
- Core and facade default feature sets are empty.
- `query` selects engine-neutral accepted-schema planning, cache, execution,
  dynamic, and typed-read surfaces.
- `sql` depends on `query`; `sql-explain` depends on `sql`; diagnostics is
  separate.
- Typed-query canisters explicitly enable `icydb/query` without SQL.

### 3.2 Package-boundary verdict

**PASS:** source/model authoring is independently packageable and the core
query/persistence engine does not depend on model crates.

**PASS:** dynamic and typed reads compile and execute without SQL parsing,
lowering, DDL, mutation, or response DTO modules.

**PASS:** generated canister wiring supplies the sealed physical store registry
and optional frontend surfaces only. It no longer emits an entity route table.

No legacy feature alias or compatibility feature restoring generated
persistence was found.

## 4. Runtime authority and derived state

| Object | Canonical source | Binding/invalidation | Verdict |
| --- | --- | --- | --- |
| Accepted entity snapshots | durable accepted bundle | revision/fingerprint | canonical |
| Source bindings | durable accepted source-binding catalog | same publication as bundle | canonical |
| Row layouts/value catalogs | accepted snapshot | compiled per accepted fingerprint | canonical derived state |
| `AcceptedInspectionPlan` | accepted snapshot/catalog | identity includes revision/fingerprint | canonical derived state |
| `SchemaInfo` | accepted inspection projection | cached inside catalog context | necessary planner projection |
| Accepted row constraint program | accepted snapshot/catalog | exact fingerprint check during evaluation | canonical derived state |
| Plan cache | structural query + accepted identity/index visibility | key includes accepted revision/version/fingerprint | coherent |
| Name/source lookup cache | accepted inspection plan | current authority rechecked on hit | coherent locally |
| Accepted runtime entity catalog | current source-bound accepted bundles joined to sealed stores | reconstructed from the current head; exact lookups allocate only the matched route | canonical derived state |
| Typed entity binding | accepted state at issuance | incarnation/revision/fingerprint/generation | fail-closed, but name-heavy |
| Relation/index runtime plans | accepted snapshot/layout | built through inspection/commit context | canonical after entity route selection |
| Recovery domain key | store registry plus commit allocation | no generated entity topology | coherent |

The accepted caches inspected revalidate their current accepted authority and
plan keys include relevant accepted revision/fingerprint/index visibility.
No cache was found that silently falls back to authored schema, and no
separate generated entity domain remains.

## 5. Validator and constraint inventory

### 5.1 Ownership matrix

| Concern | Authoring owner | Accepted representation | Runtime evaluator | Phase | SQL | Structural | Typed | Integrity/activation | Diagnostic owner |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Rust construction invariants | user/model Rust | none | Rust constructor/type | before DB boundary | n/a | n/a | caller only | none | application |
| Authored graph shape | model/schema builders | proposal nodes | fragment/proposal validation | proposal construction | DDL has own syntax binding | proposal | generated proposal | none | schema |
| Proposal bounds/closure | `icydb-schema` | bounded proposal | `SchemaProposal` validation/composition | pre-application | yes through lowering | yes | yes | recovery decodes accepted only | schema |
| Candidate/reconciliation validity | core schema application | candidate + source bindings | application lowering/preflight | pre-publication | shared catalog transition | shared | shared | replay uses durable operation | schema/application |
| Accepted snapshot integrity | core catalog codecs | accepted snapshot/catalog | decode/integrity closure checks | load/recovery | shared | shared | shared | yes | corruption |
| Input type/domain admission | accepted row/value contracts | field/type/named catalogs | accepted value admission | mutation lowering/after-image | shared final authority | shared | shared | decode separately | input/boundary |
| Required / active `NOT NULL` | field nullable flag + constraint catalog identity | field contract + `AcceptedConstraintKind::NotNull` | fingerprint-bound compiled row program before non-null physical encoding and over representable final after-images | after mutation policy, before commit | shared | shared | shared | activation uses the same not-null program | `ConstraintDiagnostic` |
| Pending `NOT NULL` | DDL activation | activation snapshot | `CompiledAcceptedRowConstraints` | final after-image / activation scan | shared | shared | shared | same program | `ConstraintDiagnostic` |
| Primary key shape/non-null | entity schema | primary-key fields + constraint identity | accepted row/key derivation | final after-image | shared | shared | shared | catalog integrity | key/error taxonomy |
| Active unique index | index + constraint catalog identity | accepted index/constraint | identity-bound index commit planning over complete batch overlay | scheduler commit preflight | shared | shared | shared | activation has typed barrier | `ConstraintDiagnostic` |
| Relation integrity | relation + constraint catalog identity | accepted relation/constraint | identity-bound relation projections over complete batch overlay | scheduler commit preflight | shared | shared | shared | activation typed | `ConstraintDiagnostic` |
| Accepted scalar `CHECK` | source rule or SQL DDL | `AcceptedConstraintKind::Check` expression | compiled accepted row program | final after-image | shared | shared | shared | same semantic evaluator | `ConstraintDiagnostic` |
| Numeric bounds | authored source rule | ordinary accepted check expression | same compiled check program | final after-image | shared | shared | shared | same | `ConstraintDiagnostic` |
| Length bounds | authored source rule | ordinary accepted check expression | same compiled check program | final after-image | shared | shared | shared | same | `ConstraintDiagnostic` |
| Enum/named membership | authored type graph | accepted enum/composite/value catalogs | accepted value admission | boundary/final row | shared | shared | shared | decode/catalog checks | input vs corruption |
| List/set/map/tuple bounds | schema/value contract | accepted type/value bounds | bounded admission/codec | boundary/decode | shared | shared | shared | recovery decode | input vs corruption |
| Defaults | authored/DDL write policy | accepted field write policy | patch/after-image resolver | before constraints | shared | shared | shared | recovery never regenerates | mutation policy |
| Managed timestamps | explicit audit policy | accepted generated/database-owned fields | frozen-batch mutation policy | before constraints | shared | shared | shared | recovery preserves bytes | mutation policy |
| Explicit normalizers | application/model API | none | explicit caller invocation | outside DB mutation | not implicit | not implicit | explicit | none | application |
| Application validators | application/model API | none | explicit caller invocation | outside DB mutation | not implicit | not implicit | explicit | none | application |
| Mutation after-image validation | accepted contracts/program | compiled/runtime contracts | `AcceptedMutationConstraintScheduler` | policy-complete row-local evaluation, then storage preflight | shared | shared | shared | activation scans same scalar evaluator | DB |
| Physical row decode | accepted layout/value catalog | physical layout contract | bounded fallible row decoder | read/recovery | shared | shared | shared after output | recovery same boundary | corruption |
| Typed adapter compatibility | generated source contracts | opaque binding to accepted identity | binding issue/current checks | facade boundary | n/a | n/a | yes | none | typed boundary |

### 5.2 Semantic classes that must remain distinct

1. **Schema well-formedness:** reference resolution, placement, names, bounds,
   graph closure, and proposal/candidate validity. It runs before durable
   publication and is not a row constraint.
2. **Durable row-local constraints:** active/pending `NOT NULL`, accepted
   `CHECK`, and built-in numeric/length checks. These should share a compiled
   accepted program and final-after-image evaluator.
3. **Cross-row/entity constraints:** primary keys, unique indexes, and
   relations. They need overlay-aware storage access and maintenance. They
   should share accepted identity, scheduling, atomicity, and diagnostics, not
   the scalar expression evaluator.
4. **Database-owned mutation policy:** defaults and managed timestamps complete
   the after-image before constraints. They transform intent and are not
   validators.
5. **Explicit application operations:** normalizers and application validators
   stay outside durable enforcement unless separately authored as a database
   constraint.
6. **Boundary/corruption validation:** typed binding, wire/catalog decode, and
   physical row decode protect trust boundaries. A corrupt row is not a user
   constraint violation.

## 6. Constraint-unification assessment

### 6.1 What already exists

`CompiledAcceptedRowConstraints` is an exact-fingerprint-bound compiled program
for:

- active accepted `CHECK` rules;
- built-in numeric and length checks lowered into those rules;
- active accepted `NOT NULL` rules;
- pending `CHECK` activations;
- pending `NOT NULL` activations;
- pending unique write barriers.

`AcceptedMutationConstraintScheduler` is the shared write-enforcement
boundary. It receives policy-complete after-images, evaluates the compiled
row-local program, and produces an opaque batch for storage-backed commit
preflight. The activation runner uses the same compiled expression semantics,
and `evaluate_integrity_check` evaluates active checks by stable ordinal with
the same program.

This is a strong and mostly correct unification. New scalar checks do not
require edits to SQL, structural, and typed entry points.

### 6.2 R4/R5 consolidation result

R4 corrected the lifecycle split:

- accepted and activating `NOT NULL` rules compile into
  `CompiledAcceptedRowConstraints`;
- explicit null reaches the compiled accepted identity before a physically
  non-null slot can flatten it into a storage-admission error;
- active field-path and expression-index unique collisions carry the paired
  accepted constraint ID/name and key-item paths;
- active relation save/delete failures and pending relation projections carry
  the accepted or activation-reserved identity; and
- every live failure uses `ConstraintDiagnostic`, while physical decoding of
  an impossible persisted null remains corruption.

R5 then corrected orchestration and batch semantics:

- every normal save/delete commit window accepts an opaque
  `AcceptedMutationConstraintBatch`, so callers cannot bypass scheduling with
  raw row operations;
- row-local checks execute after defaults and managed timestamps;
- the commit overlay is pre-seeded with every final batch data image before
  unique and relation preflight;
- committed unique ownership is released only when that owner's final row
  moves or is deleted;
- active and pending relations compile to the same projection/evaluator shape;
- delete relation proof understands same-batch self-source deletion; and
- `CommitPrepareMode` distinguishes normal admission, durable replay, and
  derived-state rebuild.

Unique-index and relation evaluators remain specialized because their physical
proof and maintenance responsibilities differ. That separation is now below a
single lifecycle and atomicity boundary rather than a parallel write path.

### 6.3 Exact recommended boundary

The implemented boundary is:

```text
CompiledAcceptedRowConstraints (current owner)
  - exact AcceptedCatalogIdentity/fingerprint
  - compiled row-local checks
  - active and pending NOT NULL field/slot contracts
  - required logical slots
  - evaluate_final_after_image(...)
      -> Result<(), ConstraintDiagnostic>

AcceptedMutationConstraintScheduler
  - receives policy-complete final after-images/deletes
  - runs the row-local program
  - emits one opaque AcceptedMutationConstraintBatch

AcceptedStorageConstraintSchedule
  - active index plans and candidate unique barriers
  - active and pending relation projections
  - evaluates one complete final-row batch overlay
  - derives index/reverse-relation maintenance atomically
```

The precise unification is:

```text
accepted durable constraint identity
  -> fingerprint-bound runtime contract
  -> final-after-image scheduling
  -> specialized semantic/physical evaluator
  -> one ConstraintDiagnostic envelope
```

R4 evaluates active `NOT NULL` at the logical pre-encoding edge when
physical non-nullability makes `NULL` unrepresentable, then evaluates all
representable row-local final after-images after defaults and managed
timestamps. Physical decoding continues treating a persisted null in a
non-null slot as corruption.

Unique and relation evaluators must remain specialized because they need index
lookups, overlay awareness, reverse maintenance, and delete semantics. They
receive accepted constraint ID/name/field paths when compiled and emit the same
diagnostic envelope as activation.

No persisted-format or public API change was required. The scheduler is a
runtime-owned internal contract; it does not force overlay-aware
unique/relation proofs into the scalar row evaluator.

## 7. Query engine and Rust-struct separation

### 7.1 Verdict

**The planner, executor, and database session are independent of user Rust
structs below the typed boundary.**

No user entity generic, Rust `TypeId`, generated row decoder, derive validator,
Rust field name, Rust default implementation, or generated physical codec was
found below the facade typed boundary. `EntityAuthority`, `SchemaInfo`,
`StructuralQuery`, logical/access plans, prepared execution plans, cursors,
projections, grouping, ordering, and terminals use accepted/runtime identities
and engine-neutral values.

### 7.2 Surviving Rust/generated occurrences

| Occurrence | Classification |
| --- | --- |
| `Query<C, E>`/`PhantomData<E>` in facade | legitimate compile-time ergonomics |
| Generated field/source tokens used to request a typed binding | necessary boundary adapter |
| Typed output conversion from `OutputValue` | necessary boundary adapter |
| Generated source keys plus accepted IDs/slots stored in opaque typed binding | necessary boundary identity; direct after R6 |
| Old comments naming `Query<E>`/generated model contexts in core | documentation sediment |

### 7.3 Frontend convergence

| Layer | SQL | Dynamic/structural | Typed/fluent | Convergence |
| --- | --- | --- | --- | --- |
| Syntax/ergonomics | SQL AST | dynamic request | typed tokens/generics | intentionally distinct |
| Accepted identity/type binding | accepted `SchemaInfo` | accepted catalog | opaque accepted typed binding | semantically common, different adapters |
| Semantic query | `StructuralQuery` for normal selects | `StructuralQuery` | `StructuralQuery` | common |
| Logical/access planning | core planner | core planner | core planner | common |
| Execution/terminals | core executor | core executor | core executor | common |
| Mutation intent | accepted patches | accepted patches | accepted patches after adapter | common |
| After-image/constraints/commit | structural save/delete pipeline | same | same | common |
| Result conversion | structural payload -> SQL statement adapter | structural payload -> dynamic rows | dynamic rows -> typed decoder | one engine-neutral payload, intentionally distinct outward adapters |

SQL aggregate command/lowering is a justified frontend semantic layer, not a
separate access or physical engine.

### 7.4 Negative-space proof

R2 removes the generated registration type and constructor entirely, so every
maintained runtime test now constructs sessions from a store registry only.
Focused coverage publishes a source-bound accepted entity and resolves it by
tag, source path, display name, and deterministic enumeration. The live-only
interruption test now introduces a new entity in the marker candidate, clears
the live catalog, recovers it, and resolves the entity from the restored
accepted head. Relation target resolution calls the same accepted runtime
catalog. R6 coverage proves immutable typed entity/field binding across renames,
old-name reuse, missing differently sourced fields, and cross-binding patch
rejection, then executes a direct ID/slot typed insert through the shared
mutation pipeline. R7 extends that same regression through query-only dynamic
execution and adds a facade compile-pass contract plus maintained generated
canisters that do not enable SQL.

## 8. Redundant-flow and sediment inventory

| Item | Evidence/reachability | Consequence | Decision |
| --- | --- | --- | --- |
| Generated runtime entity registration gate | initially reachable from accepted lookup, catalog listing, commit, relation, diagnostics, and recovery | accepted entity could be durable but invisible | **deleted in R2**; current source-bound accepted bundles own entity routing |
| Generated-vs-accepted schema check | initially reachable through the public DTO and CLI command | called a method current canisters did not generate | **deleted in R1** |
| Active constraint diagnostic paths | initially active not-null admission, index uniqueness, and relation save/delete discarded accepted identity | lifecycle-dependent public errors | **consolidated in R4** around accepted identity and `ConstraintDiagnostic` |
| Family-specific mutation scheduling | row-local save validation, session relation admission, commit index planning, and a parallel active reverse-relation walker formed separate lifecycle seams | final batch semantics depended on row order and new rules required multiple entry-point edits | **consolidated in R5** under `AcceptedMutationConstraintScheduler` and `AcceptedStorageConstraintSchedule`; specialized physical evaluators retained |
| `SemanticIndexKeyItems::{Fields, Accepted}` | initially constructed from accepted `SchemaInfo` with many downstream dual matches | repeated planner logic and drift risk without distinct authority | **consolidated in R3** to one ordered `Vec<SemanticIndexKeyItem>` |
| Name-based typed binding map | initially `DynamicTypedEntityBinding` stored source-to-display-name and write/current checks resolved names again | source/name/slot churn; rename could surface unsupported lookup rather than uniform stale binding | **consolidated in R6** to immutable entity source/tag and field ID/slot bindings; names remain output/value labels |
| Query API under `sql` | initially core/facade dynamic query modules and typed-query canisters required `sql`, and dynamic execution used SQL DTOs | unnecessary parser/DTO feature coupling and footprint | **consolidated in R7** under `query`, `StructuralProjectionContract`, and `StructuralProjectionPayload`; SQL is a sibling adapter |
| Generated origin in query `SchemaInfo` | initially projected solely so `SHOW INDEXES` could render accepted origin | catalog display policy leaked into planner metadata | **moved in R3** to direct persisted-snapshot lookup at the introspection boundary |
| Stale generic/generated planner comments | comments named `Query<E>`, `PreparedExecutionPlan<E>`, generated fallback/model context | taught an architecture the code no longer implements | **corrected in R3** with current accepted/generic-free terms |
| SQL DDL transition machinery | catalog-native transition and generated/user origin flags | distinct mutation frontend/lifecycle, not duplicate authority | retain |
| `SchemaInfo` versus accepted snapshot | fingerprint-bound query projection | planner-specific derived view; avoids persistence/model coupling | retain |
| accepted rich index wrappers plus semantic contracts | both used for candidate discovery/order and selected access semantics | possibly necessary derived views; no proven duplicate owner | retain pending a narrower construction audit |
| application normalizers/validators | model/application visitors only; no core mutation call sites | explicit application behavior | retain separately |

No generated physical row codec, generated persistence trait, runtime validation
callback registry, authored-schema fallback in recovery, or typed-only planner
was found.

## 9. Representation and conversion inventory

| Domain | Representations and purpose | Assessment |
| --- | --- | --- |
| Schema | model nodes -> `SchemaFragment` -> `SchemaProposal` -> candidate revision -> accepted bundle/snapshot -> runtime projections | necessary authority stages; no reverse conversion to model |
| Entity identity | source key -> accepted entity ID/tag + registry-owned store -> display name | coherent; current accepted head is the sole entity discovery authority |
| Field identity | source key -> accepted field ID/slot; display name projected separately as output metadata | coherent after R6 |
| Types | public scalar/named proposal type -> accepted value catalogs -> runtime `Value` contracts | necessary boundary/persistence stages |
| Expressions | typed/SQL syntax -> accepted-bound expression/predicate -> logical/access/executable artifacts | necessary compiler stages |
| Index keys | accepted field-path or expression index -> ordered `Vec<SemanticIndexKeyItem>` -> borrowed item view | coherent owned/borrowed semantic boundary after R3 |
| Rows | `InputValue` -> runtime `Value` -> accepted canonical row -> physical `RawRow`; reverse to `OutputValue` | necessary, fallible trust and storage boundaries |
| Write patches | structural cells -> `DynamicStructuralPatch`; typed cells -> fingerprint-bound ID/slot patch; both -> `AcceptedMutationIntentPatch` | coherent distinct frontend adapters converging before policy |
| Constraint programs | accepted expression/catalog -> fingerprint-bound row program and storage schedule -> opaque mutation batch | canonical after R5; storage-backed evaluators remain specialized below one scheduler |
| Diagnostics | accepted constraint identity -> `ConstraintDiagnostic` for active and pending write admission; internal typed errors for corruption/boundary failures | coherent after R4 |

No flow was found that converts SQL values through a user Rust entity or
structural rows through generated physical adapters.

## 10. Findings by severity

### P0

No P0 finding.

### P1-001: Generated registration was a closed-world runtime entity owner — corrected in R2

**Initial evidence**

- `Db<C>` stores `&'static [EntityRegistration<C>]`.
- `resolve_runtime_registration_by_path/tag` begins with that slice.
- accepted source/name lookup scans it in
  `db/session/accepted_schema.rs`.
- `show_entities`, commit preparation, delete relation source scans,
  diagnostics, integrity, rebuild, and recovery use the same registration
  domain.
- `preflight_initial_application` proves store emptiness/readiness but does not
  prove proposal entities equal registered routes.
- Public `DbSession::apply_schema(&SchemaProposal)` can publish source-keyed
  proposals independent of generated entity descriptors.
- Tests apply an initial schema with an empty registration list, while the
  relation negative test explicitly requires registered target authority.

**Affected paths/symbols**

- `crates/icydb-core/src/db/entity_registration.rs`
- `crates/icydb-core/src/db/mod.rs::Db`
- `crates/icydb-core/src/db/session/accepted_schema.rs`
- `crates/icydb-core/src/db/session/catalog.rs::show_entities`
- `crates/icydb-core/src/db/commit/{recovery,rebuild}.rs`
- `crates/icydb-core/src/db/diagnostics/storage_report.rs`
- `crates/icydb-core/src/db/relation/`

**Violated invariant**

The durable accepted catalog must enumerate every runtime entity and route; no
authored/generated source may be a second reachability gate after acceptance.

**Concrete consequence before correction**

An accepted entity absent from generated registrations can be durably
published, then fail name/source lookup, disappear from catalog listings, or
fail relation, integrity, commit, or recovery routing. Fail-closed behavior
limits corruption risk but does not restore authority coherence.

**Correction**

`AcceptedRuntimeEntity` now joins each source-bound snapshot in the current
accepted bundle to its registry-owned store. Exact tag/path/name lookup,
catalog listing, commit preparation, relation work, diagnostics, integrity,
rebuild, and recovery use that owner. Unpublished marker candidates resolve
their own source-bound identity during replay. `EntityRegistration`,
`new_with_registrations`, its generated static table, and the registration
pointer in recovery-domain identity are deleted.

### P1-002: Storage-backed batch constraints observed row-order state — corrected in R5

**Initial evidence**

- active relation targets were admitted against committed storage while save
  rows were still being assembled;
- unique preflight staged each data-row override only after preparing that row,
  so the final transition of a later committed owner was invisible;
- relation delete proof treated a self-source row deleted by the same batch as
  a surviving reference; and
- normal write, recovery replay, and derived rebuild expressed their different
  admission/effect responsibilities with booleans and call-site convention.

**Affected paths/symbols**

- `crates/icydb-core/src/db/session/write.rs`
- `crates/icydb-core/src/db/executor/mutation/commit_window.rs`
- `crates/icydb-core/src/db/index/plan/unique.rs`
- `crates/icydb-core/src/db/relation/{reverse_index,validate}.rs`
- `crates/icydb-core/src/db/commit/{prepare,recovery,rebuild}.rs`

**Violated invariant**

Atomic mutation constraints must evaluate the complete final batch state, not
the committed state plus an input-order prefix.

**Concrete consequence before correction**

Valid unique-key swaps/cycles, forward or self-referential relation inserts,
and self-referential deletes could fail closed depending on row order. The
failure happened before marker acquisition, so no partial durable mutation or
corruption resulted.

**Correction**

`AcceptedMutationConstraintScheduler` now owns policy-complete row admission
and emits the opaque batch required by normal commit windows.
`PreflightStoreOverlay` is pre-seeded with every final data image before
storage-backed proof. Unique ownership is released only when the existing
owner's final row moves or is deleted, active/pending relations use the same
projection evaluator, and delete proof recognizes same-batch self-source
deletion. `CommitPrepareMode` gives normal writes, durable replay, and
derived-state rebuild explicit, non-overlapping responsibilities.

### P2-001: Retired generated-versus-accepted schema-check surface — corrected in R1

**Initial evidence**

The core/facade publicly export `EntitySchemaCheckDescription`. The CLI
configures and invokes `icydb_schema_check` and has a large analysis/test
surface. Current model actor generation emits only `icydb_schema` and asserts
that `icydb_schema_check` is absent.

**Violated invariant**

Pre-1.0 retired dual-authority comparison paths must be deleted, and maintained
CLI features must target generated canister surfaces.

**Consequence before correction**

The CLI exposes a command that current generated canisters cannot serve, and
tests keep obsolete generated-runtime-schema concepts alive.

**Correction**

R1 hard-deleted the DTO, exports, CLI command/configuration,
decoder/renderer/analyzer, and obsolete tests. Accepted catalog inspection is
the only maintained schema-observability surface.

### P2-002: Active durable constraints did not share accepted diagnostics — corrected in R4

**Initial evidence**

- active `NOT NULL` is absent from `CompiledAcceptedRowConstraints`;
- value admission flattens relevant failures to `executor_unsupported`;
- active unique conflict arguments are discarded by
  `InternalError::index_violation`;
- active relation missing-target/delete failures flatten to generic unsupported;
- activation variants of the same constraint families emit
  `ConstraintDiagnostic`.

**Violated invariant**

Every accepted durable constraint must preserve accepted identity and emit one
diagnostic envelope irrespective of frontend or activation state.

**Consequence**

Clients cannot reliably identify the violated accepted constraint, and
activation/runtime diagnostics can drift even though SQL/structural/typed
mutation atomicity is shared.

**Correction**

R4 adds accepted `NOT NULL` to the fingerprint-bound row program, evaluates
unrepresentable explicit null before physical encoding, and projects stable
accepted identity into active unique/relation runtime contracts. Active and
pending failures now use `ConstraintDiagnostic`; the specialized
unique/relation evaluators and their metrics/physical maintenance remain
intact. Generic active relation and argument-discarding unique failure
constructors were removed.

### P3-001: Semantic index keys had two accepted-derived representations — corrected in R3

**Evidence**

`SemanticIndexKeyItems::Fields(Vec<String>)` was built for accepted field-path
indexes and `Accepted(Vec<SemanticIndexKeyItem>)` for accepted expression
indexes. Downstream planner/access modules repeatedly branched over both, while
`SemanticIndexKeyItem` already represented fields and expressions.

**Consequence**

Every new access capability must update parallel branches, increasing
planner-drift risk.

**Correction**

Field-path and expression indexes now construct the same ordered
`Vec<SemanticIndexKeyItem>`. Prefix/range selection, covering projection,
ordering, slot compilation, canonical comparison, and explain projection walk
that representation directly. The collection enum and its borrowed dual are
deleted.

### P3-002: Typed binding used editable names below immutable source binding — corrected in R6

**Initial evidence**

Before R6, `DynamicTypedEntityBinding` stored source-to-display-name maps and
currentness reopened the accepted catalog by entity display name.

**Consequence before correction**

Extra resolution work, inconsistent stale-binding errors after rename, and
avoidable dependence on editable labels below the adapter issuance boundary.

**Correction**

`DynamicTypedEntityBinding` now retains immutable entity source/tag and each
field's source key, durable accepted ID, slot, and output label under the exact
accepted revision/fingerprint/layout generation. Typed writes carry an opaque
ID/slot patch directly into accepted mutation lowering; typed queries select
the catalog through immutable binding identity. Currentness rechecks entity and
field source bindings, and cross-binding patches fail closed. Names remain only
for output and public named-value labels.

### P3-003: Dynamic/typed query support was coupled to SQL feature and DTOs — corrected in R7

**Evidence**

Before R7, core and facade dynamic query modules were
`cfg(feature = "sql")`, dynamic execution imported `SqlStatementResult`, and
typed-query canisters enabled `icydb/sql`.

**Consequence**

Using the engine-neutral query API pulls in SQL frontend code and prevents a
clean feature-level statement that the query engine is independent of SQL.

**Action**

R7 adds `query`, makes `sql` depend on it, moves the projection contract and
payload into the query session, and deletes `SqlProjectionPayload`. Dynamic
execution converts the structural payload directly; SQL constructs its
statement result only at the SQL response boundary. Query-only compile and
runtime coverage prove the separation.

### P3-004: Catalog provenance leaked into query metadata — corrected in R3

**Evidence**

`SchemaIndexInfo.generated` and `SchemaExpressionIndexInfo.generated` were
consumed only by `SHOW INDEXES` formatting and retained dead-code expectations
when SQL was disabled.

**Consequence**

Catalog presentation policy occupied the query projection and made origin look
like a planning input even though no planner decision consumed it.

**Correction**

The query projection fields/accessors are deleted. `SHOW INDEXES` now resolves
origin by accepted ordinal from the same persisted snapshot used to construct
the query projection, retaining the user-visible `generated`/`ddl` label
without making it planner metadata.

### P4-001: Comments preserved retired generic/generated architecture — corrected in R3

Core comments still refer to `Query<E>`, `PreparedExecutionPlan<E>`, generated
model contexts, and generated fallbacks where the surviving types are
generic-free and accepted-driven.

**Correction**

The affected planner, executor, and SQL-lowering comments now describe
frontend binding, `StructuralQuery`, and accepted schema without naming
deleted generic execution shells or generated fallback authority.

## 11. Comparison with the 0.213 closeout

This comparison was performed after the independent reconstruction.

The closeout correctly describes:

- the core/model package split;
- accepted row layouts and codecs as physical authority;
- catalog-native schema application;
- common SQL/structural/typed mutation after-images;
- removal of implicit callbacks and generated physical persistence;
- common accepted-check/numeric/length evaluation.

The initial audit found two claims stronger than the code then supported:

1. Its authority matrix said accepted entity snapshots were used by all
   reads/writes/SQL/integrity with no reachable duplicate authority. R2 now
   enforces that claim by removing generated entity routing.
2. Its constraint parity statements remain accurate for accepted `CHECK` rules and
   built-in numeric/length rules, but not for the identity/diagnostic treatment
   of active `NOT NULL`, unique, and relation constraints.

Later proposed 0.214-0.216 designs have not altered the current implementation.
The proposed 0.216 taxonomy correctly keeps source rules, application
validators, and normalizers distinct; this report does not recommend merging
them or encoding `NOT NULL` as a source-rule expression.

## 12. Negative-space test plan

Add the following tests as part of the owning remediation slices:

1. Construct a store-only `DbSession`, publish a source-bound accepted entity,
   then perform dynamic query, structural insert/update/delete, catalog
   inspection, integrity inspection, and recovery.
2. The same scenario through SQL; no generated entity route API exists.
3. Accepted relation targets resolve from accepted catalogs.
4. Removing typed adapters from a test canister does not affect dynamic/SQL
   operation.
5. A stale typed binding after entity/field rename fails with one explicit
   stale-binding result, while a newly issued source-bound adapter succeeds.
   R6 covers this together with old-name reuse and cross-binding rejection.
6. Active and pending `NOT NULL`, unique, relation, and `CHECK` violations expose
   accepted constraint ID/name/kind consistently through SQL, structural, and
   typed writes.
7. Batch updates evaluate policy-complete after-images against one complete
   final-row overlay. R5 adds direct coverage for later-row relation targets,
   same-batch deletes, unique owner movement/deletion, preserved final unique
   collisions, and self-referential relation deletion.
8. Integrity and activation evaluate the same check truth table/work budget as
   normal mutation.
9. A query-only feature compile fixture builds typed/dynamic reads without SQL
   parser/DDL surfaces.
10. Feature-tree compile fixtures prove no core feature restores a model or
    generated persistence dependency.

Existing tests already give useful evidence for shared checks, activation,
after-image batching, schema-only proposal construction, model typed adapter
construction, accepted cache fingerprinting, accepted-only runtime entity
routing, and marker recovery of a newly introduced accepted entity.

## 13. Sequenced remediation plan

Each step was one reviewable slice with direct tests and documentation.
R1 through R7 completed the correctness, typed-boundary, and feature-boundary
findings without a broad rewrite.

### Phase A: Safe deletion

#### A1. Delete generated-versus-accepted schema-check — complete (R1)

- Modules/types: `EntitySchemaCheckDescription`; core/facade exports;
  `crates/icydb-cli/src/observability/schema_check/`; endpoint config, command
  dispatch, help, tests.
- Current problem: maintained CLI calls a deliberately absent generated method.
- Desired invariant: only accepted-catalog schema inspection is public.
- Dependencies: none.
- Persisted format: none.
- Public API: hard deletion of obsolete pre-1.0 API/CLI command.
- Tests: active `icydb_schema` inspection and CLI command enumeration.
- Deletion enabled: the complete schema-check analysis/rendering tree.
- Principal risk: stale documentation/help references.
- Scope: medium.

#### A2. Remove planner-irrelevant query provenance and stale terminology — complete (R3)

- Modules/types: `SchemaIndexInfo`, `SchemaExpressionIndexInfo`, adjacent
  accepted planner/executor comments.
- Initial problem: catalog-only generated-origin projection in planner metadata
  and obsolete generic descriptions.
- Desired invariant: query metadata contains only planner-consumed accepted
  facts.
- Dependencies: none.
- Persisted/public impact: none.
- Tests: focused core check/clippy and SchemaInfo construction tests.
- Deletion enabled: dead-code expectations/accessors.
- Principal risk: preserving accepted origin in `SHOW INDEXES`; R3 now reads it
  directly from the persisted snapshot.
- Scope: small.

### Phase B: Mechanical consolidation

#### B1. Canonicalize semantic index key items — complete (R3)

- Modules/types: `db/access/path.rs::SemanticIndexKeyItems{,Ref}` and all
  consumers in access, order, prefix/range, covering, logical semantics, and
  executor planning.
- Initial problem: parallel field-only and accepted item paths expressed one
  semantic key contract.
- Desired invariant: one ordered `Vec<SemanticIndexKeyItem>` for every accepted
  index.
- Dependencies: none.
- Persisted/public impact: none.
- Tests: planner route/index-prefix/covering/order suites for field and
  expression indexes.
- Deletion enabled: dual enums and repeated match arms.
- Principal risk: accidentally changing field-path ordering or expression-index
  capability checks.
- Scope: medium.

### Phase C: Constraint-pipeline consolidation

#### C1. Preserve accepted identity in active unique/relation diagnostics — complete (R4)

- Modules/types: `IndexPlanError`, unique index commit plans, accepted relation
  save/delete plans, `ConstraintDiagnostic`.
- Initial problem: active conflicts discarded catalog identity while pending
  barriers retain it.
- Desired invariant: every active/pending durable constraint violation carries
  accepted ID/name/kind/entity/key/field paths when available.
- Implementation: accepted index/relation plans compile the sole paired
  constraint or activation identity once from the snapshot and fail closed on
  absent or ambiguous ownership.
- Persisted format: none.
- Public API: more precise existing error detail.
- Tests: frontend parity and active-versus-activation identity.
- Deletion enabled: generic index/relation violation constructors that discard
  arguments.
- Principal risk: preserving metrics attribution while replacing error wrappers.
- Scope: medium.

#### C2. Make active `NOT NULL` a logical row-program check — complete (R4)

- Modules/types: `CompiledAcceptedRowConstraints`, accepted row admission,
  canonical row construction, and the R5
  `AcceptedMutationConstraintScheduler`.
- Initial problem: physical non-nullability rejected before accepted constraint
  diagnostics.
- Desired invariant: logical final after-images evaluate active/pending
  row-local constraints once; physical decode still treats impossible nulls as
  corruption.
- Implementation: active and pending not-null contracts share the
  fingerprint-bound compiled program. Explicit null is evaluated immediately
  before non-null encoding; representable after-images use the ordinary shared
  evaluator after database policy.
- Persisted format: none.
- Public API: diagnostic precision only.
- Tests: omission/null/default/update after-image parity for all frontends.
- Deletion enabled: special diagnostic mapping for null admission.
- Principal risk: avoiding duplicate row decode/materialization and preserving
  final-after-image ordering.
- Scope: medium.

#### C3. Introduce an accepted mutation-constraint scheduler — complete (R5)

- Modules/types: row constraint program, unique/index plans, relation plans,
  save/delete preparation.
- Initial problem: semantic checks shared atomic commit but not one explicit
  scheduling/lifecycle artifact, and storage-backed proof saw only a row-order
  prefix of the final batch.
- Desired invariant: new durable constraints plug into one final-after-image
  scheduling contract while specialized physical evaluators remain separate.
- Dependencies: C1-C2.
- Implementation: `AcceptedMutationConstraintScheduler` evaluates row-local
  policy-complete after-images and emits an opaque batch;
  `AcceptedStorageConstraintSchedule` compiles unique/relation work;
  `PreflightStoreOverlay` exposes all final data images before proof; and
  `CommitPrepareMode` distinguishes normal, replay, and rebuild semantics.
- Persisted/public impact: none.
- Tests: complete-batch relation targets and deletes, unique ownership
  movement/final collision, self-relation deletion, prepare-mode behavior, and
  existing activation/integrity suites.
- Deletion enabled: standalone save validation, separate active relation
  validation, and the parallel active reverse-relation transition walker.
- Principal risk: over-generalizing storage proofs into scalar validation.
- Scope: large; completed under the bounded R5 design.

### Phase D: Query/type boundary cleanup

#### D1. Replace generated registration gating with accepted runtime catalog — complete (R2)

- Modules/types: `Db::entity_registrations`,
  `GeneratedEntityRoute`/`EntityRuntimeRegistration`, accepted catalog lookup,
  catalog listing, commit context, relations, integrity, diagnostics, recovery,
  rebuild, cache identity.
- Initial problem: accepted schema was not the complete runtime entity set.
- Desired invariant: after publication, stores plus accepted bundles/source
  bindings enumerate all entities and routes; generated wiring cannot make an
  accepted entity invisible.
- Implementation: runtime entity paths and affected cache/commit identities
  own `Rc<str>` handles; store paths remain registry-owned `&'static str`.
  Exact lookups scan accepted store catalogs without materializing the full
  entity list.
- Persisted format: none expected. Stop if route reconstruction needs data not
  durably present.
- Public API: `new_with_registrations` and `EntityRegistration` are deleted;
  generated sessions take only `StoreRegistry`.
- Tests: all registration-free negative-space tests in section 12, plus
  interruption/recovery and mixed-store relations.
- Deletion completed: registration scans and module, pointer-based recovery
  entity-domain identity, generated route wrappers, and generated route table.
- Principal risk: multi-store enumeration, cache ownership/lifetimes, and
  recovery before all stores are ready.
- Scope: large; completed without a persisted-format change.

#### D2. Bind typed adapters directly to accepted IDs/slots — complete (R6)

- Modules/types: `DynamicTypedEntityBinding`, facade typed read/write adapters,
  accepted binding issuance/currentness.
- Initial problem: immutable source identity was converted through editable
  names back to slots.
- Desired invariant: typed adapters lower directly to fingerprint-bound accepted
  entity/field identities; labels remain output metadata.
- Dependencies: D1's accepted runtime identity owner.
- Persisted format: none.
- Implementation: binding issuance stores entity source/tag and field
  source/ID/slot under the accepted revision/fingerprint/layout generation;
  typed writes carry a binding-owned ID/slot patch, typed queries select the
  catalog by binding identity, and output labels map to slots only at result
  conversion.
- Public API: opaque binding internals only; stale failures are uniform.
- Tests: rename, old-name reuse, stale binding, cross-binding patch rejection,
  structurally similar differently sourced fields, and direct typed insert.
- Deletion enabled: source-to-display-name maps for mutation lowering and
  entity-name routing for typed execution.
- Principal risk: result-column matching for projections/aliases.
- Scope: medium; completed without a format change.

#### D3. Separate engine-neutral query support from SQL — complete (R7)

- Modules/features: core/facade Cargo features, session query modules,
  `DynamicQueryResult`, SQL response conversion, typed-query canister manifests.
- Initial problem: structural/typed reads required SQL frontend/DTOs.
- Desired invariant: SQL and typed/dynamic are sibling frontends over a query
  feature/core, not owner and dependent.
- Dependencies: none semantically; land after D1 to test truly schema-only
  dynamic operation.
- Persisted format: none.
- Public API: additive/replacement feature contract before 1.0; no aliases.
- Implementation: core/facade `query` owns the planner/executor/cache and
  structural projection boundary; `sql` depends on it and retains parser,
  lowering, SQL mutation/DDL, diagnostics, and response adaptation.
- Tests: no-default + query-only + SQL + SQL-explain feature matrix,
  query-only facade compile-pass, accepted-ID dynamic runtime execution, and
  one-/ten-entity generated query canisters.
- Deletion completed: dynamic dependence on `SqlStatementResult`, the
  duplicate `SqlProjectionContract` and `SqlProjectionPayload`, and the
  SQL-owned public dynamic-projection bridge.
- Principal risk: feature matrix and result-limit behavior divergence.
- Scope: medium; completed without a persisted or response-format change.

### Phase E: Deeper design work

#### E1. Accepted runtime identity ownership — resolved by R2

`AcceptedRuntimeEntity` owns entity source path and display name through
`Rc<str>` and borrows only registry-owned static store paths. Accepted
catalog/cache identities and commit markers use owned path handles where
required. No accepted, marker, cursor, row, or catalog format changed.

#### E2. Future nested durable constraints

Nested/member constraints need a separately designed accepted target/access
contract and bounded evaluator. Do not extend `SourceRule` or the row program
incidentally until placement identity, diagnostics, activation, and integrity
semantics are designed.

- Persisted impact: likely yes.
- Public impact: authoring and diagnostics.
- Scope: large.

Post-audit disposition: 0.213.39 N1 freezes that contract in
[0.213.39-nested-durable-constraint-targets.md](../../../../../../../design/0.213-schema-authority-and-application-model-separation/0.213.39-nested-durable-constraint-targets.md).
The design uses a persisted-root plus nominal accepted-type selector and a
bounded finite-value traversal rather than an expanded path through a
potentially cyclic schema graph. N2 now owns the source contract and N3 binds
and persists the accepted field/type identities. N4 owns the shared iterative
finite-value evaluator, accepted scalar semantics, typed concrete paths, and
resource bounds. N5 integrates that artifact into the sole final-after-image
mutation scheduler across dynamic, typed, SQL, defaulted, managed-timestamp,
and batch writes. N6 now extends that same accepted artifact through
historical activation and integrity, persists bounded typed finding paths, and
reconstructs only from durable accepted/job authority during recovery. N7
owns maintained-consumer closeout and final release evidence.

## 14. Corrections made during this audit

The initial audit made no production change. Subsequent bounded remediation
has completed:

- R1 deleted `EntitySchemaCheckDescription`, the retired CLI command and
  endpoint configuration, its analysis/rendering implementation, and obsolete
  preservation tests.
- R2 replaced generated entity routing with `AcceptedRuntimeEntity`, projected
  from source-bound snapshots in current accepted bundles and joined only to
  sealed registry-owned stores. It propagated that owner through catalog
  lookup, commit, relations, integrity, diagnostics, SQL/structural session
  lookup, rebuild, and recovery.
- R2 deleted `EntityRegistration`, `new_with_registrations`, generated
  `ENTITY_REGISTRATIONS`, generated-route fallback, and recovery-domain
  registration-pointer identity.
- Runtime paths now own accepted entity strings where static generated paths
  had leaked below the boundary. Exact lookup avoids full-catalog
  materialization; full enumeration remains for catalog listing and
  relation-domain proof.
- Marker recovery validates and routes a newly introduced entity from the
  unpublished source-bound candidate, then uses the restored accepted head.
- R3 replaced the two field-only/mixed semantic index-key collection shapes
  with one ordered key-item vector across access selection, ordering, covering,
  logical slot compilation, and executor pushdown.
- R3 removed catalog origin from `SchemaInfo`; index introspection now reads
  the accepted persisted snapshot directly and preserves the same visible
  origin labels. Obsolete generic/generated planner comments were corrected.
- R4 projects stable accepted constraint identity into active unique-index and
  relation runtime contracts, including activation-reserved relation
  projections.
- R4 compiles active and pending `NOT NULL` into the fingerprint-bound row
  program and preserves not-null identity before non-null physical encoding.
- R4 routes active/pending not-null, unique, relation, and check failures
  through `ConstraintDiagnostic`, deleting the generic active relation-target
  error and argument-discarding live unique violation path.
- R5 introduces `AcceptedMutationConstraintScheduler`; every normal save/delete
  commit window now accepts its opaque constraint batch instead of raw row
  operations.
- R5 pre-seeds the preflight overlay with complete final batch data images,
  making unique swaps/moves, later-row relation targets, and same-batch deletes
  independent of input order while preserving final collisions.
- R5 compiles active and pending relation work through one projection
  evaluator, deletes the standalone save/relation validators and parallel
  active reverse-transition walker, and makes normal/replay/rebuild prepare
  behavior explicit with `CommitPrepareMode`.
- R6 replaces typed source-to-display-name field maps with source-bound durable
  field IDs and accepted row slots, and rechecks every identity when proving
  binding currentness.
- R6 routes typed writes through an opaque `DynamicTypedMutation` ID/slot patch
  and typed queries through immutable binding-owned entity selection. Returned
  names remain output/value labels only; structural and SQL name-driven
  frontends are unchanged.
- R7 introduces the engine-neutral `query` feature, makes `sql` depend on it,
  and moves dynamic/typed reads plus the shared plan/projection boundary below
  SQL parsing and response ownership.
- R7 deletes `SqlProjectionContract` and `SqlProjectionPayload`; SQL and
  dynamic adapters consume one `StructuralProjectionPayload`, while SQL write
  selectors may consume its runtime rows without SQL output conversion.
- R7 moves the maintained typed-query canisters to query-only dependencies and
  adds query-only compile and runtime negative-space coverage.

## 15. Validation evidence

### 15.1 Initial read-only audit executed successfully

| Command | Outcome | Evidence |
| --- | --- | --- |
| `cargo fmt --all --check` | PASS | repository formatting is clean |
| `cargo check -p icydb-schema` | PASS | schema package |
| `cargo check -p icydb-model --no-default-features` | PASS | model/macro/schema authoring boundary |
| `cargo check -p icydb-core --no-default-features` | PASS | core without frontend features |
| `cargo check -p icydb-core --no-default-features --features sql` | PASS | core SQL frontend |
| `cargo check -p icydb-core --features sql-explain,diagnostics` | PASS | explain and diagnostics combination |
| `cargo check -p icydb --no-default-features` | PASS | facade minimal surface |
| `cargo check -p icydb --features sql` | PASS | facade SQL surface |
| `cargo check -p icydb-testing-model-schema-only` | PASS | schema-only compile fixture |
| `cargo check -p icydb-testing-model-typed-adapter` | PASS | typed-adapter compile fixture |
| `cargo check -p canister_audit_one_entity_typed_query` | PASS | generated typed-query canister |
| `cargo check -p icydb-cli` | PASS | CLI including stale schema-check command |
| `cargo test -p icydb-core pending_generated_check_abort_is_atomic_terminal_and_replayable --lib -- --test-threads=1` | PASS | 1 passed; also proves initial application can be constructed with an empty entity-registration slice |
| `cargo test -p icydb-core accepted_relations_require_registered_target_authority --lib -- --test-threads=1` | PASS | 1 passed; confirms the current registration-gated relation contract |
| `cargo test -p icydb-core db::schema::check::tests --lib -- --test-threads=1` | PASS | 14 passed; compiled checks, pending not-null/unique barriers, integrity semantics, bounds |
| `cargo test -p icydb-model build::actor::db::schema::tests --lib -- --test-threads=1` | PASS | 1 passed; generated schema surface explicitly omits `icydb_schema_check` |
| `cargo test -p icydb-cli schema_check -- --test-threads=1` | PASS | 11 passed; demonstrates the obsolete CLI architecture remains compiled and protected |
| `cargo clippy -p icydb-core --features sql --lib -- -D warnings` | PASS | no core SQL library warnings |
| `cargo clippy -p icydb --features sql --lib -- -D warnings` | PASS | no facade SQL library warnings |
| `cargo tree -p icydb-core -e normal,features --no-default-features --depth 2` | PASS | no model/macro dependency in core |
| `cargo tree -p icydb -e normal,features --features sql --depth 2` | PASS | facade/core/schema/config direction |
| `cargo tree -p icydb-model -e normal,features --depth 2` | PASS | model depends on macro/schema, not core |
| `cargo tree -p icydb-schema -e normal,features --depth 2` | PASS | schema package remains persistence-free |
| `make check-invariants` | PASS | dependency graph, no-production-panics, generated build, index range, layer authority, mutation atomicity, durability docs, read admission, schema/model, SQL branch, and memory-ID scripts |

### 15.2 Repository searches and static inspection

The following searches were evidence-gathering commands. A successful command
means the search executed; matches were inspected and classified rather than
treated as failures:

- `rg -n "EntitySchemaCheckDescription|SCHEMA_CHECK_ENDPOINT|icydb_schema_check" crates docs --glob '!docs/reports/**'`
- `rg -n "enum SemanticIndexKeyItems|SemanticIndexKeyItems::(Fields|Accepted)|struct DynamicTypedEntityBinding|CompiledAcceptedRowConstraints" crates/icydb-core crates/icydb`
- `rg -n "entity_registrations|EntityRegistration|GeneratedEntityRoute" crates/icydb-core/src/db --glob '*.rs'`
- `rg -n "EntityModel|IndexModel|TypeId|PhantomData<[^>]*E|sanitizer|legacy|fallback|callback|generated row|physical row bridge|persistence adapter" crates/icydb-core/src/db --glob '*.rs'`
- `rg -n "#\\[cfg\\(feature = \"sql\"\\)\\]|SqlStatementResult|SqlProjectionPayload" crates/icydb-core/src/db/session/query crates/icydb/src/db/query crates/icydb/src/db/session/query.rs`
- construction- and call-site searches for schema application, accepted catalog
  contexts, structural mutations, constraint evaluation, integrity, activation,
  unique/index errors, relation errors, recovery, and publication.

`cargo metadata --no-deps --format-version 1` was used to inspect the 30-package
workspace, version, targets, features, normal/build/dev dependencies, and
feature activation. All workspace manifests and the relevant build/proc-macro
surfaces were statically inspected.

### 15.3 Post-audit remediation executed successfully

| Command | Outcome | Evidence |
| --- | --- | --- |
| `cargo check -p icydb-core --tests --no-default-features` | PASS | no-default core library and test targets after the accepted-catalog hard cut |
| `cargo check -p icydb-core --tests --features sql` | PASS | SQL core library and test targets |
| `cargo check -p icydb-core --features sql` | PASS | maintained core SQL surface |
| `cargo check -p icydb-core --features sql-explain,diagnostics` | PASS | explain/diagnostics feature combination |
| `cargo check -p icydb --no-default-features` | PASS | minimal facade after constructor deletion |
| `cargo check -p icydb --features sql` | PASS | facade SQL surface |
| `cargo check -p icydb-testing-model-schema-only` | PASS | schema-only model fixture remains persistence-free |
| `cargo check -p icydb-testing-model-typed-adapter` | PASS | typed-adapter fixture |
| `cargo check -p canister_audit_one_entity_typed_query` | PASS | generated session wiring uses only the store registry |
| `cargo clippy -p icydb-core --no-default-features --lib -- -D warnings` | PASS | minimal core has no warnings |
| `cargo clippy -p icydb-core --features sql --lib -- -D warnings` | PASS | core SQL library has no warnings |
| `cargo clippy -p icydb --features sql --lib -- -D warnings` | PASS | facade SQL library has no warnings |
| `cargo clippy -p icydb-model --lib -- -D warnings` | PASS | generated store-only session wiring has no warnings |
| `cargo test -p icydb-core --no-default-features db::runtime_entity_catalog::tests::accepted_bundle_alone_supplies_runtime_entity_routing --lib -- --exact` | PASS | source-bound accepted entity resolves by tag, path, name, and deterministic enumeration |
| `cargo test -p icydb-core --features sql db::commit::schema_publication::tests::marker_owned_application_publishes_one_live_only_store_and_receipt --lib -- --exact --test-threads=1` | PASS | marker-owned live-only publication |
| `cargo test -p icydb-core --features sql db::commit::schema_publication::tests::interrupted_live_only_application_recovers_candidate_and_receipt_from_marker --lib -- --exact --test-threads=1` | PASS | candidate-only entity recovery and accepted-head restoration |
| `cargo test -p icydb-core --features sql db::relation::reverse_index::tests::accepted_relations_require_accepted_target_authority --lib -- --exact --test-threads=1` | PASS | relations use accepted runtime entities |
| `cargo test -p icydb-model build::actor::db::store::tests::store_registry_wiring_is_lint_clean --lib -- --exact` | PASS | generated compiler emits the sole session constructor |
| `make check-invariants` | PASS | dependency, authority, durability, generated-build, and mutation scripts |
| `cargo fmt --all --check` | PASS | formatting after R1 through R4 |
| exact retired-source and conceptual-responsibility `rg` searches listed below | PASS | no generated runtime registration owner remains in Rust source |
| `cargo test -p icydb-core --features sql db::access::path::tests::semantic_index_key_items_preserve_one_ordered_field_expression_contract --lib -- --exact` | PASS | one ordered semantic contract preserves field/expression identity |
| `cargo test -p icydb-core --features sql db::schema::format::tests::index_origin_reads_accepted_snapshot_instead_of_query_projection --lib -- --exact` | PASS | introspection origin remains accepted-snapshot driven |
| `cargo test -p icydb-core --features sql db::schema::codec::tests::persisted_schema_snapshot_round_trips_expression_indexes --lib -- --exact` | PASS | persisted expression-index contracts are unchanged |
| `cargo test -p icydb-core --features sql db::query::plan --lib -- --test-threads=1` | PASS | 41 focused planner/expression/continuation tests |
| `cargo check -p icydb-core --no-default-features` | PASS | R4 minimal core surface |
| `cargo check -p icydb-core --all-features` | PASS | R4 SQL/diagnostics feature surface |
| `cargo clippy -p icydb-core --all-features --lib -- -D warnings` | PASS | R4 core library has no warnings |
| `cargo test -p icydb-core --lib --all-features db::schema::check::tests::` | PASS | 15 row-program/check/not-null tests |
| `cargo test -p icydb-core --lib --all-features db::relation::reverse_index::tests::` | PASS | 15 accepted relation projection and identity tests |
| `cargo test -p icydb-core --lib --all-features db::schema::mutation::tests::user_index_domain::` | PASS | 11 unique index-domain tests |
| `cargo test -p icydb-core --lib --all-features unique_violation_preserves_accepted_identity_and_fields` | PASS | active unique diagnostic preserves accepted ID/name/kind/entity/key/field paths |
| `cargo test -p icydb-core --lib --all-features accepted_not_null_pre_encoding_failure_preserves_constraint_identity` | PASS | active not-null pre-encoding diagnostic preserves accepted identity |
| `rg -n "index_violation\|relation_target_missing" crates/icydb-core/src --glob '*.rs'` | PASS | no retired generic active-constraint constructor remains |
| `cargo check -p icydb-core --no-default-features` | PASS | R5 scheduler compiles without frontend features |
| `cargo check -p icydb-core --all-features` | PASS | R5 core feature surface |
| `cargo check -p icydb --all-features` | PASS | facade call sites compile against the R5 boundary |
| `cargo test -p icydb-core --lib --all-features --no-run` | PASS | all core library test targets compile after R5 |
| `cargo test -p icydb-core --lib --all-features scheduler_overlay_ -- --test-threads=1` | PASS | 2 complete-batch data-overlay tests |
| `cargo test -p icydb-core --lib --all-features db::index::plan::unique::tests:: -- --test-threads=1` | PASS | 3 unique owner-release/final-collision/corruption tests |
| `cargo test -p icydb-core --lib --all-features db::relation::validate::tests:: -- --test-threads=1` | PASS | same-batch self-relation delete proof |
| `cargo test -p icydb-core --lib --all-features db::commit::prepare::tests::commit_prepare_modes_separate_normal_admission_from_replay_and_rebuild -- --exact --test-threads=1` | PASS | explicit normal/replay/rebuild responsibilities |
| `cargo test -p icydb-core --lib --all-features db::relation::reverse_index::tests:: -- --test-threads=1` | PASS | 15 shared relation-projection tests |
| `cargo test -p icydb-core --lib --all-features db::schema::check::tests:: -- --test-threads=1` | PASS | 15 row-program/check/not-null tests after scheduler routing |
| `cargo test -p icydb-core --lib --all-features db::schema::mutation::tests::user_index_domain:: -- --test-threads=1` | PASS | 11 unique activation/domain tests |
| `cargo test -p icydb-core --features sql db::commit::schema_publication::tests::interrupted_live_only_application_recovers_candidate_and_receipt_from_marker --lib -- --exact --test-threads=1` | PASS | interrupted recovery remains valid under replay prepare mode |
| `cargo clippy -p icydb-core --all-features --lib -- -D warnings` | PASS | R5 core library has no warnings |
| `cargo fmt --all --check` | PASS | formatting after R5 |
| `git diff --check` | PASS | final R5 diff hygiene |
| `cargo check -p icydb-core --no-default-features` | PASS | R6 minimal core surface |
| `cargo check -p icydb-core --all-features` | PASS | R6 complete core feature surface |
| `cargo check -p icydb --no-default-features` | PASS | R6 minimal facade surface |
| `cargo check -p icydb --all-features` | PASS | R6 complete facade feature surface |
| `cargo clippy -p icydb-core --all-features --lib -- -D warnings` | PASS | R6 core library has no warnings |
| `cargo clippy -p icydb --no-default-features --lib -- -D warnings` | PASS | R6 minimal facade has no warnings |
| `cargo clippy -p icydb --all-features --lib -- -D warnings` | PASS | R6 complete facade has no warnings |
| `cargo test -p icydb-core --lib --all-features --no-run` | PASS | all core library test targets compile after R6 |
| `cargo test -p icydb-core --lib --all-features db::session::write::typed_adapter_tests:: -- --test-threads=1` | PASS | 3 accepted typed-contract and immutable ID/slot binding tests |
| `cargo test -p icydb-model-macros typed_adapter --lib -- --test-threads=1` | PASS | generated typed-adapter macro contracts |
| `cargo test -p icydb --test compile --features sql -- --test-threads=1` | PASS | 2 maintained facade compile/trybuild contracts |
| `cargo check -p icydb-testing-model-typed-adapter` | PASS | generated typed-adapter fixture uses the maintained boundary |
| `cargo check -p canister_audit_one_entity_typed_query` | PASS | generated typed query compiles through immutable accepted binding |
| `cargo fmt --all --check` | PASS | formatting after R6 |
| `git diff --check` | PASS | final R6 diff hygiene |
| `cargo check -p icydb-core --no-default-features` | PASS | R7 minimal core surface |
| `cargo check -p icydb --no-default-features` | PASS | R7 minimal facade surface |
| `cargo check -p icydb-core --features query` | PASS | query engine and dynamic reads without SQL |
| `cargo check -p icydb --features query` | PASS | public typed/dynamic query facade without SQL |
| `cargo check -p icydb-core --features query,diagnostics` | PASS | query-only diagnostics combination remains warning-free |
| `cargo check -p icydb-core --features sql` | PASS | SQL composes over query |
| `cargo check -p icydb --features sql` | PASS | facade SQL composes over query |
| `cargo check -p icydb-core --features sql-explain,diagnostics` | PASS | core SQL explain/diagnostics combination |
| `cargo check -p icydb --features sql-explain,diagnostics` | PASS | facade SQL explain/diagnostics combination |
| `cargo check -p icydb-core --features diagnostics` | PASS | diagnostics without query or SQL |
| `cargo check -p icydb --features diagnostics` | PASS | facade diagnostics without query or SQL |
| `cargo clippy -p icydb-core --features query --lib -- -D warnings` | PASS | query-only core has no warnings |
| `cargo clippy -p icydb --features query --lib -- -D warnings` | PASS | query-only facade has no warnings |
| `cargo clippy -p icydb-core --features query,diagnostics --lib -- -D warnings` | PASS | query/diagnostics core has no warnings |
| `cargo clippy -p icydb-core --features sql --lib -- -D warnings` | PASS | SQL core has no warnings |
| `cargo clippy -p icydb --features sql --lib -- -D warnings` | PASS | SQL facade has no warnings |
| `cargo clippy -p icydb --no-default-features --lib -- -D warnings` | PASS | minimal facade has no warnings |
| `cargo test -p icydb-core --features query db::session::write::typed_adapter_tests::typed_binding_uses_accepted_ids_and_slots_across_renames_and_name_reuse --lib -- --exact --test-threads=1` | PASS | manually published accepted schema, typed insert, and dynamic projection execute without SQL |
| `cargo test -p icydb --features query --test compile public_query_facade_compile_contract -- --exact` | PASS | query-only public/trusted dynamic facade compile contract |
| `cargo test -p icydb --test compile --features sql -- --test-threads=1` | PASS | structural, query-only, and SQL facade compile contracts |
| `cargo test -p icydb-core --lib --features sql --no-run` | PASS | complete core SQL library test target compiles |
| `cargo test -p icydb-model-macros typed_adapter --lib -- --test-threads=1` | PASS | generated typed adapter surface remains valid |
| `cargo check -p icydb-testing-model-typed-adapter` | PASS | generated typed-adapter fixture |
| `cargo check -p canister_audit_one_entity_typed_query` | PASS | one-entity generated query canister uses `query` only |
| `cargo check -p canister_audit_ten_entity_typed_query` | PASS | ten-entity generated query canister uses `query` only |
| `scripts/ci/wasm-size-report.sh --canister one_entity_typed_query --canister ten_entity_typed_query` | PASS | raw Wasm 3,537,665 and 3,539,882 bytes; no R6 delta baseline |
| `cargo metadata --no-deps --format-version 1` | PASS | `sql -> query`; `sql-explain -> sql`; no compatibility feature |
| `make check-invariants` | PASS | dependency, panic, generated-build, layer, mutation, durability, admission, schema/model, SQL-branch, and memory guards |
| `cargo fmt --all --check` | PASS | final R7 formatting |
| `git diff --check` | PASS | final R7 diff hygiene |

The new recovery regression initially exposed that its synthetic accepted
candidate lacked source bindings. The test helper was corrected to construct
the same exact source-bound candidate contract as production. The final
commands above pass; production continues to reject unbound accepted state.

Post-remediation retired-source searches:

- `rg -n "EntityRegistration|ENTITY_REGISTRATIONS|new_with_registrations|entity_registration|generated_route_for_entity_path|entity_registrations|runtime_registration_for_entity|prepare_commit_context_for_runtime_registration|for_runtime_registration" crates --glob '*.rs'`
- `rg -n "generated (entity )?(route|routing|registration)|registered (entity|source|target)|model-free registration|registration slice" crates --glob '*.rs'`

Both returned no source matches.

R3 consolidation searches:

- `rg -n "SemanticIndexKeyItems|generated provenance is query-owned" crates/icydb-core/src --glob '*.rs'`
- `rg -n "Query<E>|PreparedExecutionPlan<E>|generated fallback|direct lowering tests keep" crates/icydb-core/src/db --glob '*.rs'`

The first returned no matches. Remaining generated-model comments found by
broader searches describe explicit proposal, reconciliation, or trust-boundary
exclusions rather than runtime fallback authority.

R5 retired-path search:

- `rg -n "save_validation|validate_save_relations_for_structural_row|prepare_reverse_relation_index_mutations_for_source_slot_readers|validate_structural_accepted_after_image|ReverseRelationMutationTarget|ReverseRelationSourceTransition" crates/icydb-core/src --glob '*.rs'`

This returned no matches. Construction/call-site search for
`AcceptedMutationConstraintScheduler`, `AcceptedStorageConstraintSchedule`,
`CommitPrepareMode`, and both normal commit-window entrypoints confirmed the
scheduler is the only normal save/delete batch constructor and that recovery
and rebuild select explicit prepare modes.

R6 retired-path search:

- `rg -n "structural_patch_from_binding|fields: Vec<\(String, String\)>|accepted_schema_catalog_context_for_entity_name\(Some\(binding|\.field_name\(source\.as_ref\(\)\)" crates/icydb-core/src/db/dynamic_write.rs crates/icydb-core/src/db/session/write.rs crates/icydb/src/db/session/write.rs crates/icydb/src/db/query/typed.rs`

This returned no matches. Construction/call-site searches for
`DynamicTypedStructuralPatch`, `DynamicTypedMutation`,
`find_accepted_schema_catalog_context_for_entity_source_key`, and
`execute_public_dynamic_query_for_typed_binding` confirmed that typed writes
lower binding-owned field IDs/slots and typed queries select the accepted
catalog by immutable entity identity. The structural and SQL name-driven
frontends remain deliberately separate boundary adapters.

R7 retired-boundary searches:

- `rg -n 'SqlProjectionPayload|SqlProjectionContract|execute_public_projection_from_structural_query' crates --glob '*.rs'`
- `rg -n 'SqlStatementResult|session::sql|db::sql' crates/icydb-core/src/db/session/query crates/icydb/src/db/query --glob '*.rs'`
- manifest searches for `features = ["sql"]` and `features = ["query"]` in
  both maintained typed-query canisters.

The retired type/bridge and query-to-SQL import searches returned no matches.
Both normal and build dependencies of the typed-query canisters select
`query`, not `sql`.

The first cumulative invariant run exposed two stale guard literals left by
R5/R6: the layer guard required deleted `save_validation.rs`, and the
read-admission guard required the former name-routed typed handoff. The guards
now inspect `constraint_scheduler.rs` and the immutable-binding typed query
handoff. Their focused scripts and the remaining invariant scripts pass.

### 15.4 Not executed

- Full workspace/repository tests: **not executed**; repository governance makes
  them user-owned release/push validation.
- Full workspace clippy/all-targets/all-features: **not executed**; focused core
  and facade SQL library clippy passed.
- PocketIC/integration and runtime instruction-performance suites:
  **not executed**. Focused raw Wasm size reports were executed for the two
  typed-query canisters, but no comparable R6 artifact exists and no delta is
  claimed.
- No-default full workspace feature matrix: **not executed** because it is a
  full workspace command; affected package and fixture combinations were run
  individually.

All final validation commands passed. The full suites and measurements above
remain explicitly unexecuted.

## 16. Remaining architectural risks

No known reachable duplicate authority or frontend bypass remains from this
audit.

Two exact evidence/design boundaries remain:

1. R2 exact entity lookup is bounded by registered-store count and reuses each
   schema store's verified accepted-bundle cache, but no instruction benchmark
   was run for that lookup.
2. Nested/member durable constraints are not part of the current accepted
   field-constraint placement contract. Supporting them requires a separate
   accepted path/identity design; extending the scalar field program implicitly
   would reintroduce drift.

No evidence supports a rewrite of the planner, executor, persistence layer, or
schema proposal system. The reachable topology/public-surface, planner-key,
constraint-diagnostic, and mutation-scheduling contradictions are corrected;
the query/type feature boundary is also consolidated.
