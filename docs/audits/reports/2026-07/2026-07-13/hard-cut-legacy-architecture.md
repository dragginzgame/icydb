# IcyDB Hard-Cut Legacy Architecture Audit

Date: 2026-07-13

> Scope: repository-wide architectural-authority and legacy-sediment audit
>
> Compared baseline report: N/A. No earlier report uses this scope and method;
> `docs/audits/reports/2026-07/2026-07-04/technical-debt.md` was consulted as
> historical context only.
>
> Code snapshot: `e1928d349e9294cfc9cee3d6a78d2eb831da0bec`, plus the recorded dirty
> worktree described below.
>
> Method: Hard-Cut Architecture Audit V1
>
> Comparability: non-comparable — this is the first deletion-proof and
> single-authority audit using the A–F hard-cut classification.

## Executive assessment

Architectural sediment is **high**, but not critical.

The production read spine is substantially more coherent than the repository's
surface area suggests. Typed/fluent and reduced SQL reads both lower to
`StructuralQuery`, use the shared query-plan cache, produce an
`AccessPlannedQuery`/`SharedPreparedExecutionPlan`, and execute through the same
route planner and executor contracts. Accepted schema snapshots are the runtime
authority. Cursor decoding is one bounded, version-gated implementation. The
executor has one effective residual-filter contract and one route-selection
artifact.

The surviving old design families are:

- a public-but-hidden direct `Query<E>` surface beside the fluent facade;
- two inert `PlannedQuery<E>` / `CompiledQuery<E>` stages over the same logical
  plan;
- a test-only public-SQL admission architecture even though production SQL is
  trusted/controller-only;
- public admission DTOs that expose an internal policy vocabulary;
- an obsolete projection-response facade and explicit SQL compatibility aliases;
- diagnostics that stringify typed planner facts and reconstruct them later;
- grouped-route observability that describes the sole supported route as a
  fallback and retains unreachable rejection state;
- metrics for a removed generated-model runtime fallback;
- compatibility spellings and compile-fail tests whose only contract is keeping
  deleted names deleted;
- active documentation and `v1` terminology that describe superseded route,
  cursor, and policy structures.

Most of this sediment is structural or diagnostic. Two areas still affect the
meaning of live runtime surfaces:

1. `DbSession::execute_sql_query` is trusted/admin only by documentation and
   generated-call-site convention; the method itself carries no explicit trusted
   marker, capability, or lane.
2. access-choice and grouped-route diagnostics can present reconstructed or
   fictitious route facts instead of the exact typed facts used by execution.

The highest-risk authority conflict is therefore the SQL trust boundary,
followed by diagnostic route identity. No evidence showed a currently reachable
public-read admission bypass in generated code: generated `icydb_query` calls
`icydb_sql_surface_require_controller("query")` before entering the trusted SQL
helper. The risk is that a library caller can attach a new endpoint directly to
the ambiently trusted helper, and that future work can mistakenly treat the
test-only public-SQL lane as a supported production architecture.

IcyDB is safe to continue extending accepted-schema, storage, and already
canonical typed/fluent internals while these findings are open. It is **not safe
to extend public SQL, admission-lane configuration, direct-query APIs, projection
facades, or route diagnostics before the corresponding authority cleanup**.

No production behavior was changed by this audit.

## Scope and method

### Authority sources

The audit treated sources in this order:

1. Active designs `0.200`, `0.201`, and `0.202`, especially their accepted-schema
   authority and pre-1.0 hard-cut rules.
2. The `0.198` closeout and `0.199` boundary/authority artifacts now under
   `docs/design/archive/`.
3. Root and detailed changelogs through `0.201`, plus the current `Unreleased`
   worktree.
4. Normative contracts under `docs/contracts/` and the public facade guide.
5. Current facade, generator, planner, executor, admission, cursor, persistence,
   metrics, and error implementations.
6. Unit, integration, generated-code, compile-fail, and snapshot tests.
7. Targeted Git history for conflicting or compatibility-shaped surfaces.

The latest query/read-intent design families are archived because their release
lines closed; their closeout and boundary matrices remain the most recent
architecture evidence for those subsystems. Active `0.200`–`0.202` documents do
not replace that query design. They reinforce its two relevant invariants:
accepted snapshots are runtime authority and pre-1.0 removals are hard cuts.

### Worktree handling

The audit began and ended at commit
`e1928d349e9294cfc9cee3d6a78d2eb831da0bec`. The worktree already contained a
large user-owned `0.202` explicit-Rust-default slice affecting the changelog,
schema derive, schema/runtime validation, SQL insert/DDL validation, fixtures,
and UI tests. That slice continued changing during the audit: additional
user-owned edits appeared in persisted-row module wiring, structural save,
runtime schema tests, SQL write authority, and session write code. Those changes
were re-read where they intersected current authority or validation, but were
not attributed to this audit and were not modified or reverted.

### Evidence standard

Each deletion finding required at least two of the following:

- constructor/producer and consumer searches;
- public-signature and re-export searches;
- generated-code inspection;
- production-vs-`cfg(test)` reachability;
- current contract/example inspection;
- test purpose classification;
- persisted-format ownership inspection;
- targeted history showing the superseded role.

Mechanical searches covered source, tests, macros, generators, fixtures,
canisters, examples, scripts, Cargo features, docs, comments, and changelogs.
Neutral-named duplicates were then traced semantically rather than classified by
keyword alone.

## Current architecture map

### Surviving read and query spine

The current architecture is one shared semantic spine with two frontends:

```text
typed/fluent builder ----> Query<E> / StructuralQuery ----+
                                                         |
SQL parse + lowering ---> CompiledSqlCommand ------------+--> StructuralQuery
                                                               |
                                                               v
                                      accepted schema + visible indexes
                                                               |
                                                               v
                              shared query-plan cache / AccessPlannedQuery
                                                               |
                                                               v
                                  SharedPreparedExecutionPlan
                                                               |
                                                               v
                             ExecutionRoutePlan + executor contracts
                                                               |
                                                               v
                              terminal/result-specific response shaping
```

The SQL compiled-command cache is not a second semantic planner. It caches SQL
frontend work and may retain the same shared prepared plan. Its identity is
guarded by accepted schema revision/fingerprint and the structural query's
cache identity.

### Single-owner map

| Concern | Intended owner | Primary types/modules | Competing or misleading owners found |
| --- | --- | --- | --- |
| Public typed query construction | Facade fluent builders and semantic terminals | `crates/icydb/src/db/session/load.rs`; `load/paging.rs`; `load/partial_window.rs`; core `db/query/fluent/` | Hidden facade `Query<E>`; duplicate `.planned()` / `.plan()` stages (HCLA-004, HCLA-005) |
| SQL entry and statement-family routing | Session SQL frontend | `db/session/sql/{surface,compile,compiled,execute}`; `CompiledSqlCommand` | Test-only public-read SQL dispatch (HCLA-002) |
| Query intent | Query intent layer | `StructuralQuery`, `QueryIntent`, `NormalizedFilter` in `db/query/intent/` | No second live intent owner found |
| SQL/fluent semantic convergence | Structural query planning boundary | SQL semantic compiler emits `StructuralQuery`; `db/session/query/cache.rs` | Test-only SQL admission can make the repository appear to support another public frontend policy (HCLA-002) |
| Runtime schema and value authority | Accepted schema/catalog context | `AcceptedSchemaCatalogContext`, `SchemaInfo`, `EntityAuthority`; `db/schema/` | Obsolete generated-fallback metric vocabulary (HCLA-009) |
| Logical/access planning | Query planner | `AccessPlannedQuery`, `AccessPlan`, `ResolvedOrder`, `ResidualFilterContract`; `db/query/plan/` | `PlannedQuery` and `CompiledQuery` duplicate shells (HCLA-004) |
| Shared prepared-plan identity | Session query-plan cache | `QueryPlanCacheKey`, `StructuralQueryCacheKey`, `SharedPreparedExecutionPlan`; `db/session/query/cache/` | No competing production cache owner found |
| Public-read admission | Internal query admission policy invoked by fluent terminals | `QueryAdmissionPolicy::default_bounded_read`, `QueryAdmissionSummary`; `db/query/admission*`; fluent terminal support | Test-only SQL policy lane and public admission DTO exports (HCLA-002, HCLA-003) |
| Trusted/admin read bypass | Explicit surface boundary | Fluent `trusted_read_unchecked()`; generated SQL controller gate | Plain `execute_sql_query` relies on caller convention; phantom `AdminAdHoc` lane is not selected in production (HCLA-001) |
| Route selection | Executor route planner | `ExecutionRoutePlan`; `db/executor/planning/route/` | Grouped observability invents fallback/rejection state after selection (HCLA-008) |
| Filter semantics | Expression truth plus derived predicate subset | `Expr`/`FilterExpr`; `NormalizedFilter`; planner access consumption | No duplicate truth owner found; predicate is an optimization/contract subset, not an alternate truth model |
| Runtime residual filter | Executor preparation contract | `ResidualFilterContract`, `EffectiveRuntimeFilterProgram` | No second runtime evaluator contract found |
| Ordering | Logical builder/planner | `OrderSpec`, canonical logical order, `ResolvedOrder`; planner adds deterministic PK tie-break where required | No competing admission-owned order definition found |
| Cursor wire and validation | Cursor boundary | `db/cursor/token/codec.rs`, `ContinuationToken`, validation spine and continuation modules | Active historical checkpoint documents CBOR and no pushdown (HCLA-012) |
| Pagination API | Facade cursor-page terminals | `page`, `next_page`, `PagedResponse`, `PagedGroupedResponse` | No old page-number/total-pages response facade found |
| Projection selection | Planner and prepared execution plan | `ProjectionSpec`, covering-read plan, SQL projection contract | Dead `ProjectionResponse`, public intermediate `ProjectionRows`, compatibility aliases (HCLA-006) |
| Row/result materialization | Executor terminal and facade adapter | executor projection/materialization; `Response`, `PagedResponse`, `RowProjectionOutput`, SQL result enum | Projection response shells and aliases (HCLA-006) |
| Response cardinality | Response/cardinality extension | core `Response<R>`, `ResponseCardinalityExt`; facade `Response<E>` | No duplicate executor cardinality authority proven |
| Execution diagnostics | Frozen logical/route artifacts projected by explain | `FinalizedQueryDiagnostics`, execution descriptor, route plan | access rejection string reparse and fictitious grouped outcomes (HCLA-007, HCLA-008) |
| Persistence | One current codec per durable surface | database boot gate; row, commit, journal, schema, index and cursor codecs | No compatibility decoder or dual-read branch found |
| Recovery | Database format admission, commit recovery, schema/index recovery | `db/database_format`, `db/commit/recovery`, schema/index recovery owners | `LEGACY_STABLE_CELL_MAGIC` is a fail-closed overwrite guard, not a legacy decoder |
| Errors | Domain producer, converted at public boundary | `QueryError`, `PlanError`, `InternalError`, diagnostic codes | diagnostic reason strings lose typed ownership (HCLA-007); no broad string-matched runtime error path found |

### Frontend traces

#### Typed/fluent read

1. `icydb::db::DbSession::load::<E>()` constructs a facade
   `FluentLoadQuery<'_, E>` over the core fluent builder.
2. Builder operations mutate the typed `Query<E>` / `StructuralQuery` intent;
   semantic terminals such as `page`, `try_one`, `collect_complete`, and exact
   aggregates declare result intent.
3. Ordinary execution applies `QueryAdmissionPolicy::default_bounded_read()`;
   trusted maintenance code must opt into `trusted_read_unchecked()`.
4. `DbSession::cached_shared_query_plan_for_entity` resolves accepted schema and
   visible indexes, builds or reuses the `AccessPlannedQuery`, and returns a
   `SharedPreparedExecutionPlan`.
5. The executor route planner freezes one route result. Terminal code applies the
   executor-owned residual program, order/window/cursor contract, and result
   shaping.

#### SQL read

1. `execute_sql_query::<E>` parses and surface-validates reduced SQL, loads the
   accepted catalog context, and builds a `SqlCompiledCommandExecutionContext`.
2. Scalar/grouped `SELECT` and global aggregates carry or produce a
   `StructuralQuery`; SQL projection labels remain a frontend/result concern.
3. `select_plan` / `aggregate_plan` use the same shared prepared-plan machinery
   and `EntityAuthority` used by fluent execution.
4. Generated `icydb_query` is controller-gated before it calls the trusted SQL
   helper. The library method itself does not encode that authority (HCLA-001).

SQL and fluent therefore converge at `StructuralQuery` and the shared prepared
plan, not at parsing and not only inside the executor.

### Execution route inventory

| Operation | Entry condition | Route authority | Distinct semantics | Result assembly | Audit disposition |
| --- | --- | --- | --- | --- | --- |
| Scalar entity/projection load | Non-grouped read intent | `ExecutionRoutePlan` from shared prepared plan | Streaming vs materialized; primary/secondary/range/composite access; covering projection where proven | Scalar terminal and facade response adapters | Retain; routes represent real capability/performance distinctions |
| Global scalar aggregate | Aggregate terminal or SQL global aggregate | Aggregate route plan plus prepared aggregate contract | Dedicated exact/count/seek/pushdown routes where semantically eligible | Scalar aggregate output | Retain; not a v1/v2 split |
| Grouped aggregate | Grouped plan | Planner `GroupedPlanStrategy`, executor `GroupedExecutionMode` | `HashMaterialized` vs `OrderedMaterialized`; no grouped scalar fast path | Grouped rows and optional cursor | Retain algorithms; delete fictitious fallback/rejection diagnostics (HCLA-008) |
| Cursor continuation | `next_page` or grouped cursor | Cursor validation plus route plan | Same query signature/order/window; strict continuation boundary | `PagedResponse` / grouped page | Retain one current contract |
| Delete | Fluent delete or SQL mutation command | Query mode, delete plan, write policy | Materialized affected-row selection; returning shape optional | count or `RowProjectionOutput` | Retain; no duplicate v1/v2 executor found |
| SQL insert/update/delete | Trusted library or explicitly generated write surface | SQL write policy, command-specific planning, accepted-schema write authority | Statement-family-specific candidate and mutation rules | count or returning projection | Retain; routes differ semantically |
| DDL | `execute_sql_ddl` / generated controller DDL | Catalog-native schema mutation | SQL is syntax only; accepted schema publication owns mutation | DDL status/result | Retain; no SQL-as-authority path found |
| Test-only public SQL read | `execute_sql_query_with_read_admission_policy` under `cfg(test)` | Shadow SQL-specific admission/budget handoff | Simulates a public SQL product that production does not expose | SQL projection/grouped response | Delete (HCLA-002) |

## Findings table

| ID | Severity | Category | Area | Finding | Evidence | Risk | Recommended action |
| --- | --- | --- | --- | --- | --- | --- | --- |
| HCLA-001 | High | B — Collapse into current authority | Admission / public SQL API | Trusted/admin SQL is ambient caller convention, not an explicit lane at the library boundary | Public `execute_sql_query` in facade/core; generated controller gate exists separately; production has no `AdminAdHoc` policy producer | A new endpoint can call a broad helper without a visible authority transition; docs and runtime lane taxonomy disagree | Hard-cut rename the broad read helper to an explicitly trusted/admin name, update generated calls, and remove unused lane fiction; add no alias |
| HCLA-002 | Medium | A — Delete now | SQL / tests / admission | A complete test-only public-SQL admission execution path survives after public SQL was removed from the product | `execute_sql_query_with_read_admission_policy`; `execute/select/read_budget.rs`; 3,582-line admission test file; architecture note calls it legacy matrix coverage | Future changes may preserve or extend a nonexistent public SQL architecture and duplicate fluent admission semantics | Delete the test-only dispatch, response-budget module, and public-SQL policy tests; keep lowering parity and fluent admission tests |
| HCLA-003 | Medium | C — Privatise | Public API / admission | Internal admission DTOs are intentionally re-exported through the application facade without a developer use case | `crates/icydb/src/db/mod.rs` re-exports eleven `QueryAdmission*` / `QueryBoundKind` types; no facade signature or guide uses them | Encourages applications and diagnostics to bind to internal planner-policy facts and makes lane cleanup appear public-breaking | Remove facade re-exports and narrow core visibility to the diagnostic/admission owners that consume them |
| HCLA-004 | Medium | A — Delete now | Query intent / public API | `PlannedQuery<E>` and `CompiledQuery<E>` are two public stages over the same `AccessPlannedQuery` | Both wrap `QueryPlanHandle`; both expose only `explain` and hash in production; session adapters clone the same cached logical plan | False lifecycle stages invite features to attach to a nonexistent compile boundary and duplicate every builder terminal | Delete both types, `.planned()` / `.plan()`, adapters, re-exports, and stage-only tests/docs; keep direct `.explain()`, trace and hash |
| HCLA-005 | Medium | A — Delete now | Public API / diagnostics | The hidden direct `Query<E>` facade and two session wrappers have no remaining generated or repository caller | `db/query/mod.rs` hidden re-export; `execute_query_result_with_attribution`; `trace_query`; reference search finds only definitions | Public-but-hidden surface bypasses intended fluent ergonomics and obscures which boundary owns admission | Remove only facade exposure/wrappers; retain core `Query<E>` as the fluent implementation type |
| HCLA-006 | Medium | A + C — Delete/privatise | Responses / projection / compatibility | An obsolete `ProjectionResponse` facade and explicit SQL compatibility aliases coexist with the current row-projection payload | No producer of facade/core `ProjectionResponse`; `SqlProjectionRows` and `SqlQueryRowsOutput` are documented as compatibility aliases; `RowProjectionOutput` is current Candid payload | Multiple names and conversion shells obscure row-shape ownership and perpetuate a retired projection API | Delete `ProjectionResponse`; hard-cut aliases to `RowProjectionOutput`; privatise/collapse `ProjectionRows` if only assembly/rendering needs it |
| HCLA-007 | Medium | B — Collapse into current authority | Planner / diagnostics | Typed access-choice rejection and candidate identity are rendered to strings, then reparsed/matched to build public diagnostics | `AccessChoiceRejectedReason::render_for_index`; `AccessChoiceExplainSnapshot.rejected: Vec<String>`; `parse_rejected_index_label`; label match with one-candidate fallback | EXPLAIN can drift from planner truth after harmless rendering changes or attach residual facts to the wrong candidate | Carry typed index/reason/candidate identity in the planner snapshot; render strings only at output boundaries |
| HCLA-008 | Medium | B + D — Collapse/re-document | Route selection / diagnostics | The sole grouped route is always reported as `materialized_fallback`; rejection/capability-mismatch state is unreachable | grouped fast-path order is empty; `eligible = fast_path_order.is_empty()`; false branch creates rejection; snapshots assert fallback with no fallback reason | Operators and contributors see a fallback where no preferred route was attempted and may optimize the wrong layer | Project actual `GroupedExecutionMode` and planner fallback reason; remove route outcome/rejection/eligibility fiction |
| HCLA-009 | Medium | A + D — Delete/re-document | Schema authority / metrics | Metrics still expose `GeneratedFallback` after generated-model runtime fallback was hard-cut | Only production producer records `AlreadyFinalized`; tests manufacture `GeneratedFallback`; `0.200` forbids runtime reconstruction | Observability advertises an illegal authority path and makes its return appear supported | Delete `GeneratedFallback` and its counters; collapse the one-variant outcome abstraction into a specifically named reuse/finalization metric if still useful |
| HCLA-010 | Low | A — Delete now | CLI compatibility | `primary_key` remains as a Clap alias for canonical `primary-key` | `ConfigInitUpdatePolicy` value attribute and alias-specific CLI test | Preserves an obsolete public spelling contrary to pre-1.0 hard cuts | Delete alias and alias-only test; retain underscore in generated config value where that is the config grammar |
| HCLA-011 | Low | A — Delete now | Compile-fail tests / governance | Two UI tests exist only to keep already removed API/module names rejected | `db_default_removed.rs`; `contracts_module_absent.rs`; stale guard index; history shows the module was deleted | Tests turn removed architecture into a maintained negative contract and directly violate current hard-cut rules | Delete both fixture pairs and the stale guard row; rely on positive current-surface tests and privacy tests for live modules |
| HCLA-012 | Medium | D — Rename/re-document | Documentation / terminology | Active contracts and comments still describe CBOR/no-pushdown cursors, legacy SQL test lanes, phantom lanes, moved evidence paths, and unqualified grouped “v1” | `CURSOR.md`; `SQL_SURFACE_MAPPING.md`; `READ_ADMISSION.md`; `DURABILITY.md`; grouped validation comments/errors | New work can attach to an archived route or infer a v2/legacy contract that does not exist | Archive or rewrite stale checkpoint text, repair links, delete legacy-lane claims, and name current invariants without v1 terminology |

## Detailed findings

### HCLA-001 — Trusted/admin SQL is ambient rather than explicit

**Files and symbols**

- `crates/icydb/src/db/session/sql.rs` — public
  `DbSession::execute_sql_query<E>` and
  `execute_sql_query_with_perf_attribution<E>`.
- `crates/icydb-core/src/db/session/sql/mod.rs` — core
  `DbSession::execute_sql_query<E>`.
- `crates/icydb-build/src/db/sql.rs` — generated
  `icydb_sql_surface_require_controller` and generated `icydb_query` dispatch.
- `crates/icydb-core/src/db/query/admission.rs` —
  `QueryAdmissionLane::{AdminAdHoc, DevTest}`.
- `crates/icydb-core/src/db/query/admission/policy.rs` — test-only
  `QueryAdmissionPolicy::admin_ad_hoc`.
- `docs/contracts/READ_ADMISSION.md` — surface inventory and lane definitions.

**Current behavior.** A library caller invokes a broadly capable method named
`execute_sql_query`. Its docs say trusted/admin and warn against caller-controlled
SQL, but the type/signature does not require a trusted marker. Generated
`icydb_query` is safe because generated code separately calls
`icydb_sql_surface_require_controller("query")`. Fluent maintenance reads use a
visibly explicit `trusted_read_unchecked()` transition.

The admission vocabulary claims that `execute_sql_query` selects
`AdminAdHoc`, but production SQL never constructs an admin policy or records
that lane. `QueryAdmissionPolicy::admin_ad_hoc` is `cfg(test)`; `DevTest` has no
constructor/producer at all. The actual architecture is therefore:

- production fluent public reads: evaluated by built-in `PublicRead` policy;
- diagnostic explain: evaluated as `DiagnosticExplain` without rows;
- trusted fluent: explicit method marker, policy bypass;
- trusted SQL: ambient method/caller contract, policy bypass;
- generated SQL: controller gate outside the SQL session method.

**Historical design represented.** The lane enum and documentation describe a
multi-lane execution model in which admin and dev/test work are selected and
observable admission lanes. The runtime has instead converged on a public policy
gate plus explicit bypass surfaces.

**Obsolescence proof.** Production reference search finds no
`QueryAdmissionLane::AdminAdHoc` producer and no `DevTest` producer. Generated
tests explicitly assert that generated SQL contains the controller gate and does
not call the hidden public-read-policy method. The public-read contract says
custom application policy is not a current product surface.

**Reachability and compatibility.** The broad SQL helper is reachable public
API and executes rows. The phantom lane variants are reachable only as public
values or tests, not as production decisions. There is no external compatibility
obligation. No persisted format is involved.

**Tests preserving it.** Generator tests correctly preserve the controller
gate and should remain. Admission unit tests manufacture `AdminAdHoc`; those
should go with the unused lane. Public SQL policy tests are addressed separately
by HCLA-002.

**Recommended end state.** Rename the facade helper to an explicitly trusted
name such as `execute_trusted_sql_query` and update the core/generator handoff in
the same hard-cut slice. Do not keep `execute_sql_query` as an alias. Keep the
generated controller gate. Remove `AdminAdHoc` and `DevTest` from admission
policy taxonomy unless production execution begins to carry those exact typed
facts; do not introduce a general capability framework merely to preserve the
enum.

**Semantic impact.** Intentional source break only. Generated authorization,
SQL semantics, planning, execution, response shape, and performance must remain
unchanged.

**Focused validation.** Facade compile tests, generated SQL surface tests,
controller-gate source assertions, SQL unit/integration tests, and a repository
search proving the old method and phantom lane names are absent.

### HCLA-002 — Test-only public-SQL admission architecture

**Files and symbols**

- `crates/icydb-core/src/db/session/sql/mod.rs` —
  `execute_sql_query_with_read_admission_policy` (`cfg(test)`).
- `crates/icydb-core/src/db/session/sql/execute/mod.rs` —
  `execute_compiled_sql_context_with_read_admission_policy` (`cfg(test)`).
- `crates/icydb-core/src/db/session/sql/execute/select.rs` — test-only dispatch
  and response-budget hooks.
- `crates/icydb-core/src/db/session/sql/execute/select/read_budget.rs` — entire
  247-line test-only module.
- `crates/icydb-core/src/db/session/sql/execute/global_aggregate.rs` —
  `execute_global_aggregate_compiled_statement_ref_with_read_admission_policy`.
- `crates/icydb-core/src/db/session/tests/read_admission.rs` — 3,582 lines,
  including the `public_read_sql_*` matrix.
- `docs/architecture/SQL_SURFACE_MAPPING.md` — explicit statement that the
  helpers keep “legacy matrix coverage” stable.

**Current behavior.** Tests compile SQL, attach a `PublicRead` policy, mutate
group execution caps, execute rows, estimate Candid response bytes, and assert
public-policy outcomes. No production facade or generated endpoint can enter
this path. Production SQL is trusted/admin; ordinary public reads are fluent.

**Historical design represented.** A product architecture in which arbitrary
SQL could be admitted as a normal public read lane, with SQL-specific response
budget enforcement after execution.

**Obsolescence proof.** Every entry and helper is `cfg(test)`. Generator tests
assert generated surfaces do not call it. `READ_ADMISSION.md` forbids public
caller-controlled SQL, and `SQL_SURFACE_MAPPING.md` itself describes the helper
as legacy coverage rather than live API.

**Reachability and compatibility.** Unreachable in production; reachable only
inside core tests. No public or persisted compatibility obligation exists.

**Tests preserving it.** Functions named `public_read_sql_*` cover missing
limits, full scans, PK equality/`IN`, nonzero offsets, materialized order,
projection bytes, grouped budgets, and distinct budgets. Many semantic facts are
valid, but the frontend/policy combination is not a product contract. Equivalent
current facts already belong in planner/admission unit tests, SQL lowering
parity tests, and fluent `PublicRead` tests.

**Recommended end state.** Delete the test-only SQL policy entry/dispatch and
`select/read_budget.rs`. Delete tests whose asserted contract is “public SQL is
admitted/rejected.” Move only genuinely missing frontend-neutral planner proofs
to planner/admission tests, and retain SQL tests for lowering/type/error parity
without running them through a fictitious public lane.

**Semantic impact.** None in production. Test architecture and compile time get
simpler. This deletion must not weaken fluent `PublicRead` admission or generated
SQL controller gating.

**Focused validation.** Core admission tests, SQL lowering/execution tests,
generated SQL tests, fluent public-read tests, and a source search for the
deleted method/module.

### HCLA-003 — Internal admission DTOs leak through the facade

**Files and symbols**

- `crates/icydb/src/db/mod.rs` publicly re-exports:
  `QueryAdmissionAccessKind`, `QueryAdmissionDecision`,
  `QueryAdmissionGroupedSummary`, `QueryAdmissionLane`,
  `QueryAdmissionOrdering`, `QueryAdmissionPlanShape`,
  `QueryAdmissionRejection`, `QueryAdmissionResidualFilter`,
  `QueryAdmissionSummary`, `QueryBoundKind`, and
  `QueryMaterializationSummary`.
- `crates/icydb-core/src/db/mod.rs` re-exports the same internal policy/summary
  vocabulary.
- `crates/icydb-core/src/db/query/admission.rs` owns the types.
- `crates/icydb-core/src/db/query/explain/execution.rs` stores the summary only
  inside crate-private `FinalizedQueryDiagnostics`.

**Current behavior.** Applications can name and construct parts of the
admission model, but no public facade method accepts or returns these DTOs.
Production consumers are the admission policy, explain rendering, and internal
diagnostics. The public guide does not present them as a use case.

**Historical design represented.** A configurable/application-facing policy or
structured admission API. Current contracts explicitly exclude configurable
public read policies from the facade.

**Obsolescence proof.** Repository-wide facade/reference search finds the types
only at the re-export; core uses remain inside admission and diagnostics. The
public `QueryError` path exposes diagnostic codes rather than requiring callers
to construct a `QueryAdmissionSummary`.

**Reachability and compatibility.** Publicly nameable, but not required by a
current public signature. No persisted format. No external user compatibility
obligation.

**Tests preserving it.** Core admission tests legitimately test the internal
types. No external compile test demonstrates an application use case.

**Recommended end state.** Remove the facade re-exports. Narrow core visibility
to `pub(in crate::db)` or the smallest practical owner boundary after facade
removal. Keep stable public error/diagnostic codes where callers actually receive
them; do not expose the policy construction model.

**Semantic impact.** Source-surface reduction only; no admission decision or
error code changes.

**Focused validation.** Facade public API/compile tests, docs, diagnostics
rendering tests, and core admission unit tests.

### HCLA-004 — `PlannedQuery` and `CompiledQuery` are duplicate stages

**Files and symbols**

- `crates/icydb-core/src/db/query/intent/query.rs` — `QueryPlanHandle`,
  `PlannedQuery<E>`, `CompiledQuery<E>`.
- `crates/icydb-core/src/db/session/query/planning.rs` —
  `planned_query_with_visible_indexes` and
  `compile_query_with_visible_indexes`.
- `crates/icydb-core/src/db/query/fluent/load/builder.rs`,
  `load/partial_window.rs`, and `fluent/delete.rs` — `.planned()` / `.plan()`.
- `crates/icydb/src/db/session/load.rs`, `load/partial_window.rs`, and
  `delete.rs` — duplicate facade terminals.
- `crates/icydb-core/src/db/mod.rs` and `crates/icydb/src/db/query/mod.rs` —
  re-exports.
- `docs/guides/public-facade-api.md` — teaches both as diagnostics.

**Current behavior.** Both public types own the same private `QueryPlanHandle`
containing `Box<AccessPlannedQuery>`. Both production impls expose `explain()`
and `plan_hash_hex()`. The “compiled” type has additional plan extraction only
under `cfg(test)`. Both session methods call the same
`cached_shared_query_plan_for_entity` and clone
`prepared_plan.logical_plan()`; neither carries executor compilation state.
Production execution consumes `SharedPreparedExecutionPlan`, not either shell.

**Historical design represented.** Separate logical-planning and executable
compilation stages once surfaced to users. The shared prepared-plan cache made
that distinction inert; the `0.93` changelog records that typed execution moved
directly to the cached prepared plan while these shells were retained.

**Obsolescence proof.** Constructor/consumer searches find no production
executor handoff from either public shell. Their methods and payloads are
structurally identical at runtime. Direct query `.explain()`, `.trace()`, and
`.plan_hash_hex()` already expose every live diagnostic use case.

**Reachability and compatibility.** Fully reachable public API and documented,
but no external compatibility obligation. No persistence.

**Tests preserving it.** `db/query/intent/tests/explain.rs` asserts both shells;
other intent tests call `.plan()` to extract an internal plan under `cfg(test)`.
Those tests can use planner test helpers or the authoritative cached plan rather
than preserving public stages.

**Recommended end state.** Delete both shell types, all `.planned()` / `.plan()`
facade and core terminals, the two session adapters, re-exports, stage-only
tests, and guide entries. Keep one internal `AccessPlannedQuery` and one
executor-ready `SharedPreparedExecutionPlan`; keep public explain, trace, and
hash terminals.

**Semantic impact.** Intentional source break; no execution or diagnostic
content change.

**Focused validation.** Core query/explain tests, facade compilation, generated
code, public guide doctests, and a source search proving both type names and
terminals are absent.

### HCLA-005 — Hidden facade `Query<E>` has no remaining caller

**Files and symbols**

- `crates/icydb/src/db/query/mod.rs` — `#[doc(hidden)] pub use
  icydb_core::db::Query`.
- `crates/icydb/src/db/session/mod.rs` — hidden
  `execute_query_result_with_attribution` and `trace_query`.
- `docs/design/archive/0.199-technical-debt-audit/0.199-boundary-matrix.md` —
  retained it only for possible generated/internal diagnostics after direct row
  execution was removed.

**Current behavior.** Normal callers use `DbSession::load` and fluent terminals.
Fluent queries expose their own trace and attribution helpers. A search across
the facade, generator, canisters, fixtures, and tests finds no caller of the two
session wrappers and no generated construction of facade `Query<E>`.

**Historical design represented.** A direct query-builder/product surface and
an earlier perf/diagnostic path that required raw query extraction. The `0.199`
boundary work removed direct facade row execution and moved perf attribution to
fluent terminals, but deferred this last exposure.

**Obsolescence proof.** Definition-only reachability inside the repository,
combined with current generated-code inspection and fluent replacement methods.
The core `Query<E>` remains heavily used as the implementation type; only facade
exposure is obsolete.

**Reachability and compatibility.** Public-but-hidden and therefore technically
reachable downstream. No current documented developer use case and no external
compatibility obligation. No persistence.

**Tests preserving it.** No facade/generated caller test was found. Core query
tests are current and remain.

**Recommended end state.** Remove the facade re-export and both hidden session
wrappers. Retain core `Query<E>` with internal visibility appropriate to fluent
builders. Keep `.trace()`, `.explain()`, and attribution on fluent terminal
surfaces.

**Semantic impact.** Source-surface reduction only.

**Focused validation.** Facade and generator compile tests, diagnostic-feature
build, fluent attribution tests, and a repository search for facade `Query` use.

### HCLA-006 — Projection response and compatibility aliases

**Files and symbols**

- `crates/icydb-core/src/db/response/mod.rs` —
  `type ProjectionResponse<E> = Response<ProjectedRow<E>>`.
- `crates/icydb/src/db/response/mod.rs` — public wrapper
  `ProjectionResponse<E>`.
- `crates/icydb/src/db/response/rows.rs` — `ProjectionRows` and
  `RowProjectionOutput`.
- `crates/icydb/src/db/sql/types.rs` — explicitly documented compatibility
  aliases `SqlProjectionRows` and `SqlQueryRowsOutput`.
- `crates/icydb/src/db/sql/{convert,table_render,tests}.rs` — current uses of
  compatibility names.
- `crates/icydb/src/db/session/write.rs` — write-returning assembly.
- `docs/guides/public-facade-api.md` — teaches
  `ProjectionResponse::from_core` despite no producer.

**Current behavior.** Current SQL and fluent write-returning endpoints return
the Candid-friendly `RowProjectionOutput`. `ProjectionRows` is an assembly and
rendering intermediate. `RowProjectionOutput::as_projection_rows` clones the
payload to reconstruct that intermediate. The old typed
`ProjectionResponse<E>` has no function returning it; repository use is limited
to declarations, re-exports, and guide text.

**Historical design represented.** The `0.52` projection execution API returned
typed `ProjectionResponse<E>`. Later endpoint projection moved to structural
rows and `RowProjectionOutput`; SQL aliases were kept when the concrete payload
moved out of the SQL module.

**Obsolescence proof.** Producer/signature search finds no live
`ProjectionResponse` result. The two aliases call themselves compatibility
aliases. `SqlQueryRowsOutput` is widely used only because internal/current code
continued using the alias name, not because it denotes a distinct shape.

**Reachability and compatibility.** All named types are public. No external
compatibility obligation. No persisted format; `RowProjectionOutput` is a live
Candid response shape and must be retained or deliberately changed in its own
hard-cut slice.

**Tests preserving it.** SQL/facade tests instantiate the alias names. The guide
preserves the dead response constructor. No test proves a live API produces
`ProjectionResponse`.

**Recommended end state.** Delete core/facade `ProjectionResponse` and guide
text. Replace `SqlQueryRowsOutput` uses with `RowProjectionOutput` and delete the
alias. Delete `SqlProjectionRows`; make `ProjectionRows` private to response
assembly or render directly from `RowProjectionOutput`. If the intermediate
continues to enforce a useful construction invariant, retain it privately and
avoid clone-based round trips.

**Semantic impact.** Public names are hard-cut. Preserve the current Candid row
payload, column order, row count, rendering, and write/SQL result behavior.

**Focused validation.** Facade response tests, SQL rendering and conversion
tests, write-returning tests, generated Candid checks, integration SQL canister
tests, and public docs.

### HCLA-007 — Access-choice diagnostics discard and reconstruct typed facts

**Files and symbols**

- `crates/icydb-core/src/db/query/plan/access_choice/model.rs` —
  `AccessChoiceRejectedReason`, `AccessChoiceCandidateExplainSummary`, and
  `AccessChoiceExplainSnapshot { alternatives: Vec<String>, rejected:
  Vec<String> }`.
- `crates/icydb-core/src/db/query/plan/access_choice/mod.rs` — candidate
  evaluation followed by `render_for_index`.
- `crates/icydb-core/src/db/query/explain/plan.rs` —
  `ExplainRejectedIndexV1::from_rejection`,
  `parse_rejected_index_label`, and `selected_candidate_summary`.

**Current behavior.** The planner owns typed ranking and rejection enums, but
freezes rejected candidates as strings such as `index:<name>=<reason>`. Public
explain later parses that grammar into optional index/reason fields. Selected
candidate residual facts are found by comparing a rendered candidate label to a
selected label, with a fallback that chooses the sole candidate if labels fail
to match.

**Historical design represented.** A text-first EXPLAIN model subsequently
wrapped in a structured V1 response. Structured diagnostics were added without
moving typed identity across the boundary.

**Obsolescence proof.** The typed reason exists before stringification, so no
information or layering constraint requires parsing. Both parser and fallback
exist solely because the snapshot discarded structure.

**Reachability and compatibility.** Reached by current EXPLAIN and diagnostics;
not reached by execution decisions. Public `ExplainRejectedIndexV1.label` may be
a current diagnostic field, but internal parser grammar is not persisted. No
cursor/storage format is involved.

**Tests preserving it.** Planner/explain tests and execution semantic snapshots
assert rendered labels and structured fields. They should be updated to assert
the typed projection, retaining stable outward strings only where explicitly
part of the diagnostic response.

**Recommended end state.** Store a typed rejected-candidate record containing
index identity and `AccessChoiceRejectedReason` in
`AccessChoiceExplainSnapshot`. Give eligible candidates a stable typed identity
used for selection. Derive `ExplainRejectedIndexV1` directly and render `label`
last. Delete `parse_rejected_index_label`, string identity matching, and the
one-candidate fallback.

**Semantic impact.** Execution unchanged. Intended diagnostic values unchanged;
incorrect or ambiguous reconstruction becomes impossible.

**Focused validation.** Access-choice ranking tests, logical/verbose/JSON
EXPLAIN tests, same-score tie-break tests, execution semantic snapshots, and
diagnostic schema tests.

### HCLA-008 — Grouped route observability reports an impossible fallback model

**Files and symbols**

- `crates/icydb-core/src/db/executor/planning/route/contracts/shape.rs` —
  `GROUPED_AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 0]`.
- `crates/icydb-core/src/db/executor/planning/route/contracts/execution/plan.rs`
  — `ExecutionRoutePlan::grouped_observability`.
- `crates/icydb-core/src/db/executor/planning/route/contracts/execution/observability.rs`
  — `GroupedRouteDecisionOutcome`, `GroupedRouteRejectionReason`, and
  `GroupedRouteObservability`.
- `crates/icydb-core/src/db/executor/explain/descriptor/load.rs` — grouped
  projection into diagnostics.
- `crates/icydb-core/src/db/executor/planning/route/tests/grouped.rs` and
  `crates/icydb-core/src/db/executor/tests/semantics.rs` — tests/snapshots.

**Current behavior.** Grouped aggregation intentionally has no scalar fast-path
order and always uses materialized grouped execution. Observability computes
`eligible = self.fast_path_order.is_empty()`, which is always true for a valid
grouped plan, and maps true to `MaterializedFallback`. The false branch emits
`Rejected(CapabilityMismatch)` but no current grouped plan can reach it. Even
plans with `GroupedPlanFallbackReason::None` report
`grouped_route_outcome=materialized_fallback`.

**Historical design represented.** Scaffolding for a future or abandoned
grouped fast-path selection gate. It models materialization as falling back from
an option that the route contract deliberately does not contain.

**Obsolescence proof.** The canonical grouped fast-path array is statically
empty and route-intent tests require it. All valid grouped route constructors
use that array. Snapshot evidence shows `eligible=true`,
`materialized_fallback`, and fallback reason `none` together.

**Reachability and compatibility.** The true/fallback diagnostic is reached for
all grouped queries. The rejection variant is unreachable. Execution is not
affected. Diagnostic JSON/text is public observability, not a persisted format.

**Tests preserving it.** Many grouped route tests assert
`MaterializedFallback`; the semantic snapshot embeds three grouped route fields.
Those tests currently preserve the false model rather than a current product
contract.

**Recommended end state.** Retain `GROUPED_AGGREGATE_FAST_PATH_ORDER` if the
empty order remains a useful explicit invariant. Remove its use as an
eligibility flag. Expose the actual `GroupedExecutionMode` and the planner-owned
`GroupedPlanFallbackReason`. Delete `GroupedRouteDecisionOutcome`,
`GroupedRouteRejectionReason`, `eligible`, and their diagnostic properties
unless a real alternate grouped route is introduced now.

**Semantic impact.** Diagnostics change from “materialized fallback” to the
actual selected grouped mode; execution and result ordering/cardinality must not
change.

**Focused validation.** Grouped route tests, hash/ordered materialization
parity, grouped cursor tests, verbose/JSON explain, semantic snapshots, and
metrics labels.

### HCLA-009 — Generated-model fallback survives only in metrics

**Files and symbols**

- `crates/icydb-core/src/metrics/sink/events.rs` —
  `PreparedShapeFinalizationOutcome::{AlreadyFinalized, GeneratedFallback}` and
  docs.
- `crates/icydb-core/src/metrics/sink/counters/planning.rs` — branch counters.
- `crates/icydb-core/src/metrics/state.rs`, `state/ops.rs`, and
  `state/summary.rs` — `prepared_shape_generated_fallback` state/accessors.
- `crates/icydb-core/src/db/executor/authority/entity.rs` — sole production
  producer, recording only `AlreadyFinalized`.
- `crates/icydb-core/src/metrics/tests.rs` — directly manufactures both
  outcomes and expects the fallback counter.
- `docs/design/0.200-schema-native-enums/0.200-design.md` — accepted schema is
  sole runtime authority and generated reconstruction is forbidden.

**Current behavior.** If the plan already has a static execution-planning
contract, executor authority records `AlreadyFinalized`. Otherwise it requires
accepted `SchemaInfo` and finalizes from accepted schema. It never records or
performs `GeneratedFallback`. Tests are the only producer of that variant.

**Historical design represented.** Executor lowering once finalized missing
static shape from generated `EntityModel`. The metric was designed to measure
the transition away from that fallback.

**Obsolescence proof.** Production producer search finds only
`AlreadyFinalized`. The missing-contract branch fails without accepted schema
and calls the accepted-schema finalizer. The active `0.200` design explicitly
forbids generated runtime reconstruction.

**Reachability and compatibility.** The fallback branch is unreachable in
production. Metrics fields are observable but not persisted database state.
There is no compatibility obligation before 1.0.

**Tests preserving it.** Metrics tests inject `GeneratedFallback`, assert a
counter increment, and include the field in summaries. These tests preserve an
illegal authority path as if it were live.

**Recommended end state.** Delete `GeneratedFallback`, its counter/state/summary
fields and test data. Replace the two-variant outcome event with a directly named
`prepared_shape_already_finalized`/reuse recording function if the remaining
counter is operationally useful; otherwise delete the entire transition metric.
Do not add a new accepted-vs-generated outcome enum.

**Semantic impact.** No database/query behavior change. Metrics schema loses a
dead field and more accurately describes accepted-schema authority.

**Focused validation.** Metrics unit/snapshot tests, executor authority tests,
accepted-schema missing/finalized tests, generated diagnostics endpoints, and a
search proving `GeneratedFallback` is absent.

### HCLA-010 — CLI update-policy compatibility spelling

**Files and symbols**

- `crates/icydb-cli/src/cli/config.rs` —
  `ConfigInitUpdatePolicy::PrimaryKey` with
  `#[value(name = "primary-key", alias = "primary_key")]`.
- `crates/icydb-cli/src/tests/cli.rs` —
  `cli_args_config_init_update_policy_accepts_primary_key_alias`.

**Current behavior.** Both `--update-policy primary-key` and
`--update-policy primary_key` parse. Generated config uses `"primary_key"`,
which is a separate TOML/config grammar and is not the CLI alias.

**Historical design represented.** A transitional command-line spelling kept
after hyphenated Clap values became canonical.

**Obsolescence proof.** It is explicitly declared as an alias and has a test
whose sole purpose is accepting the alternate spelling. No doc requires the
underscore CLI spelling.

**Reachability and compatibility.** Live public CLI compatibility only; no
persisted data. No external users require it.

**Recommended end state.** Delete the Clap alias and alias-only test. Keep
`config_value() -> "primary_key"` unless the configuration language itself is
separately hard-cut.

**Semantic impact.** Intentional rejection of one obsolete CLI spelling.

**Focused validation.** CLI parser tests, config-init output tests, generated
config tests, and help/snapshot output.

### HCLA-011 — Anti-resurrection compile-fail tests

**Files and symbols**

- `testing/macro-tests/tests/ui/db_default_removed.rs` and `.stderr` — proves
  removed `db_default` is rejected and suggests `default`.
- `crates/icydb-core/tests/ui/db/contracts_module_absent.rs` and `.stderr` —
  proves deleted `icydb_core::db::contracts` cannot be imported.
- `crates/icydb-core/tests/ui/ARCHITECTURE_GUARDS.md` — stale row still names
  `db/contracts_module_private.rs` and says the removed module “remains private.”

**Current behavior.** UI suites compile intentionally obsolete syntax/path and
snapshot the compiler failure. Positive current-default tests and current
module-privacy tests exist independently.

**Historical design represented.** `db_default` was a prior macro spelling.
`db::contracts` was first guarded as private and was then deleted in `0.131.1`;
history shows the UI file was renamed from “private” to “absent” at deletion.

**Obsolescence proof.** Each test's only success condition is that a removed
surface remains removed. Neither tests a current type invariant, privacy
boundary of a live module, malformed current input, nor persisted-format
rejection. The project hard-cut rules explicitly prohibit these tests.

**Reachability and compatibility.** Test-only. No runtime or persisted
compatibility.

**Recommended end state.** Delete both `.rs`/`.stderr` pairs and the stale guard
index row. Keep positive `default`/Rust-default tests and compile-fail tests that
enforce live syntax or live private boundaries.

**Semantic impact.** None.

**Focused validation.** Macro trybuild suite, core UI suite, and guard-index
consistency.

### HCLA-012 — Active documentation and version terminology describe old designs

**Files and symbols**

- `docs/contracts/CURSOR.md` — a historical checkpoint in the active contracts
  directory whose “Current Behavior” still says CBOR and no cursor pushdown.
- `crates/icydb-core/src/db/cursor/token/codec.rs` — actual custom bounded
  binary `TOKEN_WIRE_VERSION: u8 = 2` implementation.
- `docs/architecture/SQL_SURFACE_MAPPING.md` — broken active path to the archived
  `0.166` matrix and a paragraph legitimizing legacy test-only SQL lanes.
- `docs/contracts/READ_ADMISSION.md` — documents `DevTest` and hidden prebuilt
  helpers as live lane/surface facts.
- `docs/contracts/DURABILITY.md` and `docs/operations/DURABILITY_GUIDE.md` —
  links to `0.189` / `0.190` paths that moved under `docs/design/archive/`.
- `crates/icydb-core/src/db/query/plan/validate/plan_shape.rs`,
  `validate/errors.rs`, `validate/grouped/policy/mod.rs`, and
  `plan/tests/group.rs` — “GROUP BY v1” / “grouped v1” language with no v2 peer.

**Current behavior.** Cursor tokens are lower/upper-case-tolerant hex at the
external boundary over one custom, bounded binary wire. Version mismatch fails
closed; there is no CBOR decoder. Cursor continuation may be pushed into routes
that prove safe ordered continuation. Grouped planning has one current grouped
contract. SQL public-read helpers are test-only and scheduled for deletion by
HCLA-002.

**Historical design represented.** The February cursor checkpoint before route
pushdown and before final CBOR removal; SQL surface-matrix preservation during a
transition; an anticipated grouped v2; pre-archive design paths.

**Obsolescence proof.** Current source directly contradicts the checkpoint.
`docs/changelog/0.83.md` records final CBOR removal; the cursor codec accepts only
its current version. Reference search finds no grouped v2 architecture. The
referenced design files now exist only under archive paths.

**Reachability and compatibility.** Documentation/comments/errors are active
developer and diagnostic surfaces. Durable identifiers such as token version,
schema version, stable-memory names, `expr:v1`, and Structural Binary v1 are not
part of this finding and must remain unless their own persisted contract changes.

**Tests preserving it.** Grouped test messages say “grouped v1”; cursor tests
correctly test current bytes and therefore contradict rather than preserve the
stale doc. SQL tests preserve the old lane via HCLA-002.

**Recommended end state.** Move the cursor checkpoint to an archive or replace
its implementation claims with a short historical pointer to the normative
query contract. Repair archived design links. Remove legacy-matrix and phantom
lane claims after code deletion. Replace unqualified “grouped v1” text with the
actual invariant (for example grouped DISTINCT adjacency or grouped ORDER BY
admission) without inventing a v2 name.

**Semantic impact.** Documentation/diagnostic wording only.

**Focused validation.** Link check, docs grep for removed names, doc tests, UI
stderr review for changed error prose, and cursor/query contract review against
source.

## Deletion inventory

The following inventory is safe to remove in the named remediation slices. It
does not authorize deleting current persisted-format gates or current semantic
routes.

### Modules and files

- `crates/icydb-core/src/db/session/sql/execute/select/read_budget.rs` in full.
- `testing/macro-tests/tests/ui/db_default_removed.rs`.
- `testing/macro-tests/tests/ui/db_default_removed.stderr`.
- `crates/icydb-core/tests/ui/db/contracts_module_absent.rs`.
- `crates/icydb-core/tests/ui/db/contracts_module_absent.stderr`.
- The active implementation-claim body of `docs/contracts/CURSOR.md`; archive
  the checkpoint or replace it with a pointer rather than leaving a second
  “current” contract.

### Types and enum variants

- `PlannedQuery<E>`.
- `CompiledQuery<E>`.
- Their private `QueryPlanHandle`, if no remaining internal consumer exists
  after deleting both shells.
- Core and facade `ProjectionResponse<E>`.
- `SqlProjectionRows`.
- `SqlQueryRowsOutput`.
- `QueryAdmissionLane::AdminAdHoc` and `QueryAdmissionLane::DevTest` when the
  actual trusted bypass surface is made explicit and remains outside policy
  evaluation.
- `GroupedRouteDecisionOutcome`.
- `GroupedRouteRejectionReason`.
- `GroupedRouteObservability.eligible`, `.outcome`, and `.rejection_reason`.
- `PreparedShapeFinalizationOutcome::GeneratedFallback`.
- The entire `PreparedShapeFinalizationOutcome` enum after its remaining event
  is collapsed to a specifically named counter/event.

### Methods and constructors

- `DbSession::execute_sql_query_with_read_admission_policy`.
- `DbSession::execute_compiled_sql_context_with_read_admission_policy`.
- `DbSession::execute_global_aggregate_compiled_statement_ref_with_read_admission_policy`.
- Test-only SQL select methods whose only caller is that dispatch.
- `QueryAdmissionPolicy::admin_ad_hoc`.
- All public/core `.planned()` and `.plan()` terminals listed in HCLA-004.
- `DbSession::planned_query_with_visible_indexes`.
- `DbSession::compile_query_with_visible_indexes`.
- Facade `DbSession::execute_query_result_with_attribution` that accepts raw
  `Query<E>`.
- Facade `DbSession::trace_query` that accepts raw `Query<E>`.
- `ProjectionResponse::from_core` and its iteration/cardinality methods.
- `ExplainRejectedIndexV1::from_rejection` in its string-parsing form.
- `parse_rejected_index_label`.
- `selected_candidate_summary` label/fallback matching; replace with typed ID
  lookup, not another string helper.
- `ExecutionRoutePlan::grouped_observability` fields that manufacture outcome
  and eligibility; retain a smaller projection of actual grouped mode/fallback
  reason if needed.
- `RowProjectionOutput::as_projection_rows` if renderers consume the current
  payload directly.

### Re-exports and public surface

- Facade/core re-exports of `PlannedQuery` and `CompiledQuery`.
- Facade hidden re-export of `Query<E>`.
- Facade/core re-exports of `ProjectionResponse`.
- SQL module re-exports of `SqlProjectionRows` and `SqlQueryRowsOutput`.
- Facade re-exports of `QueryAdmissionAccessKind`,
  `QueryAdmissionDecision`, `QueryAdmissionGroupedSummary`,
  `QueryAdmissionLane`, `QueryAdmissionOrdering`,
  `QueryAdmissionPlanShape`, `QueryAdmissionRejection`,
  `QueryAdmissionResidualFilter`, `QueryAdmissionSummary`,
  `QueryBoundKind`, and `QueryMaterializationSummary`.
- The `primary_key` Clap alias for `ConfigInitUpdatePolicy::PrimaryKey`.

### Metrics and diagnostic fields

- `prepared_shape_generated_fallback` in metrics ops, state, summary, reports,
  snapshots, accessors, and tests.
- The `GeneratedFallback` sink-counter match arms.
- `grouped_route_eligible`.
- `grouped_route_outcome` while it can only mean the fictitious
  `materialized_fallback`.
- `grouped_route_rejection_reason` while no rejection is reachable.
- The string-only `AccessChoiceExplainSnapshot.rejected` representation after a
  typed replacement lands.

### Tests and fixtures

- Every `public_read_sql_*` test whose contract is executing SQL through
  `PublicRead` policy.
- `public_read_sql_text_projection_values` and helpers used only by that matrix.
- Admission tests that construct `AdminAdHoc` only to preserve the unused lane.
- Planned-vs-compiled parity tests that assert two shells over the same plan.
- Projection-response facade examples/tests with no producer.
- Grouped route tests/snapshot fields that assert “materialized fallback” solely
  because the fast-path list is empty.
- Metrics tests that manufacture `GeneratedFallback` or seed the dead counter.
- `cli_args_config_init_update_policy_accepts_primary_key_alias`.
- Both anti-resurrection UI fixture pairs named above.

### Comments and documentation sections

- “legacy matrix coverage” justification in
  `docs/architecture/SQL_SURFACE_MAPPING.md`.
- `DevTest` and hidden prebuilt-query surface claims in
  `docs/contracts/READ_ADMISSION.md` after code cleanup.
- Public guide entries for `.planned()`, `.plan()`, and
  `ProjectionResponse::from_core`.
- Comments/docs describing generated-model prepared-shape fallback.
- Unqualified “GROUP BY v1” / “grouped v1” terminology.
- Stale `db/contracts_module_private.rs` row in
  `crates/icydb-core/tests/ui/ARCHITECTURE_GUARDS.md`.

### Feature flags and persisted fields

No feature flag or persisted field met the deletion-proof threshold. The `sql`,
`sql-explain`, and `diagnostics` features each have a meaningful current build.
No prepared plan is persisted. Version fields on durable codecs remain active
fail-closed format boundaries.

## Authority-conflict map

| Domain fact | Current competing representations/owners | Intended surviving owner | Resolution |
| --- | --- | --- | --- |
| SQL trust/admission lane | Method docs; generated controller guard; public `QueryAdmissionLane::AdminAdHoc`; no runtime lane producer | Explicitly named trusted SQL surface plus generated controller guard | HCLA-001 hard-cut method name; remove phantom policy lane |
| Public-read admission | Fluent built-in policy; test-only SQL policy execution and response budgeting | Internal admission policy invoked by ordinary fluent terminals | Delete test SQL lane (HCLA-002) |
| Admission public vocabulary | Internal summary/policy types; facade re-exports | Internal admission + frozen diagnostics renderer | Privatise DTOs (HCLA-003) |
| Query lifecycle stage | `PlannedQuery`; `CompiledQuery`; actual `SharedPreparedExecutionPlan` | `AccessPlannedQuery` for logical plan and `SharedPreparedExecutionPlan` for executor-ready state | Delete inert shells (HCLA-004) |
| Public query construction | Fluent builder; hidden direct `Query<E>` | Fluent builder/semantic terminals | Remove facade direct query (HCLA-005) |
| Projection response shape | `ProjectionResponse<E>`; `ProjectionRows`; `RowProjectionOutput`; two SQL aliases | Planner projection contract internally; `RowProjectionOutput` at endpoint boundary | Delete/privatise extras (HCLA-006) |
| Rejected index identity/reason | Typed planner enums; rendered `index:name=reason`; reparsed optional fields | Typed planner snapshot | Carry typed records to renderer (HCLA-007) |
| Selected access-candidate identity | Planner candidate; rendered label; single-candidate fallback | Stable typed candidate/index identity | Delete label matching (HCLA-007) |
| Grouped route identity | `GroupedExecutionMode`; planner fallback reason; manufactured route outcome/eligibility | Selected grouped mode plus real planner fallback reason | Delete fictitious route state (HCLA-008) |
| Static-plan schema authority | Accepted `SchemaInfo`; dead generated-fallback metric | Accepted schema / `EntityAuthority` | Delete fallback metric vocabulary (HCLA-009) |
| Cursor implementation truth | Custom binary v2 codec and route-dependent pushdown; historical CBOR/no-pushdown “Current Behavior” | Cursor codec/validation plus normative query contract | Archive/rewrite checkpoint (HCLA-012) |
| Grouped contract generation | One grouped planner; comments/errors call it “v1” | Current grouped validation invariant | Remove misleading version adjective (HCLA-012) |

The following high-risk facts were checked and did **not** show competing owners:

- filter truth: `NormalizedFilter` retains the expression and only derives a
  predicate subset for access/execution optimization;
- runtime residual filtering: `EffectiveRuntimeFilterProgram` is the one
  executor-facing program contract;
- order requirements: canonical logical ordering and `ResolvedOrder` feed both
  route planning and cursor construction;
- response cardinality: response/cardinality helpers own entity cardinality,
  while executors own only row production;
- cache identity: SQL compile cache and semantic plan cache represent different
  stages and share accepted revision/fingerprint guards;
- persisted schema fingerprint: accepted schema/catalog owners supply it to
  sessions, caches, and execution contexts rather than consumers recomputing it.

## False positives and deliberate retention

### Current pagination response types

`PagedResponse` and `PagedGroupedResponse` are not the obsolete response-page
facade described by the audit prompt. They contain current items, opaque next
cursor, trace/read-intent metadata, and no page number, total pages, or ambient
offset state. Retain them.

### SQL `OFFSET`

`OFFSET` remains intentional in trusted/admin reduced SQL. Ordinary public
fluent reads do not expose it, and `PublicRead` rejects nonzero offset. This is a
semantic lane distinction, not a compatibility path.

### Cursor wire version and rejection corpus

`TOKEN_WIRE_VERSION = 2` is the sole accepted cursor wire. The decoder checks
equality and fails closed; it has no legacy branch. Tests for old/future tags are
malformed/unsupported-current-boundary tests, not compatibility tests. Retain
the version byte and rejection coverage.

### Database boot legacy-magic check

`LEGACY_STABLE_CELL_MAGIC` in `db/database_format/mod.rs` does not decode an old
database. It detects a non-virgin pre-boot-record stable cell and refuses to
initialize over it. It protects real data from overwrite and should remain,
with its fail-closed purpose documented.

### Durable `v1` names and version fields

Stable-memory allocation names, Structural Binary v1, `expr:v1`, database boot
version, row/commit/journal/schema codec versions, schema `SchemaVersion` values,
and cache method versions are current identity/version contracts. A schema v1
followed by schema v2 in tests represents accepted schema revisions, not dual
execution architecture. Do not rename these cosmetically.

### One current codec per durable surface

The persisted-format inventory and source show one active database boot format,
one raw-row envelope, one commit marker/control format, one journal batch format,
one accepted schema snapshot/catalog format, one index encoding, and one cursor
wire. Unsupported versions reject. No compatibility reader, migration decoder,
or “try old then new” branch was found. Retain version gates and bounded fallible
decoders.

### Generated-model-only schema visibility

`VisibleIndexes::GeneratedModelOnly` and generated `SchemaInfo` constructors are
used for proposal, reconciliation/model-only utilities, and tests. Production
guarded execution selects accepted schema or fails closed. This matches the
architecture rule and is not evidence of runtime fallback. The obsolete metric
in HCLA-009 is the part to remove.

### Predicate and expression representations

`Expr` is the full semantic truth representation. `Predicate` is a deliberately
narrow, validated subset for access selection and efficient residual execution.
`ResidualFilterContract` and `EffectiveRuntimeFilterProgram` preserve which
forms are available without reconstructing missing truth. These are justified
stage contracts, not equivalent peer authorities.

### Prepared execution wrappers

`PreparedExecutionPlan<E>` and `SharedPreparedExecutionPlan` share a core but
serve a real typed vs type-erased cache/handoff boundary. Unlike
`PlannedQuery`/`CompiledQuery`, they are consumed by production execution and
carry executor-ready state. Retain them.

### Grouped materialization algorithms

`HashMaterialized` and `OrderedMaterialized` are distinct physical algorithms
with different ordering/precondition behavior. `GroupedPlanFallbackReason` can
explain a real planner choice between them. Retain these; remove only the outer
fiction that all materialized grouped execution is a route fallback.

### Current fallback terms

Load-order materialization fallback, covering-read fallback, predicate-pushdown
fallback, and scalar aggregate capability fallback remain real route choices
where a preferred optimization can be proven or rejected without changing
semantics. Their producers and consumers are live. Do not delete “fallback” by
keyword alone.

### Explain `V1` DTOs

`ExplainAccessDecisionV1` and related DTOs are one current versioned diagnostic
projection, not a v1/v2 executor split. They remain reachable through public
EXPLAIN. Retain the version tag until a deliberate diagnostic-schema hard cut;
fix HCLA-007 underneath it.

### Old/new row images

“Old row” and “new row” in commit/journal/mutation code are before/after images
for atomic mutation and recovery. They are not legacy/current persisted-format
decoders.

## Architectural uniqueness result

| Question | Answer | Evidence / finding |
| --- | --- | --- |
| Is there exactly one ordinary typed/fluent query architecture? | **Yes at runtime; no at the public diagnostic surface.** | Fluent execution is singular, but direct `Query` and duplicate planned/compiled stages remain (HCLA-004, HCLA-005). |
| Is there exactly one SQL architecture? | **Yes in production; no repository-wide.** | Production uses one SQL compile/execute system; test-only public-SQL admission is a shadow architecture (HCLA-002). |
| Where do SQL and fluent semantics converge? | **At `StructuralQuery`, shared access planning/cache, and `SharedPreparedExecutionPlan`.** | SQL frontend retains only syntax/projection/result concerns after lowering. |
| Is there exactly one owner of public-read admission? | **Yes in production, no across source/tests.** | Fluent built-in policy is live owner; test SQL duplicates application (HCLA-002). |
| Is there exactly one owner of ordering requirements? | **Yes.** | Logical order canonicalization and planner `ResolvedOrder` feed route/cursor decisions; admission consumes summaries. |
| Is there exactly one cursor contract? | **Yes in code; no in active documentation.** | One current v2 binary codec; stale CBOR checkpoint (HCLA-012). |
| Is there exactly one runtime residual-filter contract? | **Yes.** | `ResidualFilterContract` lowers to `EffectiveRuntimeFilterProgram`; no independent executor reconstruction found. |
| Is there exactly one authoritative route-selection result? | **Yes for execution; grouped diagnostics add false state.** | `ExecutionRoutePlan` selects execution, while HCLA-008 manufactures fallback/rejection facts. |
| Do diagnostics consume execution truth rather than reconstruct it? | **No.** | String reparse/label fallback (HCLA-007) and grouped route fiction (HCLA-008). |
| Are trusted/admin bypasses explicit and isolated? | **Fluent yes; SQL no.** | `trusted_read_unchecked()` is explicit; `execute_sql_query` is ambient (HCLA-001). |
| Does each public type correspond to a current developer use case? | **No.** | Admission DTOs, planned/compiled shells, direct `Query`, and projection response/aliases (HCLA-003 through HCLA-006). |
| Are obsolete compatibility promises absent? | **No.** | SQL aliases, CLI alias, and anti-resurrection tests (HCLA-006, HCLA-010, HCLA-011). |
| Are all alternate execution routes justified by semantics rather than history? | **Production routes mostly yes; shadow SQL admission and grouped observability no.** | HCLA-002 and HCLA-008. |
| Can a new contributor identify the current path without understanding prior pivots? | **No.** | Conflicting public stages, legacy tests, and active stale docs (HCLA-002–HCLA-006, HCLA-012). |

## Remediation sequence

Each slice is intentionally hard-cut and must land without aliases, shims, dual
dispatch, or anti-resurrection tests.

### Slice 1 — Make trusted SQL authority explicit

**Theme:** correctness and bypass risk.

**Likely files:** `crates/icydb/src/db/session/sql.rs`,
`crates/icydb-core/src/db/session/sql/mod.rs`,
`crates/icydb-build/src/db/sql.rs`, generator tests,
`docs/contracts/READ_ADMISSION.md`, public guide/examples.

**Delete:** ambient `execute_sql_query` name; unused `AdminAdHoc` / `DevTest`
policy variants and test-only constructor if the trusted method remains outside
policy evaluation.

**Authority after:** explicitly named trusted SQL method; generated controller
gate owns endpoint authorization; internal admission owns only actual evaluated
lanes.

**Must not change:** SQL subset, controller check, accepted-schema lookup,
planner/cache route, response DTO, error semantics, or perf attribution.

**Focused tests:** facade compile tests, generator source assertions, SQL
execution, controller-gated integration, diagnostics feature build.

### Slice 2 — Delete the shadow public-SQL admission system

**Theme:** authority unification.

**Likely files:** core SQL session/execute modules,
`execute/select/read_budget.rs`, `session/tests/read_admission.rs`,
`docs/architecture/SQL_SURFACE_MAPPING.md`.

**Delete:** all test-only SQL policy entry/dispatch/budget code and tests whose
product contract is public SQL.

**Authority after:** `QueryAdmissionPolicy` is exercised by fluent public reads
and diagnostic explain only; SQL lowering parity is tested independently.

**Must not change:** fluent policy outcomes, SQL trusted execution, generated
controller gates, grouped hard caps, or response-byte enforcement on actual
public fluent responses.

**Focused tests:** planner/admission unit tests, fluent read-admission tests, SQL
lowering and trusted execution tests, generated surface tests.

### Slice 3 — Make diagnostics carry typed execution truth

**Theme:** planner/diagnostic authority unification.

**Likely files:** `query/plan/access_choice/{model,mod}.rs`,
`query/explain/plan.rs`, explain tests/snapshots.

**Delete:** rejection-string parser, selected-label matching, sole-candidate
fallback.

**Authority after:** planner snapshot carries typed candidate identity and typed
rejection reason; renderers only project.

**Must not change:** access selection, ranking, plan hash/cache identity,
execution, or outward reason codes unless an existing diagnostic is proven
wrong.

**Focused tests:** ranking/tie-break, structured V1 explain, text/JSON/verbose
parity, semantic snapshots.

### Slice 4 — Remove fictitious grouped-route state

**Theme:** route-selection truth.

**Likely files:** executor route execution contracts, grouped explain
projection, query explain property keys, grouped route and semantic tests.

**Delete:** grouped eligibility/outcome/rejection types and properties.

**Authority after:** `GroupedExecutionMode` plus real
`GroupedPlanFallbackReason`.

**Must not change:** hash vs ordered algorithm selection, row order, grouped
limits, cursor behavior, or aggregate results.

**Focused tests:** grouped route selection, algorithm parity, grouped cursors,
explain/metrics/snapshots.

### Slice 5 — Excavate unreachable fallback and compatibility machinery

**Theme:** deletion of unreachable paths.

**Likely files:** metrics sink/state/summary/tests, executor entity authority,
CLI config/tests, both UI fixture directories and architecture guard index.

**Delete:** `GeneratedFallback` metric path, `primary_key` CLI alias, two
anti-resurrection fixture pairs.

**Authority after:** accepted schema is the only prepared-shape authority;
canonical CLI spelling and current positive compile contracts stand alone.

**Must not change:** accepted-schema finalization, config output grammar,
current default syntax, or live module privacy tests.

**Focused tests:** metrics, accepted-schema executor tests, CLI, macro trybuild,
core UI suite.

### Slice 6 — Hard-cut inert query stages and direct facade query

**Theme:** public query API contraction.

**Likely files:** core query intent/session planning/fluent builders, facade
load/delete/query/session modules, re-exports, guide and query tests.

**Delete:** `PlannedQuery`, `CompiledQuery`, `.planned()`, `.plan()`, raw facade
`Query` re-export, and raw-query trace/attribution wrappers.

**Authority after:** fluent builders for public construction;
`AccessPlannedQuery` internal logical plan; `SharedPreparedExecutionPlan`
executor handoff; fluent diagnostics for inspection.

**Must not change:** query construction, plan hash, explain content, cache reuse,
admission, or execution.

**Focused tests:** core query/explain, facade compile/API, generator, fluent
attribution, docs.

### Slice 7 — Collapse response/projection public vocabulary

**Theme:** public API hard cut and internal simplification.

**Likely files:** core/facade response modules, SQL types/convert/render/tests,
write facade, integration SQL types, guide.

**Delete:** `ProjectionResponse`, SQL compatibility aliases, public
`ProjectionRows` exposure, clone-back adapter if no invariant requires it.

**Authority after:** planner/executor projection contract internally and one
`RowProjectionOutput` at the Candid/facade boundary.

**Must not change:** output values, entity/columns/rows/row_count wire shape,
column order, rendering, or returning semantics.

**Focused tests:** SQL conversion/rendering, write returning, facade response,
Candid/generated API, integration SQL canister.

### Slice 8 — Privatise admission internals

**Theme:** public/internal type separation.

**Likely files:** facade/core `db/mod.rs`, admission modules, explain renderer,
public API tests/docs.

**Delete:** facade admission DTO re-exports; unnecessary `pub` visibility.

**Authority after:** admission and frozen diagnostics internals; public callers
receive domain errors/codes and rendered explain, not policy construction DTOs.

**Must not change:** diagnostic codes, rejection text/JSON, policy decisions, or
public result types.

**Focused tests:** facade public API, admission, explain rendering, error-code
mapping.

### Slice 9 — Align active docs and terminology

**Theme:** diagnostics, naming, and documentation cleanup.

**Likely files:** `CURSOR.md`, `QUERY_CONTRACT.md`, `READ_ADMISSION.md`,
`SQL_SURFACE_MAPPING.md`, `DURABILITY.md`, durability guide, grouped validation
comments/errors/tests, public facade guide.

**Delete:** stale CBOR/no-pushdown “current” text, legacy matrix rationale,
phantom lane text, `v1` adjectives without a peer, broken active paths.

**Authority after:** normative contracts point to current implementation and
archived designs only as history.

**Must not change:** durable wire/version names or error meaning.

**Focused tests:** docs/link check, doctests, UI stderr affected by wording, and
repository searches for removed names.

## Final hard-cut checklist

- [ ] No deprecated public APIs.
- [ ] No aliases for removed APIs.
- [ ] No v1/v2 execution split; remaining version tags are documented durable or
      diagnostic schema identities.
- [ ] No duplicate planner authority.
- [ ] No duplicate admission authority.
- [ ] No obsolete query DTOs.
- [ ] No unreachable plan or route-observability variants.
- [ ] No stale cursor format unless a documented persisted compatibility
      obligation requires it.
- [ ] No old response pagination or projection-response facade.
- [ ] No application-facing custom policy machinery or internal admission DTO
      exports.
- [ ] No fallback route masking planner errors.
- [ ] No tests preserving retired behavior.
- [ ] No stale generated methods.
- [ ] No active docs describing removed designs.
- [ ] No metrics or diagnostics using obsolete terminology.
- [ ] No compatibility module or alias without a documented live compatibility
      obligation.
- [ ] Trusted/admin SQL entry is explicit at the method/type boundary and
      generated SQL remains controller-gated.
- [ ] Diagnostics project typed planner/route truth without reparsing labels.
- [ ] Accepted schema remains the sole runtime schema authority.
- [ ] One current fail-closed decoder remains for each persisted format.

## Verification Readout

| Validation area | Status | Notes |
| --- | --- | --- |
| Report whitespace/path convention | PASS | Canonical dated report path is used; report-only whitespace validation passed. |
| Formatting | PASS | Cargo sort, derive ordering, and Rust formatting checks passed for the workspace. |
| Workspace compilation | PASS | Workspace all-target/all-feature compilation passed. |
| Clippy | PASS | Warnings-denied workspace/all-targets, core SQL-only, and core diagnostics-only lint lanes passed. |
| Unit tests | PASS | Core: 4,483 passed and 5 ignored; facade: 74 passed; generator: 18 passed; CLI: 132 passed; schema derive: 126 passed; macro/schema runtime: 123 passed; ancillary library suites: 94 passed. |
| Focused integration tests | PASS | SQL perf-matrix structure/classification: 40 passed, 7 ignored; PK focused-manifest test: 1 passed, 3 ignored. All integration test targets also compiled during all-target checks. |
| Full PocketIC/ICP integration execution | BLOCKED | Not run: project rules reserve ICP lifecycle for the user, and the audit changed no runtime code. PocketIC/manual perf and SQL-canister runtime cases remain unexecuted. |
| Compile-fail/UI tests | PASS | Core UI, facade public compile contract, and macro trybuild suites passed, including the two anti-resurrection fixtures classified for later deletion. |
| Feature matrix | PASS | No-default, SQL-only, diagnostics-only, and workspace no-default checks passed. |
| Generated-code tests | PASS | Generator unit tests passed; generated SQL canister libraries compiled. |
| Documentation tests | PASS | Workspace doc-test invocation passed; crates contain zero executable doctests. The build emitted an existing dirty-worktree dead-code warning for `AcceptedRowLayoutRuntimeContract::field_kind_by_name`. |
| Benchmark compilation | FAIL | `cargo check --workspace --benches --all-features` failed in the concurrently changing user-owned persisted-row/default worktree: `save/structural.rs:509` cannot see a helper re-export gated by `cfg(test)`, followed by a key-type mismatch at line 521. The audit report does not touch either file; the expensive check was not rerun. |
| Wasm/perf measurement | BLOCKED | No measurement was run: this is a report-only change, benchmark compilation failed, and no wasm-producing source changed. |
| Architecture checks | PASS | Dependency graph, executor panic policy, generated config, index range, layer authority, mutation atomicity, durability docs, read admission, SQL ownership, and memory-ID invariants passed. |
| Full `make test` | BLOCKED | Intentionally not run because repository agent rules prohibit the full push-workflow suite; the focused suites above are the broadest permitted validation. |

## Change and cost readout

- Production files changed by this audit: **0**.
- Audit artifacts added: **1 Markdown report, approximately 1,400 lines**.
- Runtime implementation complexity: **unchanged**.
- Runtime performance delta: **none; report-only change**.
- Raw non-gzipped wasm delta: **0 bytes by construction; no wasm-producing
  source changed**. No wasm artifact was rebuilt.
- Gzip wasm context: not applicable.
- Changelog: not changed because this is a governance/audit-only artifact.

## Follow-up ownership

Because overall risk is high, follow-up is required:

| Owner boundary | Action | Target report/run |
| --- | --- | --- |
| Facade + generated SQL + admission | Complete Slices 1–2 and prove explicit trusted SQL plus one public-read owner | Next SQL/admission hard-cut closeout |
| Query planner + diagnostics | Complete Slices 3–4 and prove diagnostics consume typed execution truth | Next planner/diagnostics closeout |
| Metrics + CLI + compile-fail suites | Complete Slice 5 deletion inventory | Next routine cleanup slice |
| Query facade + response facade | Complete Slices 6–8 without aliases | Next public API hard-cut closeout |
| Contracts/docs | Complete Slice 9 after code names settle | Final hard-cut closeout audit |
