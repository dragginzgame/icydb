# IcyDB 0.197 Closeout Audit

## Executive Summary

Verdict: `PARTIAL_CLOSEOUT`.

0.197 has a substantial deterministic exact-primary-key implementation: strict primary-key equality can route as key access, finite primary-key sets are bounded, invalid exact-key-looking inputs fail closed in the tested paths, admission consumes planner-selected exact-key access, and SQL/fluent/explain tests cover the main behavior. It is not yet a full closeout because several proof and artifact gates from the design remain incomplete.

Top blockers:

1. Real 0.197 focused before/after/delta performance artifacts are missing. The only focused artifacts found were local `/tmp` synthetic smoke artifacts.
2. Fresh full deterministic SQL matrix before/after evidence was not present and was not reproduced.
3. The design-required `PrimaryKeyCanonicalization` artifact is implemented implicitly through `AccessPlan`, access snapshots, and input-resource summaries, not as the explicit planner-owned enum described by the design.
4. `Empty` has semantic tests, but zero-IO proof is incomplete for shapes such as `pk = a AND pk IN (b)`.
5. SQL parameter exact-key canonicalization is design-required but currently unsupported by the SQL parameter contract; this needs either implementation or a design amendment.

Top non-blocking follow-ups:

1. Update `READ_ADMISSION.md` so exact strict primary-key filters are documented as admitted, not only explicit `by_id(...)`.
2. Add canonicalization disabled/failure diagnostics, or explicitly document the current diagnostic contract as selected-access oriented.
3. Link the eventual 0.197 closeout artifacts from the design and changelog.
4. Add fast-path inventory detail for the exact-primary-key canonicalization owner and guards.
5. Re-run broad workspace validation in an environment with a configured PocketIC binary.

0.197 can be considered closed: No.

0.198 design can start safely: Yes. The boundary is clear and no 0.198 API implementation was found.

0.198 implementation can start safely: Not yet. It should wait until the 0.197 closeout blockers are either fixed or explicitly re-scoped, because 0.198 read-intent ergonomics depends on the exact-key admission contract being stable.

Biggest residual risk: admission and execution mostly agree through selected access today, but the lack of a single explicit canonicalization artifact leaves proof ownership spread across planning, access canonicalization, admission, explain, and cache keys.

Performance claim level: `none`. The current evidence supports behavior/correctness progress, not a measured performance closeout.

## Scope

Date: 2026-07-06

Commit: `b7309b5ec0f16cb8bbe6a917ca754bc98316ea90`

Dirty worktree at audit start:

| Path | Classification | Notes |
| --- | --- | --- |
| `CHANGELOG.md` | expected 0.197 closeout artifact | Uncommitted changelog slice. |
| `docs/changelog/0.197.md` | expected 0.197 closeout artifact | Uncommitted detailed changelog slice. |
| `docs/design/0.197-deterministic-optimizer-canonicalization/focused-matrix-manifest.json` | expected 0.197 closeout artifact | Focused matrix manifest update. |
| `docs/design/0.197-deterministic-optimizer-canonicalization/focused-matrix-manifest.md` | expected 0.197 closeout artifact | Human-readable manifest update. |
| `docs/design/0.197-deterministic-optimizer-canonicalization/implementation-results.json` | expected 0.197 closeout artifact | Results ledger update. |
| `docs/design/0.197-deterministic-optimizer-canonicalization/implementation-results.md` | expected 0.197 closeout artifact | Results ledger update. |
| `testing/integration/tests/pk_canonicalization_focused_artifact.rs` | expected 0.197 implementation/test change | Untracked focused artifact harness. |

Files inspected:

- `docs/design/0.197-deterministic-optimizer-canonicalization/0.197-design.md`
- `docs/design/0.197-deterministic-optimizer-canonicalization/implementation-results.md`
- `docs/design/0.197-deterministic-optimizer-canonicalization/implementation-results.json`
- `docs/design/0.197-deterministic-optimizer-canonicalization/focused-matrix-manifest.md`
- `docs/design/0.197-deterministic-optimizer-canonicalization/focused-matrix-manifest.json`
- `docs/changelog/0.197.md`
- `CHANGELOG.md`
- `docs/contracts/READ_ADMISSION.md`
- `docs/contracts/QUERY_CONTRACT.md`
- `docs/contracts/SQL_SUBSET.md`
- `docs/contracts/CURSOR.md`
- `docs/design/0.196-sqlite-comparison-audit/implementation-results.md`
- `docs/design/0.198/0.198-design.md`
- `docs/governance/fast-path-inventory.md`
- `crates/icydb-core/src/db/query/plan/access_planner.rs`
- `crates/icydb-core/src/db/query/plan/access_choice/model.rs`
- `crates/icydb-core/src/db/query/intent/state.rs`
- `crates/icydb-core/src/db/query/intent/key_access.rs`
- `crates/icydb-core/src/db/access/plan.rs`
- `crates/icydb-core/src/db/access/canonical.rs`
- `crates/icydb-core/src/db/query/plan/pipeline.rs`
- `crates/icydb-core/src/db/query/plan/planner/compare.rs`
- `crates/icydb-core/src/db/query/plan/planner/predicate.rs`
- `crates/icydb-core/src/db/query/plan/planner/prefix.rs`
- `crates/icydb-core/src/db/query/plan/key_item_match.rs`
- `crates/icydb-core/src/db/query/admission.rs`
- `crates/icydb-core/src/db/query/explain/json.rs`
- `crates/icydb-core/src/db/session/tests/read_admission.rs`
- `crates/icydb-core/src/db/session/tests/explain_execution.rs`
- `crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `crates/icydb-core/src/db/query/intent/tests/scalar.rs`
- `crates/icydb-core/src/db/query/intent/tests/cache_key.rs`
- `crates/icydb-core/src/db/query/plan/tests/structural_guards.rs`
- `crates/icydb-core/src/db/sql/lowering/tests/mod.rs`
- `testing/integration/tests/pk_canonicalization_focused_artifact.rs`

Matrix/perf artifacts inspected:

- `docs/design/0.197-deterministic-optimizer-canonicalization/focused-matrix-manifest.json`
- `docs/design/0.197-deterministic-optimizer-canonicalization/focused-matrix-manifest.md`
- `/tmp/icydb-197-focused-before.json`
- `/tmp/icydb-197-focused-after.json`
- `/tmp/icydb-197-focused-delta.json`
- `/tmp/icydb-197-focused-delta.md`

The `/tmp` focused artifacts are synthetic smoke artifacts with `admission_result: synthetic`; they are not real before/after closeout evidence.

What was not reproduced:

- Full workspace test completion was not reproduced because integration tests requiring PocketIC failed due missing `/tmp/pocket-ic-server-14.0.0/pocket-ic`.
- Full deterministic SQL matrix before/after was not reproduced.
- Real focused 0.197 performance capture was not reproduced.
- No no-default-features matrix was run.

Relationship to 0.196 and 0.198:

- 0.196 ordered-read pushdown should not be reopened here.
- 0.197 remains exact primary-key canonicalization and admission, not general optimization.
- 0.198 read-intent ergonomics remains a separate design line; no `page(...)`, `collect_complete()`, `count_exact()`, `admin_batch(...)`, read-policy builder, or generated endpoint read-intent redesign was found in this audit.

## Closeout Gate Summary

| Gate | Status | Evidence | Blocking? |
| --- | --- | --- | --- |
| Scope | Pass | Design/results docs and implementation stayed exact-primary-key focused. | No |
| Canonicalization artifact | Partial | `AccessPlan`, access snapshots, and key-resource summaries carry most facts, but no explicit design enum exists. | Yes |
| Accepted-schema authority | Pass | `SchemaInfo::scalar_primary_key_name`, field lookup, strict type checks, and shared key access normalization. | No |
| Validation/fail-closed | Pass with gaps | Wrong types, invalid residuals, over-budget inputs, expression-wrapped PK, secondary unique, and composite partial paths are tested. SQL params unsupported. | No if SQL params are amended out; Yes if still required |
| ByKey behavior | Pass | Focused tests show filter/by_id parity, admission without fake limit, residual validation, heap/journaled/deleted behavior. | No |
| ByKeys behavior | Pass with documentation gap | Finite primary-key sets are bounded and deterministic; raw/dedup/resource policies tested. | No |
| Empty zero-row behavior | Partial | Semantic empty tests exist; zero-IO proof is not complete for all contradictory exact-key shapes. | Yes |
| SQL/fluent parity | Partial | SQL literal and fluent parity covered; SQL parameter parity intentionally unsupported. | Yes unless design amended |
| Cache/parameter safety | Partial | Structural and literal cache tests pass; same-shape/different-parameter tests are not applicable because SQL parameters are unsupported. | Yes unless design amended |
| Read admission | Pass with caveat | Admission consumes selected access and exact-key resource summaries. | No |
| EXPLAIN/diagnostics | Partial | Selected access and key-resource facts exposed; no explicit canonicalization attempted/applied/disabled/failure contract. | No for runtime, Yes for strict design |
| Public API/compatibility | Pass | No persisted-format, cursor-token, or 0.198 API changes found. Exact-key filters intentionally change admission behavior. | No |
| Hard-cut/cache | Pass with gap | No compatibility shim found; cache structural tests pass. Stale pre-version cache fixture not proven. | No |
| Performance/behavior evidence | Fail | Only synthetic focused artifacts found; no fresh full matrix. | Yes |
| 0.198 boundary | Pass | No read-intent terminal/API implementation found. | No |
| Docs/changelog | Partial | Changelog/results exist; `READ_ADMISSION.md` still implies by-id is the preferred exact-key admission path. | No |
| Feature/CI | Partial | fmt, clippy, and focused tests pass; full test command blocked by missing PocketIC binary. | Yes for release-quality validation |
| Validation commands | Partial | Required commands were run; one broad test failure is environmental. | No if CI covers PocketIC elsewhere |

## Scope Audit

| Out-of-scope area | Evidence | Classification | Blocking? |
| --- | --- | --- | --- |
| Cost-based optimizer | No cost model or stats-driven route ranking found. Exact primary-key access only. | avoided | No |
| Broad exact-cardinality ranking | Finite key-set admission was added only for primary-key access. | avoided | No |
| Secondary unique masquerading as PK | `public_read_fluent_admission_keeps_unique_secondary_equality_off_primary_key_access` covers this. | avoided | No |
| Partial composite-key canonicalization | `public_read_fluent_admission_keeps_partial_composite_key_filter_off_primary_key_access` covers this. | avoided | No |
| Expression/coercive/casefold PK equality | `expression_wrapped_primary_key` and cache-key strict/casefold tests cover non-canonical shapes. | avoided | No |
| Primary-key range optimizer | Existing range planner remains separate. | avoided | No |
| Cursor-token changes | No cursor-token diffs found. | avoided | No |
| Persisted-format changes | No persisted row/index/schema/journal changes found. | avoided | No |
| Generated-model runtime fallback | Planning uses accepted schema info, not generated model reconstruction. | avoided | No |
| 0.198 read-intent API work | No 0.198 terminal or policy APIs found. | avoided | No |
| Executor-only shortcut | Execution consumes selected access from planner/admission path. | avoided | No |
| Finite key-set non-key ordering | Bounded finite key sets can materialize/sort. | safe incidental support | No |

## Canonicalization Artifact Audit

| Check | Evidence | Pass/Fail/Unknown | Blocking? | Notes |
| --- | --- | --- | --- | --- |
| First-class artifact exists | No `PrimaryKeyCanonicalization` enum found. Facts are split across `AccessPlan`, `AccessChoiceExplainSnapshot`, `PrimaryKeyInputResourceSummary`, and admission. | Fail | Yes | Either add the explicit artifact or amend the design to the current selected-access artifact model. |
| Produced at shared semantic layer | `plan_compare`, predicate planning, SQL lowering tests, and fluent tests route through the query planning path. | Pass | No | SQL literals and fluent filters converge. |
| Used by SQL and fluent | SQL literal and fluent exact-key tests pass. SQL parameters are unsupported. | Partial | Yes if params remain required | SQL parameter parity is absent by contract. |
| Read admission consumes it | Admission consumes `selected_access` and `primary_key_input_resource`. | Pass | No | Good proof flow for implemented shapes. |
| EXPLAIN consumes it | Explain output exposes selected access and key-resource facts. | Partial | No | Missing canonicalization attempted/applied/disabled/failure fields. |
| Cache identity accounts for it | `cache_key` and structural guard tests pass for duplicate/permutation/strictness cases. | Pass | No | SQL parameter cache case unavailable. |
| Execution consumes selected access | Planner-selected `AccessPlan` is passed into access execution. | Pass | No | No executor-only rediscovery found. |
| Tied to accepted schema authority | `SchemaInfo` scalar primary key and field metadata drive matching and type checks. | Pass | No | See accepted-schema audit. |
| Carries residual predicate info | Residual filters remain in compiled predicates when exact key proof does not remove the whole predicate. | Pass | No | Invalid residual tests pass. |
| Distinguishes valid non-application from validation failure | Tests cover noncanonical shapes and invalid exact-key inputs separately. | Partial | No | Diagnostics do not expose the design's `None { reason }` versus validation-failure split as a single artifact. |
| Represents `Empty` as zero-row result | Empty semantic tests exist. | Partial | Yes | Zero-IO route proof is incomplete. |

## Accepted-Schema Authority Audit

| Authority invariant | Evidence | Test file/name | Pass/Fail/Unknown | Blocking? |
| --- | --- | --- | --- | --- |
| Primary-key field identity comes from accepted schema snapshot | `plan_compare` uses `SchemaInfo::scalar_primary_key_name()` and `schema.field(name)`. | Structural inspection | Pass | No |
| External primary keys use same logical key construction as explicit `by_id` | `filter(pk = value)` and `by_id(Id::from_key(value))` path parity is tested. | `external_primary_key_filter_and_by_id` | Pass | No |
| Generated/default primary keys use same key encoder as explicit `by_id` | Read-admission and focused scenarios cover generated scalar PK. | `public_read_fluent_admission_admits_primary_key_filter_without_limit`; manifest scenarios | Pass | No |
| Secondary unique field equality does not report `ByKey` | Dedicated admission test. | `public_read_fluent_admission_keeps_unique_secondary_equality_off_primary_key_access` | Pass | No |
| Frontend aliases/Rust names do not override accepted schema | Planning uses accepted schema field metadata. | Structural inspection | Pass | No |
| Schema generation invalidates stale canonicalization/cache facts | Cache-key/version tests exist. | `cache_key` test filter; `structural_guards` tests | Pass | No |
| Stale schema/index metadata cannot produce key proof | Accepted planning state is schema-info driven. | Structural inspection | Pass | No |

## Validation And Fail-Closed Audit

| Case | Expected | Evidence | Test | Pass/Fail/Unknown | Blocking? |
| --- | --- | --- | --- | --- | --- |
| Wrong primary-key value type | Validation/admission failure, no scan fallback. | SQL wrong-type tests and literal type checks. | `public_read_sql_primary_key_wrong_type_literal_fails_closed` | Pass | No |
| Malformed `IN` list | Validation/admission failure. | SQL IN over-budget/malformed coverage exists for supported forms. | `read_admission` filter | Pass with gap | No |
| Missing SQL parameter | Failure before admission. | SQL parameters rejected by contract. | `public_read_sql_primary_key_parameter_shape_fails_before_admission` | Pass for current contract | Yes if design still requires params |
| Over-budget raw IN input count | Failure before scan. | Primary-key resource summary and admission policy tests. | `read_admission` filter | Pass | No |
| Over-budget deduplicated key count | Failure before scan. | Admission policy tests. | `read_admission` filter | Pass | No |
| Invalid residual predicate | Validation failure before returning not-found. | Existing/missing invalid residual tests. | `public_read_fluent_primary_key_filter_invalid_residual_*` | Pass | No |
| Unknown residual field | Validation failure. | Invalid residual tests. | `read_admission` filter | Pass | No |
| Expression-wrapped PK | Does not canonicalize. | Focused test. | `expression_wrapped_primary_key` | Pass | No |
| Partial composite PK | Does not canonicalize. | Dedicated test. | `public_read_fluent_admission_keeps_partial_composite_key_filter_off_primary_key_access` | Pass | No |
| Coercive/casefold equality | Does not share strict canonical cache identity. | Cache-key strict/casefold tests. | `cache_key` filter | Pass | No |
| Malformed cursor combined with exact key | No 0.197 cursor-token change; not deeply tested in this line. | Cursor docs and no cursor diffs. | Unknown | Unknown | No |
| `pk = missing AND unknown_field = x` | Must fail validation, not return not-found. | Invalid residual missing-key test. | `public_read_fluent_primary_key_filter_invalid_residual_missing_key_still_fails_validation` | Pass | No |

## ByKey Audit

| Scenario | Expected selected access | Expected admission | Result parity | IO/counter bound | Pass/Fail/Unknown |
| --- | --- | --- | --- | --- | --- |
| Fluent existing key | `ByKey` | admitted without fake limit | equals `by_id` | at most one logical key fetch | Pass |
| Fluent missing key | `ByKey` | admitted without fake limit | same zero-row/not-found semantics | at most one logical key fetch | Pass |
| External PK | `ByKey` | admitted | equals `by_id(Id::from_key(value))` | bounded | Pass |
| Generated/default PK | `ByKey` | admitted | equals explicit `by_id` | bounded | Pass |
| Residual true | `ByKey` plus residual | admitted | row returned if residual true | bounded | Pass |
| Residual false | `ByKey` plus residual | admitted | no row if residual false | bounded | Pass |
| Invalid residual | validation failure | fail closed | no not-found masking | no scan | Pass |
| SQL literal existing/missing | `ByKey` | admitted | matches key lookup semantics | bounded | Pass |
| SQL commuted literal | `ByKey` | admitted | matches key lookup semantics | bounded | Pass |
| SQL parameter | currently unsupported | fails before admission | no scan | no scan | Partial |
| Public read no explicit limit | `ByKey` proof | admitted | exact-key result | bounded | Pass |
| Heap/journaled/deleted row | `ByKey` | admitted | deleted/tombstoned not returned | bounded | Pass |

## ByKeys Audit

| Scenario | Raw count | Deduped count | Selected access | Result order | Expected | Pass/Fail/Unknown |
| --- | ---: | ---: | --- | --- | --- | --- |
| `pk IN ()` | 0 | 0 | Empty or empty `ByKeys` | deterministic empty | empty result, admitted as zero-row proof | Pass with zero-IO gap |
| `pk IN (a)` | 1 | 1 | `ByKey` or singleton key-set | deterministic | one-key access | Pass |
| `pk IN (a, a, a)` | 3 | 1 | `ByKey`/`ByKeys` canonicalized | deterministic | duplicates collapse | Pass |
| `pk IN (b, a, b)` versus `pk IN (a, b)` | 3 / 2 | 2 | `ByKeys` | canonical deterministic order | no hash-order leakage | Pass |
| Mixed existing/missing keys | finite | finite | `ByKeys` | deterministic | missing keys absent | Pass |
| Residual true/false | finite | finite | `ByKeys` with residual | deterministic | residual can only reduce rows | Pass |
| Over-budget raw input | over cap | maybe under cap | failure | none | no scan fallback | Pass |
| Over-budget deduped input | over cap | over cap | failure | none | no scan fallback | Pass |
| Wrong-type element | malformed | malformed | failure | none | no scan fallback | Pass with SQL-specific gaps |
| Heap and journaled stores | finite | finite | `ByKeys` | deterministic | equivalent semantics | Pass |

## Empty Audit

| Empty scenario | Terminal | Expected result | data_store.get | index ranges | Pass/Fail/Unknown | Blocking? |
| --- | --- | --- | ---: | ---: | --- | --- |
| `pk = a AND pk = b`, `a != b` | optional/collection/count | empty/0/not-found | expected 0 | expected 0 | Unknown | Yes |
| `pk = a AND pk = a` | optional/collection | same as `ByKey(a)` | <= 1 | 0 | Pass | No |
| `pk = a AND pk IN (a, b, c)` | optional/collection | same as `ByKey(a)` | <= 1 | 0 | Pass | No |
| `pk = a AND pk IN (b, c)` | optional/collection/count | empty/0/not-found | expected 0 | expected 0 | Unknown | Yes |
| `pk IN ()` | optional/collection/count | empty/0/not-found | expected 0 | expected 0 | Unknown | Yes |
| required one-row lookup | required terminal | same not-found/cardinality helper | expected 0 for contradiction | expected 0 | Semantic pass, IO unknown | Yes |
| SQL and fluent forms | supported forms | equivalent | expected zero for contradiction | expected zero | Partial | Yes |

The current implementation appears to produce correct empty results, but `primary_key_child_access_candidate` can choose a singleton key child and leave the contradictory set as residual work. That is correct semantically but does not prove the design's zero-IO `Empty` route for every contradictory exact-key shape.

## SQL/Fluent Parity Audit

| Logical query | Fluent result | SQL literal result | SQL parameter result | Explicit key result | Pass/Fail/Unknown |
| --- | --- | --- | --- | --- | --- |
| `pk = existing` | `ByKey`, admitted | `ByKey`, admitted | unsupported by contract | `by_id` | Partial |
| `pk = missing` | `ByKey`, admitted | covered by SQL literal behavior | unsupported by contract | `by_id` | Partial |
| `pk IN (...)` | `ByKeys`/Empty, admitted if bounded | SQL IN tested for supported forms | parameter list unsupported/unknown | repeated key lookup model | Partial |
| residual true/false | evaluated after key access | SQL residual tested | unsupported by params | explicit lookup plus residual model | Pass for non-param forms |
| wrong type | validation/admission failure | validation/admission failure | unsupported/failure | explicit key typing prevents mismatch | Pass |
| missing parameter | not applicable | not applicable | failure before admission | not applicable | Pass for current contract |

Mismatches:

- SQL parameters are a design-required 0.197 proof area, but current SQL lowering rejects parameter placeholders. This is either an implementation gap or a design-contract mismatch that must be amended before closeout.

## Cache And Parameter Audit

| Cache invariant | Evidence | Test | Pass/Fail/Unknown | Blocking? |
| --- | --- | --- | --- | --- |
| Parameterized SQL caches shape and slot, not concrete key | SQL parameters unsupported. | `public_read_sql_primary_key_parameter_shape_fails_before_admission` | Unknown/not applicable | Yes if design still requires params |
| Same cached shape with key A then key B returns B | SQL parameters unsupported. | None found | Unknown | Yes if design still requires params |
| Wrong-type parameter fails and does not scan | SQL parameters unsupported. | Parameter-shape failure | Pass for current contract | No |
| Missing parameter fails and does not scan | SQL parameters unsupported. | Parameter-shape failure | Pass for current contract | No |
| Literal SQL value cache does not pollute parameterized cache | Literal cache test exists; parameterized cache not applicable. | `primary_key_literal_sql_cache_identity_keeps_concrete_key_values_distinct` | Partial | No |
| Stale generic/rejected plans are not reused | Structural cache-key tests and version tests pass. | `cache_key` filter | Pass | No |
| Cache identity includes schema identity/generation where necessary | Structural guard/cache tests cover schema-sensitive identity. | `cache_key` and `structural_guards` filters | Pass | No |
| Cache does not embed runtime liveness/generation facts | No liveness in cache-key evidence found. | Structural inspection | Pass | No |

## Read Admission Audit

| Admission shape | Expected | Evidence | Pass/Fail/Unknown | Blocking? |
| --- | --- | --- | --- | --- |
| `ByKey` | returned-row bound 1, no fake limit, no scan rejection | `public_read_fluent_admission_admits_primary_key_filter_without_limit`; SQL literal tests | Pass | No |
| `ByKeys` | returned-row bound is deduped count; raw and dedup budgets enforced | `read_admission` tests and `PrimaryKeyInputResourceSummary` | Pass | No |
| Empty | bounded zero-row proof unless contradiction involved invalid predicate | Semantic empty tests | Partial | Yes |
| Residual predicates | validate before return; evaluate before row result | invalid residual existing/missing tests | Pass | No |
| Runtime fallback | executor/admission use same selected access artifact | structural inspection | Pass | No |

## EXPLAIN And Diagnostics Audit

| Diagnostic field/behavior | Evidence | Pass/Fail/Unknown | Public API impact | Blocking? |
| --- | --- | --- | --- | --- |
| `canonicalization_attempted` | Not found as explicit field. | Fail | Diagnostic gap | No |
| `canonicalization_applied` | Implied by selected access. | Partial | Diagnostic gap | No |
| `canonicalization_rule` | Implied by `AccessChoiceSelectedReason`. | Partial | Diagnostic gap | No |
| disabled reason for valid non-application | Reasons exist in access snapshots, but not the design enum. | Partial | Diagnostic gap | No |
| validation/admission failure reason | Existing error/admission diagnostics used. | Pass | Public diagnostics | No |
| primary key field and accepted schema source | Planning uses accepted schema; explain facts include selected access/resource. | Partial | Diagnostic gap | No |
| selected access `ByKey`/`ByKeys`/Empty | Selected access exposed. | Pass | Public/diagnostic DTO | No |
| raw/dedup key counts | Primary-key input resource summary exposed to admission/explain. | Pass | Diagnostic DTO | No |
| residual present | Residual behavior tested; explicit explain residual field coverage unclear. | Unknown | Diagnostic gap | No |
| bounded/redacted raw values | No unbounded debug value emission found. | Pass | Diagnostic safety | No |
| avoid `.limit(1000)` workaround for exact-key shapes | Docs partially stale. | Partial | Docs/API | No |

## API / Compatibility Audit

| Surface | Changed? | Breaking? | Versioned? | Documented? | Tests? | Blocking? |
| --- | --- | --- | --- | --- | --- | --- |
| Public query builder APIs | No new API found. | No | N/A | N/A | Existing tests | No |
| SQL facade APIs | No new API; behavior changed for supported exact-key literals. | No | Pre-1.0 behavior | Partial | SQL tests | No |
| EXPLAIN DTOs | Selected-access diagnostics expanded/used. | No known breaking change | Diagnostic/pre-1.0 | Partial | Explain tests | No |
| Read-admission DTOs/errors | Exact-key filters now admitted. | Intentional behavior change | Pre-1.0 | Partial | Read-admission tests | No |
| SQL result DTOs | No change found. | No | N/A | N/A | Existing tests | No |
| Error codes | Exact-key missing-limit style errors disappear for admitted shapes. | Intentional | Pre-1.0 | Partial | Admission tests | No |
| Generated macro API | No generated runtime-authority change found. | No | N/A | N/A | Existing tests | No |
| Candid/DTO surfaces | No persisted/cursor change found. | No | N/A | N/A | Not deeply audited | No |
| Feature-gated APIs | No 0.198 APIs found. | No | N/A | N/A | Not fully matrixed | No |

## Hard-Cut Audit

| Hard-cut check | Required? | Evidence | Pass/Fail/Unknown | Notes |
| --- | --- | --- | --- | --- |
| No persisted-format change | Yes | No row/index/schema/journal format diffs found. | Pass | Required. |
| No cursor-token format change | Yes | No cursor-token diffs found. | Pass | Required. |
| No legacy compatibility shim | Yes | No compatibility flag preserving missing-limit behavior found. | Pass | Exact-key filters intentionally use new behavior. |
| No compatibility flag for old exact-key missing-limit behavior | Yes | No such flag found. | Pass | Good pre-1.0 hard-cut posture. |
| Cache/method version bumped if needed | Required if cache identity changed | Structural cache tests pass; explicit version evidence not fully audited. | Partial | Add fixture if cache serialization is persisted externally. |
| Old cached generic/rejected plans miss | Required if method/cache changed | Cache-key tests cover structural shape behavior. | Partial | No old fixture found. |
| No dual-read compatibility path | Yes | No dual-read path found. | Pass | Good. |
| Changelog documents behavior changes | Yes | `docs/changelog/0.197.md` and `CHANGELOG.md` have uncommitted notes. | Partial | Need final closeout wording after blockers fixed. |

## Correctness Invariant Audit

| Invariant | Evidence | Test file/test name | Pass/Fail/Unknown | Blocking? |
| --- | --- | --- | --- | --- |
| `filter(pk = value)` equals `by_id(value)` | Route parity and result tests. | `external_primary_key_filter_and_by_id`; read-admission tests | Pass | No |
| `WHERE pk = ?` equals `by_id(value)` after binding | SQL parameters unsupported. | Parameter-shape rejection test | Unknown/not applicable | Yes if design still requires params |
| finite `pk IN (...)` equals deterministic repeated key lookup | ByKeys canonicalization and deterministic order tests. | `read_admission`; `cache_key` | Pass | No |
| Empty equals zero-row proof with zero IO | Semantic tests only; zero-IO missing. | Empty read-admission tests | Partial | Yes |
| Invalid residual is not hidden by missing key | Dedicated test. | `public_read_fluent_primary_key_filter_invalid_residual_missing_key_still_fails_validation` | Pass | No |
| Residual filters are evaluated for existing rows | Dedicated tests. | `read_admission` residual tests | Pass | No |
| Secondary unique equality does not become PK proof | Dedicated test. | `public_read_fluent_admission_keeps_unique_secondary_equality_off_primary_key_access` | Pass | No |
| Composite partial PK does not become scalar `ByKey` | Dedicated test. | `public_read_fluent_admission_keeps_partial_composite_key_filter_off_primary_key_access` | Pass | No |
| Coercive/casefold/expression equality does not canonicalize | Strict/coercion and expression tests. | `cache_key`; `expression_wrapped_primary_key` | Pass | No |
| Admission cannot be bypassed by runtime fallback | Shared selected access/admission flow. | Structural inspection | Pass | No |
| SQL and fluent equivalent queries behave equivalently | SQL literal and fluent forms tested. | `read_admission`; `sql_surface` | Partial | SQL params absent |
| Heap and journaled stores preserve semantics | Store state tests. | `read_admission` store tests | Pass | No |
| Tombstoned/deleted rows are not returned | Store state tests. | `read_admission` deleted tests | Pass | No |
| Cache does not embed concrete parameter values unless part of identity | Literal key cache test passes; parameters unsupported. | `primary_key_literal_sql_cache_identity_keeps_concrete_key_values_distinct` | Partial | No |
| Cache does not embed runtime liveness/generation facts | No liveness facts in cache key found. | Structural inspection | Pass | No |
| Diagnostics do not change behavior | Explain/read tests separate route facts from execution. | Explain tests | Pass | No |

## Performance And Behavior Evidence Audit

| Metric | Before | After | Delta | Notes |
| --- | ---: | ---: | ---: | --- |
| focused scenarios | null | null | null | No real focused capture committed. |
| focused common successes | null | null | null | Synthetic `/tmp` artifacts are not evidence. |
| newly admitted exact-key shapes | unknown | unknown | unknown | Behavior tests prove admission, but no before/after artifact. |
| new failures | unknown | unknown | unknown | Full matrix not reproduced. |
| non-key selected-access changes | unknown | unknown | unknown | Full matrix not reproduced. |
| aggregate focused total instructions | null | null | null | Not measured. |
| aggregate focused execute instructions | null | null | null | Not measured. |
| aggregate focused `data_store.get` | null | null | null | Not measured. |
| aggregate focused index ranges | null | null | null | Not measured. |
| focused `ByKey` count | synthetic only | synthetic only | none | Synthetic manifest smoke, not closeout evidence. |
| focused `ByKeys` count | synthetic only | synthetic only | none | Synthetic manifest smoke, not closeout evidence. |
| focused `Empty` count | synthetic only | synthetic only | none | Synthetic manifest smoke, not closeout evidence. |
| validation failures | synthetic only | synthetic only | none | Synthetic manifest smoke, not closeout evidence. |

Focused artifacts found in `/tmp`:

- `/tmp/icydb-197-focused-before.json`
- `/tmp/icydb-197-focused-after.json`
- `/tmp/icydb-197-focused-delta.json`
- `/tmp/icydb-197-focused-delta.md`

These are generated from saved before/after artifacts but contain synthetic values such as `after_admission_result: synthetic`, constant instruction counts, and unchanged access facts. They are useful for checking the delta renderer, not for measuring 0.197.

Performance closeout status: Fail.

Behavior evidence status: Pass for implemented non-parameter exact-key shapes; partial for `Empty` zero-IO and SQL parameters.

## 0.198 Boundary Audit

| 0.198-shaped change | File | Behavior | Risk | Keep/defer/remove? | Blocking? |
| --- | --- | --- | --- | --- | --- |
| General public page terminal | Not found | No API implementation found. | Low | Defer to 0.198 | No |
| Complete-small-set terminal | Not found | No API implementation found. | Low | Defer to 0.198 | No |
| Exact aggregate terminal | Not found | No API implementation found. | Low | Defer to 0.198 | No |
| Admin batch terminal | Not found | No API implementation found. | Low | Defer to 0.198 | No |
| Named read-policy API | Not found | No API implementation found. | Low | Defer to 0.198 | No |
| User-configurable caps | Not found | No API implementation found. | Low | Defer to 0.198 | No |
| Generated endpoint read-intent redesign | Not found | No generated API redesign found. | Low | Defer to 0.198 | No |

## Documentation Audit

| Doc | Current status | Issue | Blocking? | Required edit |
| --- | --- | --- | --- | --- |
| `0.197-design.md` | Status says implementation in progress and points to results. | Acceptable during partial closeout, but not final. | No | Update status after blockers are fixed or explicitly re-scoped. |
| `implementation-results.md` | Accurately reports partial closeout and missing performance artifacts. | Good. | No | Keep as authority until final closeout. |
| `implementation-results.json` | Mirrors partial status. | Good. | No | Update after real artifacts. |
| `CHANGELOG.md` | Has uncommitted 0.197 notes. | Needs final wording after closeout blockers. | No | Do not overclaim performance. |
| `docs/changelog/0.197.md` | Has detailed notes. | Needs final wording after blockers. | No | Keep behavior-focused. |
| `READ_ADMISSION.md` | Still says exact primary-key reads should prefer key API. | Stale after 0.197 because strict PK filters can be admitted. | No | Document exact-key filters as accepted and by-id as explicit alternative. |
| `QUERY_CONTRACT.md` | Aligns with normal predicate semantics and planner optimization. | Good. | No | Optional link to 0.197 results. |
| `SQL_SUBSET.md` | Does not fully explain 0.197 exact-key literal behavior or parameter non-support. | Docs gap. | No | Add exact-key literal behavior and parameter limitation. |
| `CURSOR.md` | No 0.197 cursor change. | Good. | No | No required edit. |
| `fast-path-inventory.md` | Does not fully inventory 0.197 canonicalization/proof owner. | Governance docs gap. | No | Add exact-PK canonicalization row before 1.0. |
| `0.198-design.md` | Located at `docs/design/0.198/0.198-design.md`; boundary remains separate. | Good. | No | No required edit. |

## Feature And CI Audit

| Command / area | Result | Notes | Blocking? |
| --- | --- | --- | --- |
| `cargo fmt --check` | Pass | Required formatting check passed. | No |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Pass | Required clippy check passed. | No |
| `cargo test --workspace --all-features` | Fail, environmental | Integration tests requiring PocketIC failed because `/tmp/pocket-ic-server-14.0.0/pocket-ic` was missing. | No if CI covers it; Yes for local release closeout |
| focused read-admission tests | Pass | `82 passed`. | No |
| focused cache-key tests | Pass | `39 passed`. | No |
| focused exact-key/parity tests | Pass | Several focused tests passed. | No |
| `cargo test --workspace --no-default-features` | Not run | Too broad for this closeout pass. | No |
| SQL feature-only matrix | Not run | Covered through all-features focused tests, but not separate feature matrix. | No |
| Full deterministic SQL matrix | Not run | Missing closeout performance evidence. | Yes for performance closeout |

## Validation

Commands run:

| Command | Result | Notes |
| --- | --- | --- |
| `git status --short` | Pass | Captured dirty worktree. |
| `git rev-parse HEAD` | Pass | `b7309b5ec0f16cb8bbe6a917ca754bc98316ea90`. |
| `git diff --stat` | Pass | 6 tracked docs/artifact files changed; untracked test not included. |
| `git diff -- docs/design/0.197-deterministic-optimizer-canonicalization docs/changelog/0.197.md CHANGELOG.md docs/contracts/READ_ADMISSION.md docs/contracts/QUERY_CONTRACT.md docs/contracts/SQL_SUBSET.md docs/contracts/CURSOR.md` | Pass | No contract diffs; 0.197 docs/changelog/artifact diffs only. |
| `cargo fmt --check` | Pass | Formatting clean. |
| `cargo test --workspace --all-features` | Fail | Environmental PocketIC binary missing for integration tests. |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Pass | Clippy clean. |
| `cargo test -p icydb-core --all-features read_admission` | Pass | `82 passed`. |
| `cargo test -p icydb-testing-integration --test pk_canonicalization_focused_artifact` | Pass | `1 passed; 2 ignored`. Ignored tests need real saved artifacts. |
| `cargo test -p icydb-core --all-features session_explain_execution_external_primary_key_filter_and_by_id_use_same_access_path session_explain_execution_primary_key_filter_canonicalization_route_facts_are_stable` | Fail | Command syntax error: multiple cargo test filters supplied as positional args. |
| `cargo test -p icydb-core --all-features session_explain_execution_primary_key` | Pass | Focused explain tests passed. |
| `cargo test -p icydb-core --all-features primary_key_literal_sql_cache_identity_keeps_concrete_key_values_distinct structural_query_cache_key_treats_equivalent_in_list_permutations_as_identical structural_query_cache_key_treats_duplicate_in_list_literals_as_identical structural_query_cache_key_distinguishes_strict_from_text_casefold_coercion` | Fail | Command syntax error: multiple cargo test filters supplied as positional args. |
| `cargo test -p icydb-core --all-features cache_key` | Pass | `39 passed`. |
| `cargo test -p icydb-core --all-features primary_key_literal_sql_cache_identity` | Pass | Literal cache identity test passed. |
| `cargo test -p icydb-core --all-features expression_wrapped_primary_key` | Pass | Expression-wrapped exact-key shape did not canonicalize. |
| `cargo test -p icydb-core --all-features external_primary_key_filter_and_by_id` | Pass | Original motivating route parity passed. |
| `find docs/design/0.197-deterministic-optimizer-canonicalization -maxdepth 1 -type f -printf '%f\n' \| sort` | Pass | No committed before/after/delta performance artifacts present. |
| `find /tmp -maxdepth 1 -type f -name 'icydb-197-focused-*' -printf '%f\n' \| sort` | Pass | Synthetic local focused artifacts found. |
| `ls -l /tmp/icydb-197-focused-before.json /tmp/icydb-197-focused-after.json /tmp/icydb-197-focused-delta.json /tmp/icydb-197-focused-delta.md` | Pass | Local synthetic artifact sizes captured. |
| `sed -n '1,80p' /tmp/icydb-197-focused-delta.md` | Pass | Confirmed synthetic focused summary. |
| `head -40 /tmp/icydb-197-focused-delta.json` | Pass | Confirmed `admission_result: synthetic`. |
| `rg "0.197\|ByKey\|ByKeys\|primary-key\|exact-key\|limit\|read intent\|cursor\|persisted-format\|parameter" -n docs/contracts/QUERY_CONTRACT.md docs/contracts/SQL_SUBSET.md docs/contracts/CURSOR.md docs/governance/fast-path-inventory.md docs/design/0.198-read-intent-ergonomics/0.198-design.md docs/audits/technical-debt/icydb-technical-debt-audit-2026-07-04.md` | Fail | Optional paths did not exist; existing-file matches were still printed before exit. |
| `find docs/design -maxdepth 2 -type f \| sort \| rg "0\.198\|0\.197\|0\.196\|read-intent\|technical-debt\|debt"` | Pass | Found actual 0.198 design path and 0.197/0.196 docs. |
| `find docs/audits -maxdepth 3 -type f \| sort \| rg "technical\|debt\|0\.197\|0\.196\|closeout"` | Pass | Found 0.196 closeout artifacts; no technical-debt file at expected path. |
| `mkdir -p docs/audits/0.197-closeout` | Pass | Created audit output directory. |
| `jq . docs/audits/0.197-closeout/icydb-0.197-closeout-audit-2026-07-06.json` | Pass | JSON parses. |
| `git diff --check` | Pass | No whitespace errors. |
| `git status --short` | Pass | Final status captured after report creation. |

Representative inspection commands also run:

- `sed -n '1,260p' docs/design/0.197-deterministic-optimizer-canonicalization/0.197-design.md`
- `sed -n '320,640p' docs/design/0.197-deterministic-optimizer-canonicalization/0.197-design.md`
- `sed -n '640,980p' docs/design/0.197-deterministic-optimizer-canonicalization/0.197-design.md`
- `sed -n '980,1120p' docs/design/0.197-deterministic-optimizer-canonicalization/0.197-design.md`
- `sed -n '1,240p' docs/design/0.197-deterministic-optimizer-canonicalization/implementation-results.md`
- `sed -n '1,240p' docs/design/0.197-deterministic-optimizer-canonicalization/implementation-results.json`
- `rg "PrimaryKeyCanonical|primary_key.*canonical|ByKey|ByKeys|Empty|PrimaryKeyInput|AccessChoice|SelectedAccess|selected_access|key_access|canonicalization" -n crates/icydb-core/src testing/integration/tests docs/contracts docs/governance/fast-path-inventory.md`
- `sed -n '1,260p' crates/icydb-core/src/db/query/plan/access_planner.rs`
- `sed -n '1,340p' crates/icydb-core/src/db/query/plan/access_choice/model.rs`
- `sed -n '1,320p' crates/icydb-core/src/db/query/intent/state.rs`
- `sed -n '560,620p' crates/icydb-core/src/db/query/intent/state.rs`
- `sed -n '1,180p' crates/icydb-core/src/db/query/intent/key_access.rs`
- `sed -n '140,260p' crates/icydb-core/src/db/access/plan.rs`
- `sed -n '460,780p' crates/icydb-core/src/db/query/plan/pipeline.rs`
- `sed -n '1,180p' crates/icydb-core/src/db/query/plan/tests/structural_guards.rs`
- `sed -n '330,390p' crates/icydb-core/src/db/query/intent/tests/scalar.rs`
- `sed -n '360,420p' crates/icydb-core/src/db/session/tests/explain_execution.rs`
- `sed -n '780,1390p' crates/icydb-core/src/db/session/tests/read_admission.rs`
- `sed -n '9780,9870p' crates/icydb-core/src/db/session/tests/sql_surface.rs`
- `sed -n '860,940p' crates/icydb-core/src/db/sql/lowering/tests/mod.rs`
- `sed -n '520,820p' crates/icydb-core/src/db/query/intent/tests/cache_key.rs`
- `sed -n '1,340p' crates/icydb-core/src/db/query/plan/planner/predicate.rs`
- `sed -n '1,260p' crates/icydb-core/src/db/query/plan/planner/prefix.rs`
- `sed -n '1,240p' crates/icydb-core/src/db/access/canonical.rs`
- `sed -n '1,440p' crates/icydb-core/src/db/query/plan/planner/compare.rs`
- `sed -n '1,220p' crates/icydb-core/src/db/query/plan/key_item_match.rs`
- `sed -n '1,160p' crates/icydb-core/src/db/query/plan/mod.rs`
- `sed -n '180,410p' docs/contracts/READ_ADMISSION.md`
- `sed -n '1,120p' docs/contracts/SQL_SUBSET.md`
- `sed -n '60,110p' docs/contracts/QUERY_CONTRACT.md`
- `sed -n '1,120p' docs/governance/fast-path-inventory.md`
- `sed -n '1,220p' docs/design/0.198/0.198-design.md`
- `sed -n '1,180p' docs/contracts/CURSOR.md`
- `sed -n '1,120p' docs/design/0.196-sqlite-comparison-audit/implementation-results.md`

## Findings

### 197-CO-001: Real focused and full-matrix performance artifacts are missing

Category:

- performance / artifacts

Severity:

- High

Blocking:

- Yes

Confidence:

- High

Evidence:

- `docs/design/0.197-deterministic-optimizer-canonicalization/implementation-results.md` says focused before/after/delta artifacts and fresh full matrix are still missing.
- `find docs/design/0.197-deterministic-optimizer-canonicalization -maxdepth 1 -type f -printf '%f\n' | sort` found no committed before/after/delta capture files.
- `/tmp/icydb-197-focused-delta.json` contains synthetic measurements.

What passed:

- A focused artifact manifest and renderer smoke path exists.

What failed or is missing:

- No real before/after/delta JSON/Markdown artifacts.
- No fresh full deterministic SQL matrix.

Recommended fix:

- Run the real focused capture before/after workflow and generate committed or linked delta artifacts.
- Run the full deterministic SQL matrix before/after if any performance or broad behavior claim is made.

Acceptance criteria:

- `sql_perf_197_pk_canonicalization_before.json`
- `sql_perf_197_pk_canonicalization_after.json`
- `sql_perf_197_pk_canonicalization_delta.json`
- `sql_perf_197_pk_canonicalization_delta.md`
- Full matrix before/after/delta artifacts, or explicit correctness-only closeout with no performance claim.

Suggested patch prompt:

- "Create and run the real 0.197 focused primary-key canonicalization capture, produce before/after/delta JSON and Markdown artifacts, and update implementation-results without claiming full-matrix performance unless the full deterministic SQL matrix is also captured."

### 197-CO-002: Canonicalization proof is implicit rather than the explicit design artifact

Category:

- canonicalization / admission / EXPLAIN

Severity:

- High

Blocking:

- Yes for strict design closeout

Confidence:

- High

Evidence:

- No `PrimaryKeyCanonicalization` enum or direct equivalent was found.
- Proof facts are spread across `AccessPlan`, `AccessChoiceExplainSnapshot`, `PrimaryKeyInputResourceSummary`, access canonicalization, and admission.

What passed:

- The implemented selected-access path is coherent for the supported exact-key shapes.

What failed or is missing:

- The design-required `None`, `ByKey`, `ByKeys`, `Empty` canonicalization artifact is not present as a single planner-owned result.
- Diagnostics do not expose canonicalization attempted/applied/disabled/failure as a single stable contract.

Recommended fix:

- Either add a first-class artifact and thread it through admission/explain/cache/execution, or amend the design to declare `AccessPlan` plus snapshots as the canonical artifact and add the missing disabled/failure diagnostics.

Acceptance criteria:

- One planner-owned artifact or documented equivalent owns exact-key proof.
- Admission, EXPLAIN, cache, and execution all consume the same artifact.
- Tests prove invalid cases do not become `None { reason }` scan fallbacks.

Suggested patch prompt:

- "Tighten 0.197 proof ownership by introducing or documenting a single planner-owned exact-primary-key canonicalization artifact and updating admission/explain/cache tests to prove all consumers use it."

### 197-CO-003: Empty zero-IO proof is incomplete

Category:

- correctness / performance / tests

Severity:

- High

Blocking:

- Yes

Confidence:

- Medium

Evidence:

- Empty semantic tests exist in `crates/icydb-core/src/db/session/tests/read_admission.rs`.
- `primary_key_child_access_candidate` can select a singleton key child while contradictory `IN` work remains as residual, which can preserve correct results without proving zero data-store gets.

What passed:

- Contradictory exact-key predicates return empty results in tested terminals.

What failed or is missing:

- No test proves zero `data_store.get`, zero index ranges, and zero row decodes for all contradictory exact-key shapes, especially `pk = a AND pk IN (b)`.

Recommended fix:

- Add explicit `Empty` access selection for contradictory exact-key intersections, or document that semantic empty is the 0.197 scope and remove zero-IO from closeout criteria.

Acceptance criteria:

- `pk = a AND pk = b`, `pk = a AND pk IN (b)`, and `pk IN ()` report Empty/zero-row proof.
- Tests assert zero data-store gets, zero index ranges, zero row decodes, and correct terminal behavior.

Suggested patch prompt:

- "Add zero-IO Empty proof tests for contradictory exact-primary-key predicates and adjust planning so excluded exact-key intersections select Empty rather than fetching a key then filtering it out."

### 197-CO-004: SQL parameter exact-key canonicalization is not implemented

Category:

- SQL-fluent parity / cache / validation

Severity:

- Medium

Blocking:

- Yes unless the 0.197 design is amended

Confidence:

- High

Evidence:

- `public_read_sql_primary_key_parameter_shape_fails_before_admission` expects parameter placeholders to fail before admission.
- `implementation-results.md` classifies SQL parameters as unsupported by the current SQL parameter contract.
- The 0.197 design still lists `WHERE pk = ?` parity and cache tests as required proof cases.

What passed:

- Unsupported SQL parameter shapes fail closed and do not scan.

What failed or is missing:

- Same-shape/different-parameter cache reuse and parameter binding parity are not implemented.

Recommended fix:

- Either implement SQL parameter exact-key canonicalization with cache tests, or amend 0.197 design/results to mark SQL parameters as a future SQL-parameterization line.

Acceptance criteria:

- If implemented: `WHERE pk = ?` with key A then key B returns B through the same cached shape; wrong/missing parameter fails closed.
- If deferred: design and closeout explicitly remove SQL parameter proof cases from 0.197.

Suggested patch prompt:

- "Resolve the 0.197 SQL parameter contract mismatch by either implementing exact-primary-key parameter binding/cache proof or amending the 0.197 design and results to defer SQL parameter canonicalization."

### 197-CO-005: Read-admission docs still imply exact-key filters should use key APIs

Category:

- docs / API

Severity:

- Low

Blocking:

- No

Confidence:

- High

Evidence:

- `docs/contracts/READ_ADMISSION.md` still says exact primary-key reads should prefer the key API so admission can consume `ByKey`.

What passed:

- `QUERY_CONTRACT.md` is closer to the new behavior and describes primary keys as normal predicates with planner optimization.

What failed or is missing:

- Read-admission docs do not clearly state that strict exact primary-key filters are admitted without fake limits.

Recommended fix:

- Update `READ_ADMISSION.md` and `SQL_SUBSET.md` to describe exact-key filter admission, finite primary-key sets, unsupported SQL parameters, and fail-closed invalid inputs.

Acceptance criteria:

- Docs no longer recommend `by_id` as required ceremony for exact-key filters.
- Docs still recommend `by_id` as an explicit API when available.

Suggested patch prompt:

- "Update read-admission and SQL subset docs for 0.197 exact-primary-key canonicalization: strict PK filters and finite PK IN are bounded, invalid inputs fail closed, SQL parameters remain unsupported if not implemented."

### 197-CO-006: Broad workspace tests did not complete locally

Category:

- tests / CI

Severity:

- Medium

Blocking:

- No if CI has PocketIC configured; Yes for local release closeout

Confidence:

- High

Evidence:

- `cargo test --workspace --all-features` failed in PocketIC-dependent integration tests because `/tmp/pocket-ic-server-14.0.0/pocket-ic` was missing.

What passed:

- Core all-features focused tests, fmt, and clippy passed.

What failed or is missing:

- Local full workspace test result.

Recommended fix:

- Re-run workspace tests with `POCKET_IC_BIN` set or in CI where PocketIC is installed.

Acceptance criteria:

- `cargo test --workspace --all-features` passes in a configured environment, or the failing integration suite is explicitly excluded with rationale.

Suggested patch prompt:

- "Re-run full all-features workspace tests in an environment with PocketIC configured and update 0.197 implementation-results with the exact command output summary."

### 197-CO-007: The focused artifact harness is untracked

Category:

- artifacts / tests

Severity:

- Medium

Blocking:

- Yes for preserving evidence tooling

Confidence:

- High

Evidence:

- `testing/integration/tests/pk_canonicalization_focused_artifact.rs` is untracked.

What passed:

- `cargo test -p icydb-testing-integration --test pk_canonicalization_focused_artifact` passed its non-ignored smoke test.

What failed or is missing:

- The harness is not yet in tracked repository state.

Recommended fix:

- Add the harness in the same closeout/evidence slice if it is intended to be kept.

Acceptance criteria:

- The test file is tracked.
- Ignored real-artifact tests are documented with the command to run them.

Suggested patch prompt:

- "Track the 0.197 focused canonicalization artifact harness and document the ignored real-capture workflow in implementation-results."

### 197-CO-008: Fast-path inventory lacks the 0.197 proof-owner detail

Category:

- docs / governance

Severity:

- Low

Blocking:

- No

Confidence:

- Medium

Evidence:

- `docs/governance/fast-path-inventory.md` does not fully map the 0.197 primary-key canonicalization proof owner and guards.

What passed:

- Runtime code and tests have focused exact-key coverage.

What failed or is missing:

- Governance inventory does not yet make the proof/admission/executor ownership obvious.

Recommended fix:

- Add a row for exact primary-key canonicalization with owner, guard conditions, failure mode, and tests.

Acceptance criteria:

- Fast-path inventory names planner/admission/executor owner and points to key tests.

Suggested patch prompt:

- "Update the fast-path inventory with the 0.197 exact-primary-key canonicalization path, its guards, fail-closed cases, and test references."

## Required Follow-Up PRs

### Must fix before 0.197 can close

1. Produce real focused 0.197 before/after/delta artifacts, or explicitly close as correctness-only with no performance claim.
2. Resolve the explicit-artifact mismatch: add `PrimaryKeyCanonicalization` or amend the design to match the implemented artifact model.
3. Add zero-IO `Empty` proof for contradictory exact-key intersections, especially `pk = a AND pk IN (b)`.
4. Resolve SQL parameter proof scope: implement it or amend 0.197 to defer it.
5. Track or remove the focused artifact harness so closeout is based on a stable repository state.

### Can fix after 0.197 but before 0.198 implementation

1. Update `READ_ADMISSION.md` and `SQL_SUBSET.md` for exact-key filter admission.
2. Re-run full workspace tests in a PocketIC-configured environment.
3. Add an explain/diagnostic field or doc contract for exact-key canonicalization disabled/failure reasons.

### Can fix before 1.0

1. Add a stale-cache fixture if cache/method versions are serialized externally.
2. Expand no-default and SQL feature matrix checks.
3. Add fast-path inventory detail for exact-primary-key canonicalization.

### Measurement-only improvements

1. Add a non-synthetic focused matrix runner for exact-key behavior deltas.
2. Add a full-matrix status/access-delta report that highlights non-key selected-access changes.

### Documentation-only improvements

1. Update design status after final closeout.
2. Link closeout report and final artifacts from `implementation-results.md`.
3. Keep changelog behavior-focused and avoid performance claims without artifacts.

## Final Recommendation

Is 0.197 closed: No. It is a strong partial closeout with most exact-key behavior implemented, but not a complete closeout.

Can 0.198 design proceed: Yes.

Can 0.198 implementation proceed: Not yet. Finish or explicitly re-scope the 0.197 artifact, Empty zero-IO, SQL parameter, and evidence blockers first.

What should not be reopened:

- 0.196 ordered-read pushdown.
- Primary-key canonicalization for supported strict scalar equality and finite key sets.
- 0.198 API ergonomics as part of 0.197.

What must be fixed immediately:

- Close the proof/evidence gaps listed above or amend the 0.197 design so the closeout criteria match the intentionally supported surface.

Behavior changes to highlight to app developers:

- Strict exact primary-key filters can be admitted as bounded key reads without fake `.limit(...)` ceremony.
- Finite primary-key `IN` can be treated as bounded key-set access when within policy.
- Wrong types, malformed key terms, missing SQL parameters, invalid residuals, and over-budget inputs fail closed instead of scanning.
- SQL parameterized exact-key reads are not supported by the current SQL parameter contract unless a later patch implements them.
