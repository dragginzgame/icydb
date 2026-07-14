# Canonical Semantic Authority Audit - 2026-06-05

## 0. Scope, Baseline, and Method

| Field | Value |
| ---- | ---- |
| scope | `crates/icydb-core/src`, `crates/icydb-core/tests`, `crates/icydb-schema/src`, `crates/icydb-schema-derive/src` |
| baseline report | `docs/audits/reports/2026-05/2026-05-01/canonical-semantic-authority.md` |
| code snapshot | `200ad67ca` plus local uncommitted audit artifacts |
| method tag | `CSA-1.0` |
| comparability | comparable with caveat |

Method manifest:

- `method_version = CSA-1.0`
- `concept_inventory_model = CI-1`
- `representation_matrix_model = RM-1`
- `authority_count_rule = AC-1`
- `reparse_scan_rule = RS-1`
- `convergence_rule = CV-1`
- `risk_rubric = RR-1`
- `noise_filter_rule = NF-1`

The method, scope shape, and scoring rubric match the latest comparable
baseline. The comparability caveat is evidence-volume inflation: the current
tree includes the completed 0.178 SQL DDL transition-admission work, which adds
many schema-version, schema-fingerprint, publication-identity, and no-op DDL
test references. Those hits are classified as typed authority fanout unless
they also create a competing semantic owner.

## Artifacts

| Artifact | Lines | Purpose |
| ---- | ----: | ---- |
| `docs/audits/reports/2026-06/2026-06-05/artifacts/canonical-semantic-authority/aggregate-identity-signals.txt` | 695 | aggregate identity and distinct-normalization authority scan |
| `docs/audits/reports/2026-06/2026-06-05/artifacts/canonical-semantic-authority/expression-stage-signals.txt` | 345 | typed expression, compiled predicate, and function semantics scan |
| `docs/audits/reports/2026-06/2026-06-05/artifacts/canonical-semantic-authority/broad-normalization-signals.txt` | 14,503 | broad canonicalization, lowering, schema identity, and parser scan |
| `docs/audits/reports/2026-06/2026-06-05/artifacts/canonical-semantic-authority/explain-fingerprint-signals.txt` | 8,625 | explain, cache-key, fingerprint, and publication-identity scan |

## STEP 0 - Concept Inventory

| Concept Family | Canonical Model Present? | Current Authority | Notes |
| ---- | ---- | ---- | ---- |
| identifiers and field paths | yes | model/schema identifiers and accepted schema snapshots | no independent parser-side runtime authority found |
| predicates and boolean admission | yes | query predicate planner/runtime contracts | compiled predicate traits remain expression-owned |
| index key items | yes | index key taxonomy and schema-owned accepted index metadata | `canonical_text` is persisted metadata and fingerprint input, not a reparsed execution owner |
| route and statement classification | yes | SQL parser DTOs plus session/query route contracts | DDL schema-version contracts are parsed once then bound as typed contracts |
| projection and explain labels | yes | query/explain renderers | labels are diagnostic renderings, not semantic inputs |
| expression forms and scalar functions | yes | query expression planner and `FunctionSpec` | scalar semantics remain in function-semantics registry |
| order keys | yes | query order planning and semantic index key refs | no separate runtime ordering authority found |
| entity/index/schema identity | yes | accepted schema snapshots, schema admission identity, schema mutation identity, publication identity | 0.178 adds typed DDL admission and runtime publication identity without giving SQL DDL schema-store write authority |

## STEP 1 - Representation Matrix

| Concept | Parsed Form | Canonical Form | Runtime Form | Persisted/Cache Form | Side-Channel Authority? |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers and field paths | SQL/model path tokens | schema/model field path contracts | slots and accepted schema lookup | accepted schema snapshot | no |
| predicates | SQL predicate AST | normalized predicate/expression plan | `CompiledPredicate` / predicate program | query fingerprint sections | no |
| index key items | model/index definitions and SQL DDL intent | schema-owned accepted index key metadata | index key build/read contracts | accepted schema/index snapshots | low: display `canonical_text` only |
| route and statement classification | SQL parser statement DTOs | bound request/route contracts | session execute route | compiled SQL and plan cache keys | no |
| projection/explain labels | SQL projection AST | plan projection terms | executor result metadata | explain JSON/text | low: diagnostic rendering only |
| expression forms/functions | expression AST | typed expression/function specs | compiled expression/predicate program | plan fingerprint | no |
| order keys | order AST | semantic order/index key refs | planner/executor comparison terms | plan/cache key | no |
| entity/index/schema identity | schema snapshots and DDL version clauses | `AcceptedCatalogIdentity`, `SchemaAdmissionIdentityComparison`, `SchemaDdlMutationAdmission`, `SchemaMutationPublicationIdentity` | runtime epoch/publication identity and accepted schema cache identity | `SchemaStore` metadata and SQL cache keys with method version | no |

## STEP 2 - Authority Count

| Concept | Semantic Owners | Lowering Boundaries | Confirmed Bypasses | Risk |
| ---- | ----: | ----: | ----: | ---- |
| identifiers and field paths | 2 | 2 | 0 | Medium-Low |
| predicates and boolean admission | 4 | 2 | 0 | Medium-Low |
| index key items | 4 | 2 | 1 diagnostic/metadata rendering surface | Medium |
| route and statement classification | 3 | 2 | 0 | Medium-Low |
| projection and explain labels | 4 | 2 | 1 diagnostic rendering surface | Medium-Low |
| expression forms and scalar functions | 5 | 3 | 0 | Medium-Low |
| order keys | 4 | 2 | 0 | Medium-Low |
| entity/index/schema identity | 5 | 3 | 0 | Medium-Low |

The entity/index/schema identity owner count rose with the 0.178 migration
work, but the new owners are typed schema-owned modules:
`schema/mutation/ddl_admission.rs`, `schema/transition/admission.rs`,
`schema/mutation/identity.rs`, and `schema/reconcile/sql_ddl.rs`. SQL DDL
retains frontend binding/reporting duties.

## STEP 3 - Reparse / Side-Channel Scan

| Scan | Current Hits | Confirmed Raw-String Reparse Authority | Interpretation |
| ---- | ----: | ----: | ---- |
| aggregate identity | 695 | 0 | `AggregateIdentity` remains the common identity for lowering, grouping, and query fingerprinting |
| expression stage | 345 | 0 | compiled predicate/function semantics are typed expression-layer contracts |
| broad normalization/schema identity | 14,503 | 0 | high count is mainly generated by schema DDL/version contract tests and typed identity fanout |
| explain/fingerprint/cache | 8,625 | 0 | cache and diagnostics carry schema fingerprint method versions rather than bare fingerprint-only authority |

No runtime path was found that reparses canonical SQL/display text as the source
of execution semantics. The main side-channel-like surface remains expression
`canonical_text` in accepted index metadata; it is created by schema/index
authority and used for persistence, diagnostics, and fingerprinting.

## STEP 4 - Cross-Surface Convergence

| Surface Pair | Status | Evidence | Notes |
| ---- | ---- | ---- | ---- |
| SQL aggregate lowering vs query aggregate planning | PASS | `AggregateIdentity::from_kind_input_and_distinct`, `AggregateIdentity::normalize_distinct_for_kind` | distinct semantics converge through one identity type |
| SQL DDL version clauses vs schema mutation admission | PASS | `BoundSqlDdlSchemaVersionContract`, `SchemaDdlMutationAdmission`, `schema_admission_rejection(comparison)` | SQL binds typed intent; schema-owned admission applies the version/fingerprint matrix |
| accepted schema snapshots vs generated models | PASS | write-boundary guards assert snapshot-only accepted fingerprints and no generated fallback authority | generated models remain proposal/reconciliation inputs |
| query/compiled SQL cache vs schema fingerprints | PASS | cache keys carry `schema_fingerprint_method_version` and fingerprint | method-version-aware cache invalidation is explicit |
| schema mutation publication vs runtime identity | PASS | `SchemaMutationPublicationIdentity`, runtime epoch tests | publication identity makes stale runtime state self-invalidating |
| explain/rendering vs semantic planning | PASS with watch | explain/fingerprint scan | renderers remain diagnostic; do not feed execution authority |

Residual parity gaps are the same low/medium watch items as the baseline:
diagnostic label rendering, canonical expression text as persisted metadata,
and broad explain/fingerprint fanout.

## STEP 5 - Drift Risk Ledger

| Trigger | Current Surface | Canonical Authority | Competing Authority Found? | Risk |
| ---- | ---- | ---- | ---- | ---- |
| SQL DDL frontend starts interpreting schema fingerprints or version gaps | SQL parser/binder and DDL report modules | schema-owned `SchemaDdlMutationAdmission` and `SchemaAdmissionIdentityComparison` | no | Medium-Low |
| generated model facts become runtime fallback authority | generated reconciliation and schema proposal paths | accepted schema snapshot and snapshot-derived fingerprints | no | Medium-Low |
| expression `canonical_text` is reused as parser input | schema/index metadata, explain, fingerprinting | typed index expression metadata and expression planner | no confirmed reparse | Medium |
| query cache compares raw fingerprint bytes without method version | SQL compiled/query caches | method-version-qualified cache identity | no | Low-Medium |
| new DDL classes introduce physical work semantics in SQL frontend | SQL DDL field/index modules | schema mutation request/plan/execution modules | no | Medium-Low |
| publication identity advances outside schema-owned publication | schema mutation publication and runtime epoch | `SchemaMutationPublicationIdentity` and accepted catalog identity | no | Low-Medium |

## STEP 6 - Missing Canonical Models

| Candidate Missing Model | Status | Evidence | Risk |
| ---- | ---- | ---- | ---- |
| schema DDL version/fingerprint admission identity | present | `SchemaAdmissionIdentityComparison`, `SchemaDdlMutationAdmission`, `SchemaDdlSchemaVersionAdmissionError` | Low |
| schema mutation publication/runtime epoch identity | present | `SchemaMutationPublicationIdentity`, field-path publication tests | Low |
| aggregate distinct semantics | present | `AggregateIdentity` and grouped/fingerprint consumers | Low |
| expression scalar function semantics | present | `FunctionSpec` and compiled predicate contracts | Low |
| generated-model fallback boundary | present as guard, not a runtime model | write-boundary guards and accepted snapshot fingerprint functions | Low-Medium |

No blocker-level missing canonical model was found.

## STEP 7 - Canonical Authority Risk Index

| Area | Score | Weight | Weighted |
| ---- | ----: | ----: | ----: |
| semantic owner multiplicity | 5 | 3 | 15 |
| lowering-boundary multiplicity | 5 | 3 | 15 |
| raw-string / side-channel authority | 3 | 3 | 9 |
| reparse duplication | 4 | 2 | 8 |
| cross-surface parity gaps | 4 | 2 | 8 |
| missing canonical models | 1 | 3 | 3 |
| replay/live semantic mismatch | 2 | 3 | 6 |

Overall canonical semantic authority risk index: **3.4/10** (`64 / 19`).

Status: **PASS**. No high/critical drift finding and no unresolved
canonical-model blocker were found.

## STEP 8 - Noise Filter

| Signal | Raw Interpretation | Filtered Interpretation |
| ---- | ---- | ---- |
| scan lines grew sharply from the 2026-05 baseline | possible authority spread | mostly expected typed fanout from 0.178 DDL admission, schema identity, and tests |
| many `schema_fingerprint` hits outside schema modules | possible raw fingerprint bypass | cache/session consumers carry method versions and are guarded by source tests |
| many `canonical_text` hits | possible string authority | current uses are metadata, diagnostics, and fingerprinting; no confirmed parser re-entry |
| module splits from 0.179 cleanup | more files and references | owner-local structural improvement, not semantic drift by itself |
| uncommitted audit artifacts | dirty tree | acceptable for audit report; no production code edits in this audit slice |

## Required Summary

1. Method and baseline
   - `CSA-1.0` completed against
     `docs/audits/reports/2026-05/2026-05-01/canonical-semantic-authority.md`;
     result is comparable with an evidence-volume caveat.

2. Concept inventory
   - `8` concept families inspected; `8/8` have a canonical typed model or
     canonical accepted snapshot authority.

3. Authority counts
   - semantic owner counts range from `2` to `5`; no confirmed bypass owns
     runtime semantics.

4. Reparse/side-channel scan
   - `24,168` total signal lines inspected across four artifacts; confirmed
     raw-string reparse authority count is `0`.

5. Convergence
   - SQL DDL admission, schema mutation/publication identity, query caches,
     aggregate identity, expression semantics, and accepted snapshot authority
     converge through typed owner modules.

6. Missing models
   - blocker-level missing canonical model count is `0`.

7. Risk index
   - overall canonical semantic authority risk is `3.4/10`, PASS.

8. Follow-up
   - monitor `canonical_text` so it stays derived metadata and never becomes a
     runtime parser input.
   - keep new SQL DDL classes routed through schema-owned mutation/admission
     modules.
   - keep generated models limited to proposal, reconciliation, model-only
     convenience, and tests.

## Verification Readout

- `PASS`: required CSA-1.0 searches generated the four evidence artifacts.
- `PASS`: schema DDL admission and publication authority source anchors found
  in schema-owned modules.
- `PASS`: source guards cover no direct SQL DDL schema-store publication,
  schema admission identity ownership, method-qualified schema fingerprints,
  and runtime publication identity.
- `PASS`: `cargo test -p icydb-core --test write_boundary_guards -- --nocapture`
  passed (`75` tests).
- `PASS`: `cargo test -p icydb-core canonical -- --nocapture` passed (`202`
  matching unit tests plus `3` matching write-boundary guard tests; compile
  test filters had `0` matching tests).
- `PASS`: no high/critical canonical semantic authority drift found.
