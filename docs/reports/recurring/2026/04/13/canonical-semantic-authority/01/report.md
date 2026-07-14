# Canonical Semantic Authority Audit - 2026-04-13

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering, planner, runtime, EXPLAIN, and replay-facing contracts in `crates/icydb-core/src`, with secondary spot checks in `crates/icydb/src`, `crates/icydb-build/src`, `crates/icydb-schema/src`, and `crates/icydb-schema-derive/src`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-30/canonical-semantic-authority.md`
- code snapshot identifier: `d23cd2cf5`
- method tag/version: `CSA-1.0`
- method manifest:
  - `method_version = CSA-1.0`
  - `concept_inventory_model = CI-1`
  - `representation_matrix_model = RM-1`
  - `authority_count_rule = AC-1`
  - `reparse_scan_rule = RS-1`
  - `convergence_rule = CV-1`
  - `risk_rubric = RR-1`
  - `noise_filter_rule = NF-1`
- comparability status: `non-comparable` (the baseline report references artifact TSVs that are not present in the checked-in audit tree, so this rerun rebuilds the same concept families from current direct owner-token scans but cannot claim raw-count continuity with the missing baseline artifacts)

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-13/artifacts/canonical-semantic-authority/canonical-semantic-authority-concept-signals.tsv`
- `docs/audits/reports/2026-04/2026-04-13/artifacts/canonical-semantic-authority/canonical-semantic-authority-owner-boundaries.tsv`
- `docs/audits/reports/2026-04/2026-04-13/artifacts/canonical-semantic-authority/canonical-semantic-authority-reparse-sites.tsv`

## STEP 0 - Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Notes [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs`, `db/predicate/identifiers.rs` | SQL parser + SQL lowering identifier adaptation | `37` direct owner-token hits across `5` files | Yes | Canonical normalization still lives at the SQL/session edge before planner/runtime use; the recent helper cleanup did not introduce a second identifier owner. |
| predicates | `db/predicate/*` with planner ingress in `db/query/plan/access_planner.rs` | SQL predicate parse + builder filter APIs | `19` direct normalization/coercion hits across `6` files | Yes | Predicate normalization and coercion remain centralized; the current `0.77` query/result flattening did not reopen facade-owned predicate semantics. |
| index key items | `model/index.rs`, `db/index/key/expression.rs` | schema-declared index metadata | `389` key-item/expression hits across `34` files | Yes | Schema/runtime key-item parity remains the main monitored seam; `canonical_text(...)` is still diagnostic-only. |
| route/statement classification | `db/sql/parser/*`, `db/executor/route/contracts/shape.rs`, `db/sql/lowering/mod.rs` | SQL statement parse/lower + route shape dispatch | `68` classification hits across `17` files | Yes | Statement classification still lowers once into shared route-shape contracts rather than branching again in the facade. |
| projection labels | `db/query/plan/expr/ast.rs`, `db/query/builder/text_projection.rs`, `db/session/sql/projection/labels.rs` | SQL projection + query builder projection + bounded scalar text-function projection labels | `133` label-oriented hits across `33` files | Yes | Structural labels and bounded text-function labels are still separate presentation renderers, but both now derive from canonical expression nodes rather than a session-only computed-projection plan. |
| expression forms | `db/query/plan/expr/ast.rs` plus executor-owned bounded text-function evaluation | SQL projection/aggregate clauses + fluent builder expressions + bounded text-function projection items | `753` expression-token hits across `59` files | Yes | The bounded scalar text-function slice now lowers directly into canonical `Expr::FunctionCall` nodes and remains fail-closed for grouped projection; no new generic expression owner appeared after the `0.77` cleanup. |
| order keys | `db/sql/lowering/mod.rs`, `db/query/plan/logical_builder.rs`, `db/query/plan/model.rs` | SQL `ORDER BY` + fluent sort surfaces | `410` order-key hits across `79` files | Yes | Order normalization still converges on typed `OrderSpec` plus canonical tie-break insertion. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity/index match boundaries | SQL entity route and typed entity/index model surfaces | `802` entity/index identity hits across `137` files | Yes | Fanout remains broad usage, not multiple semantic owners. |

## STEP 1 - Representation Matrix

Evidence mode: `semi-mechanical`

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | SQL qualified identifiers | normalized field names from `normalize_identifier_to_scope(...)` and `rewrite_field_identifiers(...)` | field names in schema/entity models | normalized predicate/order field names | normalized execution field references | replay does not reparse identifiers | explain uses already-normalized fields | Yes | No |
| predicates | SQL predicate text + fluent predicate builders | `Predicate` plus canonical normalized predicate from `normalize(...)` | index predicate metadata remains input-only text until lowered | `normalize_query_predicate(...)` consumes the canonical predicate | executor consumes compiled predicate programs | replay consumes prepared operations, not text predicates | explain consumes predicate model and fingerprint surfaces | Yes | Low (input-only schema predicate text) |
| index key items | schema index declarations | `IndexKeyItem` plus `IndexExpression` | `schema::node::index::{IndexKeyItem, IndexExpression}` | planner/access consume runtime-model key items | runtime derives expression values via `derive_index_expression_value(...)` | replay uses stored key bytes, not textual expressions | explain uses lowered index-access surfaces | Yes | Low (`display` / `canonical_text` remain diagnostic only) |
| route/statement classification | SQL statement text | `SqlStatement`, `LoweredSqlCommand`, `LoweredSqlLaneKind`, `RouteShapeKind` | n/a | lowered statement mode plus logical plan mode | route-shape dispatch contracts | replay does not reclassify SQL statements | explain reflects route and plan projection | Yes | No |
| projection labels | SQL aliases and bounded scalar text-function projection items | planner `Expr`, `FieldId`, `Alias`, and bounded text-function labels rendered from canonical expressions | schema fields for field resolution | projection spec in planned query | runtime projection payload plus bounded text-function labels | replay is not label-driven | deterministic structural labels plus bounded text-function labels in session/explain surfaces | Yes | Low (`expr_{ordinal}` fallback remains presentation-only) |
| expression forms | SQL aggregate/field expressions, fluent expressions, and bounded SQL text-function projection items | planner expression AST for structural paths, including bounded `Expr::FunctionCall` nodes | schema field/type metadata | type inference and validation over `Expr`; bounded text functions intentionally do not reopen generic planner expression ownership | validated executor expression contracts plus executor-owned bounded text-function evaluation | replay unaffected | computed projection explain now renders canonical projection expressions directly while grouped computed projection stays rejected | Yes | Low (bounded typed slice, not raw-string authority) |
| order keys | SQL order terms + fluent sort calls | `OrderSpec` with canonical primary-key tie-break | schema field validation | `normalize_order_terms(...)` plus `canonicalize_order_spec(...)` | executor consumes canonical order contracts | replay ordering is not reparsed from text | explain and fingerprint consume the same order contract | Yes | No |
| entity/index identity | SQL entity names + typed API generics | `EntityModel`, `IndexModel` identity contracts | schema entity/index declarations | planner resolves entity/index contracts by typed model | runtime executes on typed entity/index identity | replay applies stored operations keyed to model identity | diagnostics render stable names | Yes | Low (raw SQL names remain ingress-only) |

## STEP 2 - Authority Count

Evidence mode: `semi-mechanical`

| Concept Family [M] | Semantic Owner Modules [M] | Owner Count [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Paths [M] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ----: | ---- | ----: | ----: | ----: | ---- |
| identifiers | `db/sql/identifier.rs`; `db/predicate/identifiers.rs` | 2 | SQL lowering identifier scope normalization; predicate field rewrite adapter | 2 | 0 | 0 | Medium-Low |
| predicates | `db/predicate/normalize.rs`; `db/predicate/semantics.rs`; `db/query/plan/access_planner.rs`; `db/predicate/coercion.rs` | 4 | SQL predicate ingress; planner normalize ingress | 2 | 0 | 0 | Medium-Low |
| index key items | `model/index.rs`; `schema/node/index.rs`; `db/index/key/expression.rs` | 3 | schema key-item metadata to runtime model; runtime model expression to derived key value | 2 | 1 | 0 | Medium |
| route/statement classification | `db/sql/parser/*`; `db/executor/route/contracts/shape.rs`; `db/sql/lowering/mod.rs` | 3 | parser statement classification to lowering; lowering plan mode to route shape | 2 | 0 | 0 | Medium-Low |
| projection labels | `db/query/plan/expr/ast.rs`; `db/query/builder/text_projection.rs`; `db/session/sql/projection/labels.rs`; `db/query/explain/plan.rs` | 4 | planner projection AST to structural SQL presentation labels; bounded text-function expression to output label | 2 | 1 | 0 | Medium-Low |
| expression forms | `db/query/plan/expr/ast.rs`; `db/sql/parser/*`; `db/sql/lowering/mod.rs`; `db/executor/projection/eval/text_function.rs` | 4 | SQL parsed aggregates and fields to planner `Expr` AST; SQL text-function projection items to canonical `Expr::FunctionCall`; builder expressions to planner projection selection | 3 | 0 | 0 | Medium |
| order keys | `db/query/plan/logical_builder.rs`; `db/query/plan/model.rs`; `db/sql/parser/mod.rs`; `db/sql/lowering/mod.rs` | 4 | SQL order terms normalization to query order spec; order spec canonicalization with primary-key tie-break | 2 | 0 | 0 | Medium-Low |
| entity/index identity | `model/entity.rs`; `model/index.rs`; `db/sql/lowering/mod.rs`; `db/session/sql/dispatch/lowered.rs` | 4 | SQL entity name to typed entity check; schema index metadata to runtime `IndexModel` identity | 2 | 0 | 0 | Medium-Low |

Current readout: owner-count range remains `2..4`, boundary-count range remains `2..3`, and no concept family added a new semantic owner in the current tree.

## STEP 3 - Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Sites [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |
| identifier normalization | `db/sql/identifier.rs::normalize_identifier_to_scope` | 37 | 15 | No | 2 | Medium-Low |
| predicate normalization | `db/predicate/normalize.rs::normalize` + `db/query/plan/access_planner.rs::normalize_query_predicate` | 19 | 4 | No | 3 | Medium-Low |
| index key-item interpretation | `db/index/key/expression.rs::derive_index_expression_value` | 389 | 375 | No | 2 | Medium |
| route/statement classification | `db/sql/parser/mod.rs::parse_statement` + `db/executor/route/contracts/shape.rs::RouteShapeKind` | 68 | 66 | No | 3 | Medium-Low |
| projection/expression tokenization | `db/query/plan/expr/ast.rs::{Expr, FieldId, Alias}` + `db/session/sql/projection/labels.rs::projection_label_from_expr` | 822 | 819 | No | 3 | Medium |
| order-key normalization | `db/sql/lowering/mod.rs::normalize_order_terms` + `db/query/plan/logical_builder.rs::canonicalize_order_spec` | 410 | 403 | No | 2 | Low |

Interpretation: the broad pattern scans still show large consumer and adapter fanout (`1,682` non-canonical hits across the six scan families), but inspection did not find any new raw-string semantic reparse authority. The recent test-helper consolidation removed support-surface duplication, not semantic-owner duplication.

## STEP 4 - Cross-Surface Convergence

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Builder/Fluent Path [M] | SQL/Frontend Path [M] | Schema Lowering Path [M] | Planner Owner [C] | Runtime Owner [C] | Replay/Recovery Owner [C] | EXPLAIN Source [C] | Converged to One Canonical Model? [C] | Parity Gaps Count [D] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| identifiers | fluent field references | SQL identifier normalization | schema field names | planner consumes normalized field names | runtime consumes resolved slots | replay does not reparse identifiers | explain uses normalized names | Yes | 0 | Low |
| predicates | fluent filter builders | SQL predicate lowering | index predicate metadata | canonical predicate normalization | compiled predicate program | replay consumes prepared operations | explain uses predicate model/fingerprint | Yes | 0 | Medium-Low |
| index key items | typed model usage | SQL/index diagnostics only | schema key-item declarations | planner consumes runtime `IndexKeyItem` | runtime derives expression keys from typed items | replay consumes stored key bytes | explain renders typed index access | Yes | 1 | Medium |
| route/statement classification | typed query API and unified `QueryResponse` | SQL parse/lower to `LoweredSqlCommand` and `RouteShapeKind` | n/a | planner and route-shape contracts | runtime route dispatch | replay does not reclassify SQL | session-owned explain lowering | Yes | 0 | Medium-Low |
| projection labels | fluent projection selection | SQL aliases + bounded scalar text-function labels | schema field names | planner `Expr` / `Alias` | runtime projection payload | replay not label-driven | structural and bounded text-function label renderers | Yes | 1 | Medium-Low |
| expression forms | fluent `Expr` and text projection APIs | SQL planner expressions + bounded scalar text-function projection items | schema field/type metadata | planner `Expr` AST | executor expression contracts + executor-owned bounded text-function evaluation | replay unaffected | canonical projection expressions render directly; grouped computed projection stays rejected | Yes | 1 | Medium |

Current parity-gap readout: `3` monitored gaps remain active, and all `3` are still bounded typed mirrors or diagnostic renderers rather than competing runtime authorities.

## STEP 5 - Drift Risk Table

Evidence mode: `semi-mechanical`

| Concept Family [M] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| bounded text functions | the bounded scalar slice broadens beyond the staged text-function contract or grouped projection stops failing closed | parser `SqlTextFunctionCall`, canonical `Expr::FunctionCall`, executor `eval_text_function_call(...)`, scalar/grouped session boundaries | parser-owned text-function taxonomy lowered once into canonical planner expressions plus executor-owned bounded evaluation | none observed | inconsistent SQL/fluent text-function semantics or accidental grouped widening | High | Medium |
| projection labels | structural and bounded computed SQL labels drift semantically | structural label helpers plus computed text-function label renderer | canonical planner expression labels and lane-local computed labels | presentation-only fallback text | unstable SQL column names and confusing explain output | Medium | Medium-Low |
| index key items | schema/build/runtime representational mismatch | schema `IndexKeyItem`, runtime `IndexKeyItem`, runtime expression derivation | runtime typed key-item metadata plus expression derivation | raw display text if reused accidentally | index lookup mismatches and explain/index-key drift | Medium | Medium |
| predicates | planner/runtime/explain semantic forks | `normalize_query_predicate(...)`, structural guard tests, explain predicate-model assertions | predicate normalize plus predicate model | none observed | inconsistent query filtering and unstable fingerprints | High | Medium-Low |
| route/statement classification | public surface flattening drifts away from core route-shape authority | unified `LoadQueryResult` / `QueryResponse`, SQL `LoweredSqlCommand`, route shape contracts | parser/lowering plus `RouteShapeKind` | none observed | wrong execution-path selection for equivalent query shapes | Medium-High | Medium-Low |
| identifiers | duplicated lowering boundaries | SQL identifier normalization plus predicate field rewrite helper | `normalize_identifier_to_scope(...)` plus `rewrite_field_identifiers(...)` | none observed | mismatched field resolution between SQL and fluent surfaces | Medium-High | Medium-Low |

## STEP 6 - Missing Canonical Models

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| identifiers | SQL plus fluent entry paths remain active | Yes | 2 | 2 | None | keep SQL normalization and predicate field adaptation split and tested | Low |
| predicates | policy/coercion surface remains active (`19` direct owner-token hits) | Yes | 2 | 4 | None | keep predicate normalization ownership guarded in structural tests | Medium-Low |
| route classification | parser plus route-contract usage remains active (`68` hits across `17` files) | Yes | 2 | 3 | None | keep `RouteShapeKind` and `LoweredSqlLaneKind` as the shared execution-shape contracts | Medium-Low |
| index key items | schema/runtime key-item metadata remains active (`389` hits across `34` files) | Yes | 1 | 3 | None | maintain schema/runtime parity tests for key-item metadata and expression derivation | Medium-Low |

Hard gate result: no growing concept is missing a canonical typed model.

The bounded scalar text-function slice also still has a canonical typed model (`SqlTextFunctionCall -> Expr::FunctionCall` plus executor-owned bounded evaluation), so it remains a monitoring seam rather than a missing-model blocker.

## STEP 7 - Canonical Authority Risk Index

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| semantic owner multiplicity | 5 | 3 | 15 |
| lowering-boundary multiplicity | 5 | 3 | 15 |
| raw-string/side-channel authority | 4 | 3 | 12 |
| reparse/normalizer duplication | 4 | 2 | 8 |
| cross-surface parity gaps | 5 | 2 | 10 |
| missing canonical models in growing concepts | 2 | 3 | 6 |
| replay/live semantic mismatch | 2 | 3 | 6 |

`overall_index = 72 / 19 = 3.8`

Interpretation: low-to-moderate drift pressure; canonical semantic authority remains stable, and the current tree did not add any new semantic owner beyond the already-monitored bounded text-function slice.

## STEP 8 - Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| High non-canonical site counts in broad pattern scans | `1,682` non-canonical consumer/adapter hits across the six scan families | structural-but-expected fanout | Most hits are downstream consumers or lane-local adapters, not competing raw-string semantic authorities. |
| Unified public query/result surfaces | fluent `LoadQueryResult` and facade `QueryResponse` now carry the same grouped/scalar split under one response family | meaningful simplification | This lowers public shape drift pressure, but it does not change the underlying semantic-owner count. |
| Direct public SQL payload proof | `sql_query_result_from_statement(...)` now has direct public tests in `crates/icydb/src/db/sql/mod.rs` | confidence gain, not owner churn | The public SQL payload surface is better proved without introducing a second SQL semantic owner. |
| Executor/session test helper consolidation plus `DEMO_RPG_WASM_PROFILE` removal | recent helper-suite cleanup and CI harness shim removal | out-of-scope support-surface reduction | These changes reduce support-surface noise and compatibility residue, but they do not create or remove canonical semantic owners in the DB semantics path. |

## Required Summary

0. Run metadata + comparability note
- method manifest remains `CSA-1.0`, but this run is `non-comparable` to the 2026-03-30 raw-count tables because the baseline report's TSV artifacts are not present in the checked-in audit tree.

1. Canonical concept inventory snapshot
- inventory still covers `8` concept families, and all `8/8` retain a canonical typed model authority.

2. Representation matrix highlights
- canonical path completeness remains `8/8`; low-risk side-channel presence remains bounded to diagnostic renderers and the already-monitored typed computed-projection mirror.

3. Owner/boundary count deltas
- owner-count range remains `2..4`; boundary-count range remains `2..3`; confirmed owner drift count is `0`.

4. Reparse/reinterpretation findings
- broad scans show `1,682` non-canonical consumer or adapter hits across six scan families, with `0` confirmed raw-string semantic reparse authorities.

5. Cross-surface convergence gaps
- parity gaps total `3`: diagnostic `canonical_text(...)`, structural `expr_{ordinal}` fallback labels, and the remaining bounded text-function label/render split across builder/session presentation helpers.

6. Missing canonical model blockers
- missing canonical model count is `0` for growing concept families.

7. Drift risk table (high/medium/low)
- observed-risk rows: `6`; high-risk rows: `0`; highest active seam remains the bounded text-function slice.

8. Canonical Authority Risk Index
- risk index is `3.8/10` (`72/19` weighted), which keeps the current tree in the low-to-moderate band.

9. Noise-filter interpretation
- the biggest raw counts remain consumer fanout, not new semantic owners; the recent test-helper cleanup and wasm-profile shim removal reduced support-surface noise outside the primary semantic-owner path.

10. Follow-up actions with owner boundary + target run
- No mandatory follow-up actions for this run (`index < 6` and no high-risk drift trigger).
- Monitoring-only: keep `SqlTextFunctionCall -> Expr::FunctionCall` on the bounded scalar path with executor-owned evaluation, and keep grouped computed projection rejected until a broader grouped expression model is designed explicitly.
- Monitoring-only: keep schema/runtime `IndexKeyItem` parity and `canonical_text(...)` render-only status in the next canonical-semantic-authority run.

11. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)
- method comparability status: `non-comparable` for raw-count deltas, `PASS` for current-run CSA evidence collection.
- all mandatory CSA steps and tables are present in this report (`STEP 0` through `STEP 8` plus required summary and verification).
- owner and boundary counts were computed from inspected source boundaries and helper tables, not mention counts alone.
- status: `PASS`.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
  - `AccessPath decision owners: 2`
  - `RouteShape decision owners: 3`
  - `Predicate coercion owners: 4`
  - `Canonicalization entrypoints: 1`
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core canonicalization_ownership_stays_in_access_and_predicate_layers -- --nocapture` -> PASS
