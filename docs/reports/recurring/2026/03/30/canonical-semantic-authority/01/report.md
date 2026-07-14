# Canonical Semantic Authority Audit - 2026-03-30

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering, planner, runtime, EXPLAIN, and replay-facing contracts in `crates/icydb-core/src`, with secondary spot checks in `crates/icydb/src`, `crates/icydb-build/src`, `crates/icydb-schema/src`, and `crates/icydb-schema-derive/src`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/canonical-semantic-authority.md`
- code snapshot identifier: `12fdfa03`
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
- comparability status: `comparable` (same CSA method manifest, same primary scope, and the same concept-family inventory as the 2026-03-24 baseline)

## Evidence Artifacts

- `docs/audits/reports/2026-03/2026-03-30/artifacts/canonical-semantic-authority/canonical-semantic-authority-concept-signals.tsv`
- `docs/audits/reports/2026-03/2026-03-30/artifacts/canonical-semantic-authority/canonical-semantic-authority-owner-boundaries.tsv`
- `docs/audits/reports/2026-03/2026-03-30/artifacts/canonical-semantic-authority/canonical-semantic-authority-reparse-sites.tsv`

## STEP 0 - Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Notes [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs`, `db/predicate/identifiers.rs` | SQL parser + SQL lowering identifier adaptation | `35` identifier-normalization hits across `4` files | Yes | Canonical normalization still lives in `db/sql/identifier.rs`; predicate rewrite remains the field-adaptation boundary, not a second text-authoritative owner. |
| predicates | `db/predicate/*` with planner ingress in `db/query/plan/access_planner.rs` | SQL predicate parse + builder filter APIs | `60` predicate-normalization/coercion hits across `14` files | Yes | Predicate normalization fanout grew slightly, but semantic ownership still converges on the canonical predicate model before execution. |
| index key items | `model/index.rs`, `db/index/key/expression.rs` | schema-declared index metadata | `176` key-item/expression hits across `19` files | Yes | The family remains split cleanly between typed key-item metadata and runtime expression derivation; `canonical_text` is still render-only. |
| route/statement classification | `db/sql/parser/*`, `db/executor/route/contracts/shape.rs`, `db/sql/lowering/mod.rs` | SQL statement parse/lower + route shape dispatch | `70` classification hits across `18` files | Yes | The parser split reduced structural complexity, but route shape still lowers once into shared typed execution-shape contracts. |
| projection labels | `db/query/plan/expr/ast.rs`, `db/session/sql/projection/labels.rs`, `db/session/sql/computed_projection/model.rs` | SQL projection + query builder projection + narrowed computed SQL projection lane | `147` projection-label hits across `19` files | Yes | The new `0.66` text-function lane adds a second label renderer, but it stays lane-local and presentation-only. |
| expression forms | `db/query/plan/expr/ast.rs` plus session-owned computed SQL projection boundary | SQL projection/aggregate clauses + fluent builder expressions + narrow text-function projection items | `1243` expression-token hits across `75` files | Yes | The main new growth signal is the bounded `SqlTextFunctionCall -> SqlComputedProjectionPlan` lane; it is typed and fail-closed, but it is still a second canonical lane for a small SQL-only surface. |
| order keys | `db/sql/lowering/mod.rs`, `db/query/plan/logical_builder.rs`, `db/query/plan/model.rs` | SQL `ORDER BY` + fluent sort surfaces | `345` order-key hits across `56` files | Yes | Canonical PK tie-break insertion remains explicit and deterministic; no new order-key lowering boundary appeared. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity/index match boundaries | SQL entity route and typed entity/index model surfaces | `853` entity/index identity hits across `142` files | Yes | Fanout remains high but still reflects broad usage rather than multiple semantic owners. |

## STEP 1 - Representation Matrix

Evidence mode: `semi-mechanical`

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | SQL qualified identifiers | normalized field names from `normalize_identifier_to_scope(...)` and `rewrite_field_identifiers(...)` | field names in schema/entity models | normalized predicate/order field names | normalized execution field references | replay does not reparse identifiers | explain uses already-normalized fields | Yes | No |
| predicates | SQL predicate text + fluent predicate builders | `Predicate` + canonical normalized predicate from `normalize(...)` | index predicate metadata remains input-only text until lowered | `normalize_query_predicate(...)` consumes canonical predicate | executor consumes compiled predicate program | replay consumes prepared operations, not text predicates | explain consumes predicate model/fingerprint surfaces | Yes | Low (input-only schema predicate text) |
| index key items | schema index declarations | `IndexKeyItem` + `IndexExpression` | `schema::node::index::{IndexKeyItem, IndexExpression}` | planner/access consume model key items | runtime derives expression values via `derive_index_expression_value(...)` | replay uses stored key bytes, not textual expressions | explain uses lowered index access surfaces | Yes | Low (`display` / `canonical_text` remain diagnostic only) |
| route/statement classification | SQL statement text | `SqlStatement`, `SqlStatementRoute`, `RouteShapeKind` | n/a | lowered statement mode + logical plan mode | route-shape dispatch contracts | replay path does not reclassify SQL statements | explain reflects route/plan projection | Yes | No |
| projection labels | SQL projection aliases and expressions, plus staged text-function projection items | planner `Expr`, `FieldId`, `Alias`, and session-owned computed projection labels | schema fields for resolution | projection spec in planned query | runtime projection payload plus lane-local computed projection label | replay not label-driven | deterministic structural labels plus computed text labels in session/explain surfaces | Yes | Low (`expr_{ordinal}` fallback remains presentation-only) |
| expression forms | SQL aggregate/field expressions, fluent expressions, and narrow SQL text-function projection items | planner expression AST for structural paths plus `SqlTextFunctionCall -> SqlComputedProjectionPlan` for the narrowed session lane | schema field/type metadata | type inference + validation over `Expr`; computed text projections intentionally do not reopen generic planner expression ownership | validated executor expression contracts plus `SqlComputedProjectionTransform` on the session lane | replay unaffected | computed projection explain rewrites to a base field-only `SELECT` before shared explain rendering | Yes | Low (`SqlComputedProjectionTransform` mirrors parser taxonomy but does not become a raw-string authority) |
| order keys | SQL order terms + fluent sort calls | `OrderSpec` with canonical PK tie-break | schema field validation | `normalize_order_terms(...)` + `canonicalize_order_spec(...)` | executor consumes canonical order contract | replay ordering is not reparsed from text | explain/fingerprint consume order contract | Yes | No |
| entity/index identity | SQL entity names + typed API entity generic | `EntityModel`, `IndexModel` identity contracts | schema entity/index declarations | planner resolves entity/index contracts by typed model | runtime executes on typed entity/index identity | replay applies stored operations keyed to model identity | diagnostics render stable names | Yes | Low (raw SQL names remain ingress-only) |

## STEP 2 - Authority Count

Evidence mode: `semi-mechanical`

| Concept Family [M] | Semantic Owner Modules [M] | Owner Count [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Paths [M] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ----: | ---- | ----: | ----: | ----: | ---- |
| identifiers | `db/sql/identifier.rs`; `db/predicate/identifiers.rs` | 2 | SQL lowering identifier scope normalization; predicate field rewrite adapter | 2 | 0 | 0 | Medium-Low |
| predicates | `db/predicate/normalize.rs`; `db/predicate/semantics.rs`; `db/query/plan/access_planner.rs`; `db/predicate/coercion.rs` | 4 | SQL predicate ingress; planner normalize ingress | 2 | 0 | 0 | Medium-Low |
| index key items | `model/index.rs`; `schema/node/index.rs`; `db/index/key/expression.rs` | 3 | schema key-item metadata to runtime model; runtime model expression to derived key value | 2 | 1 | 0 | Medium |
| route/statement classification | `db/sql/parser/*`; `db/executor/route/contracts/shape.rs`; `db/sql/lowering/mod.rs` | 3 | parser statement classification to lowering; lowering plan mode to route shape | 2 | 0 | 0 | Medium-Low |
| projection labels | `db/query/plan/expr/ast.rs`; `db/session/sql/projection/labels.rs`; `db/session/sql/computed_projection/model.rs`; `db/query/explain/plan.rs` | 4 | planner projection AST to structural SQL presentation labels; narrow text-function plan item to computed SQL output label | 2 | 1 | 1 | Medium-Low |
| expression forms | `db/query/plan/expr/ast.rs`; `db/sql/parser/*`; `db/sql/lowering/mod.rs`; `db/session/sql/computed_projection/*` | 4 | SQL parsed aggregates/fields to planner `Expr` AST; SQL text-function projection items to computed projection plan; builder expressions to planner projection selection | 3 | 0 | 1 | Medium |
| order keys | `db/query/plan/logical_builder.rs`; `db/query/plan/model.rs`; `db/sql/parser/mod.rs`; `db/sql/lowering/mod.rs` | 4 | SQL order terms normalization to query order spec; order spec canonicalization with primary-key tie-break | 2 | 0 | 0 | Medium-Low |
| entity/index identity | `model/entity.rs`; `model/index.rs`; `db/sql/lowering/mod.rs`; `db/session/sql/dispatch/lowered.rs` | 4 | SQL entity name to typed entity check; schema index metadata to runtime `IndexModel` identity | 2 | 0 | 0 | Medium-Low |

## STEP 3 - Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Sites [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |
| identifier normalization | `db/sql/identifier.rs::normalize_identifier_to_scope` | 19 | 6 | No | 2 | Medium-Low |
| predicate normalization | `db/predicate/normalize.rs::normalize` + `db/query/plan/access_planner.rs::normalize_query_predicate` | 62 | 37 | No | 4 | Medium-Low |
| index key-item interpretation | `db/index/key/expression.rs::derive_index_expression_value` | 108 | 105 | No | 2 | Medium |
| route/statement classification | `db/sql/parser/mod.rs::parse_statement` + `db/executor/route/contracts/shape.rs::RouteShapeKind` | 49 | 45 | No | 3 | Medium-Low |
| projection/expression tokenization | `db/query/plan/expr/ast.rs::{FieldId, Alias, Expr}` + `db/session/sql/projection/labels.rs::projection_label_from_expr` | 504 | 491 | No | 3 | Medium |
| order-key normalization | `db/sql/lowering/mod.rs::normalize_order_terms` + `db/query/plan/logical_builder.rs::canonicalize_order_spec` | 9 | 4 | No | 2 | Low |

## STEP 4 - Cross-Surface Convergence Gaps

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Gap Surface [M/C] | Why It Is Not Yet Split-Brain [C] | Current Guardrail [C] |
| ---- | ---- | ---- | ---- |
| index key items | `schema::node::index::IndexKeyItem::canonical_text(...)` and `model::index::IndexKeyItem::canonical_text(...)` | The duplicated renderer is still diagnostic-only and does not drive runtime key derivation. | Schema/runtime parity coverage for index metadata and runtime expression derivation keeps semantic authority on typed key items. |
| projection labels | `db/session/sql/projection/labels.rs::projection_label_from_expr` fallback `expr_{ordinal}` | The fallback is render-only and is not consumed by planner/runtime execution. | Structural projection-label tests still bind semantic identity to canonical plan shape rather than fallback text. |
| computed text projections | `db/sql/parser::{SqlTextFunction, SqlTextFunctionCall}` mirrored onto `db/session/sql/computed_projection::{SqlComputedProjectionPlan, SqlComputedProjectionTransform}` | The session mirror is typed, derives only from parser-owned text-function calls, and remains fenced behind `execute_sql_dispatch(...)` / computed-`EXPLAIN`; generic lowering and `query_from_sql(...)` still fail closed. | Session dispatch/explain tests and the `query_from_sql` rejection guard keep the narrow lane from silently becoming a second generic expression authority. |

## STEP 5 - Drift Risk Table

Evidence mode: `semi-mechanical`

| Concept Family [M] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| computed text projections | session lane broadens beyond the staged text-function contract or starts bypassing the shared lowered base `SELECT` authority | parser `SqlTextFunctionCall`, session `SqlComputedProjectionPlan`, session `SqlComputedProjectionTransform`, computed dispatch/explain | parser-owned text-function taxonomy lowered once into the session-owned plan | none active; only a bounded typed mirror | inconsistent SQL text projection semantics between dispatch, explain, and any future generic query path | High | Medium |
| projection labels | structural and computed SQL projection labels drift apart semantically | structural label helpers plus computed text-function label renderer | canonical planner expression labels and lane-local computed labels | presentation-only fallback text | unstable SQL column names and confusing explain output | Medium | Medium-Low |
| index key items | schema/build/runtime representational mismatch | schema `IndexKeyItem` mirrored in runtime model and index key expression derivation | runtime `IndexKeyItem` + expression derivation boundary | raw display text if reused accidentally | index lookup mismatches and explain/index-key drift | Medium | Medium |
| predicates | planner/access/EXPLAIN semantic forks | `normalize_query_predicate(...)`, structural guard tests, explain predicate-model assertions | predicate normalize + predicate model | none observed | inconsistent query filtering and unstable fingerprints | High | Medium-Low |
| identifiers | duplicated lowering boundaries | SQL identifier normalization + predicate field rewrite helper | `normalize_identifier_to_scope(...)` + `rewrite_field_identifiers(...)` | none observed | mismatched field resolution between SQL and fluent surfaces | Medium-High | Medium-Low |
| route/statement classification | planner/access/EXPLAIN semantic forks | SQL statement route metadata + route shape contracts | parser/lowering + route shape enums | none observed | wrong execution path selection for equivalent queries | Medium-High | Medium-Low |

## STEP 6 - Missing Canonical Models

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| identifiers | SQL + fluent entry paths continue growing | Yes | 2 | 2 | None | keep SQL normalization + predicate field adaptation split and tested | Low |
| predicates | high policy/coercion surface (`60` signals, `14` files) | Yes | 2 | 4 | None | keep canonicalization ownership guard in structural tests | Medium-Low |
| route classification | parser + route contract usage (`70` signals, `18` files) | Yes | 2 | 3 | None | keep `RouteShapeKind` as the shared canonical execution-shape contract | Medium-Low |
| index key items | schema/runtime key-item metadata remains active | Yes | 1 | 3 | None | maintain schema/runtime parity tests for key-item metadata and expression derivation | Medium-Low |

Hard gate result: no growing concept is missing a canonical typed model.

The new `0.66` computed text projection lane also has a canonical typed model (`SqlTextFunctionCall -> SqlComputedProjectionPlan`), but it stays intentionally outside the generic planner `Expr` authority and therefore remains a monitoring item rather than a missing-model blocker.

## STEP 7 - Canonical Authority Risk Index

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| semantic owner multiplicity | 5 | 3 | 15 |
| lowering-boundary multiplicity | 5 | 3 | 15 |
| raw-string/side-channel authority | 4 | 3 | 12 |
| reparse/normalizer duplication | 4 | 2 | 8 |
| cross-surface parity gaps | 5 | 2 | 10 |
| missing canonical models in growing concepts | 3 | 3 | 9 |
| replay/live semantic mismatch | 2 | 3 | 6 |

`overall_index = 75 / 19 = 3.9`

Interpretation: low-to-moderate drift pressure; canonical semantic authority remains stable, with the new bounded text-function lane as the main monitoring seam.

## STEP 8 - Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| High non-canonical site counts in broad pattern scans | e.g. projection/expression tokenization `491` non-canonical sites | structural-but-expected fanout | Most hits are downstream consumers or lane-local adapters, not competing raw-string semantic authorities. |
| New `SqlTextFunction` / `SqlComputedProjectionTransform` mirror | `0.66` added a second typed lane for a small SQL-only surface | bounded parallel authority | This is real drift pressure, but it is still fenced by parser-owned ingress, base-`SELECT` rewrite, and fail-closed generic lowering. |
| Parser split reduced file complexity | parser route/projection parsing moved into `db/sql/parser/projection.rs` | structural cleanup, not authority churn | The split lowered hotspot concentration without changing the semantic owner for statement classification. |
| Projection label dual renderers | structural labels plus computed text-function labels are both present | expected lane-local presentation split | The extra renderer is acceptable while computed text projection remains a session-only dispatch lane and not a generic planner expression surface. |

## Required Summary

0. Run metadata + comparability note
- method manifest remains `CSA-1.0`, and this run is `comparable` to the 2026-03-24 baseline.

1. Canonical concept inventory snapshot
- inventory still covers `8` concept families, and all `8/8` retain a canonical typed model authority.

2. Representation matrix highlights
- canonical path completeness remains `8/8`; side-channel presence is still low, but the count of monitored low-risk seams rose from `2` to `3` because the bounded computed text-projection lane now has its own typed transform mirror.

3. Owner/boundary count deltas
- owner-count range remains `2..4`; boundary-count range moved to `2..3` because expression forms now include the explicit `SqlTextFunctionCall -> SqlComputedProjectionPlan` lane in addition to the generic planner `Expr` lowering path.

4. Reparse/reinterpretation findings
- broad scans show `688` non-canonical consumer/adapter sites across the six scan families, with no confirmed raw-string semantic reparse authority.

5. Cross-surface convergence gaps
- parity gaps total `3`: `canonical_text`, `expr_{ordinal}`, and the new typed `SqlTextFunction` to `SqlComputedProjectionTransform` mirror for the bounded computed SQL projection lane.

6. Missing canonical model blockers
- missing canonical model count is `0` for growing concept families.

7. Drift risk table (high/medium/low)
- observed-risk rows: `5`; preventive-risk rows: `1`; high-risk rows: `0`.

8. Canonical Authority Risk Index
- risk index is `3.9/10` (`75/19` weighted), slightly above the `3.8/10` baseline but still in the low-to-moderate band.

9. Noise-filter interpretation
- the largest raw counts remain consumer fanout, not new raw-string semantic owners; the meaningful new drift pressure is the intentionally bounded text-function lane, not parser/file restructuring.

10. Follow-up actions with owner boundary + target run
- No mandatory follow-up actions for this run (`index < 6` and no high-risk drift trigger).
- Monitoring-only: keep `SqlTextFunctionCall -> SqlComputedProjectionPlan` bounded to session dispatch/explain until a generic planner expression model exists for the same semantics.
- Monitoring-only: keep `query_from_sql(...)` fail-closed for computed text projections and recheck that boundary in the next canonical-semantic-authority run.

11. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)
- method comparability status: `comparable`.
- all mandatory CSA steps/tables are present in this report (`STEP 0` through `STEP 8` + required summary + verification).
- owner and boundary counts were computed from inspected source boundaries and helper tables, not mention counts alone.
- status: `PASS`.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
  - `AccessPath decision owners: 3`
  - `RouteShape decision owners: 2`
  - `Predicate coercion owners: 4`
  - `Canonicalization entrypoints: 1`
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core canonicalization_ownership_stays_in_access_and_predicate_layers -- --nocapture` -> PASS
