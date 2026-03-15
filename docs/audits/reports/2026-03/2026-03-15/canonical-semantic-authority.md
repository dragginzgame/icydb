# Canonical Semantic Authority Audit - 2026-03-15

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering, planner, runtime, EXPLAIN, and replay-facing contracts in `crates/icydb-core/src`
- compared baseline report path: `N/A` (first `canonical-semantic-authority` report in current audit history)
- code snapshot identifier: `39b1d676`
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
- comparability status: `comparable` (initial scope baseline with unchanged CSA method manifest)

## Evidence Artifacts

- `docs/audits/reports/2026-03/2026-03-15/helpers/canonical-semantic-authority-concept-signals.tsv`
- `docs/audits/reports/2026-03/2026-03-15/helpers/canonical-semantic-authority-owner-boundaries.tsv`
- `docs/audits/reports/2026-03/2026-03-15/helpers/canonical-semantic-authority-reparse-sites.tsv`

## STEP 0 — Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Notes [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs`, `db/predicate/identifiers.rs` | SQL parser + SQL lowering identifier adaptation | `31` identifier-normalization hits across `6` files | Yes | Canonical normalization lives in `db/sql/identifier.rs`; `db/predicate/identifiers.rs` is the adaptation/rewrite boundary. |
| predicates | `db/predicate/*` with planner ingress in `db/query/plan/access_planner.rs` | SQL predicate parse + builder filter APIs | `46` predicate-normalization/coercion hits across `11` files | Yes | Structural guard test enforces canonicalization ownership boundaries. |
| index key items | `model/index.rs`, `db/index/key/expression.rs` | schema-declared index metadata | `84` key-item/expression hits across `12` files | Yes | One canonical semantic model (`model/index.rs`), one schema mirror (`schema/node/index.rs`), and one runtime deriver (`db/index/key/expression.rs`). |
| route/statement classification | `db/sql/parser/mod.rs`, `db/sql/lowering.rs`, `db/executor/route/contracts/shape.rs` | SQL statement parse/lower + route shape dispatch | `55` classification hits across `15` files | Yes | Reduced SQL statement forms map to explicit route-shape enums. |
| projection labels | `db/query/plan/expr/ast.rs`, `db/session/sql.rs`, `db/query/explain/plan.rs` | SQL projection + query builder projection | `89` projection-label hits across `12` files | Yes | Label rendering is separated from semantic projection AST tokens. |
| expression forms | `db/query/plan/expr/ast.rs` | SQL projection/aggregate clauses + fluent builder expression surfaces | `328` expression-token hits across `42` files | Yes | Planner expression AST remains typed semantic authority. |
| order keys | `db/sql/lowering.rs`, `db/query/plan/logical_builder.rs`, `db/query/plan/model.rs` | SQL `ORDER BY` + fluent sort surfaces | `315` order-key hits across `57` files | Yes | Canonical PK tie-break insertion remains explicit and deterministic. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity matching boundaries | SQL entity route and typed entity/index model surfaces | `393` entity/index identity hits across `70` files | Yes | Typed model boundaries enforce entity/index identity checks before execution. |

## STEP 1 — Representation Matrix

Evidence mode: `semi-mechanical`

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | SQL qualified identifiers | normalized field names from `normalize_identifier_to_scope(...)` and `rewrite_field_identifiers(...)` | field names in schema/entity models | normalized predicate/order field names | normalized execution field references | replay does not reparse identifiers | explain uses already-normalized fields | Yes | No |
| predicates | SQL predicate text + fluent predicate builders | `Predicate` + canonical normalized predicate from `normalize(...)` | index predicate metadata remains input-only text until lowered | `normalize_query_predicate(...)` consumes canonical predicate | executor consumes compiled predicate program | replay consumes prepared operations, not text predicates | explain consumes predicate model (`predicate_model_for_hash`) | Yes | Low (input-only schema predicate text) |
| index key items | schema index declarations | `IndexKeyItem` + `IndexExpression` | `schema::node::index::{IndexKeyItem, IndexExpression}` | planner/access consume model key items | runtime derives expression values via `derive_index_expression_value(...)` | replay uses persisted key bytes, not textual expressions | explain uses lowered index access surfaces | Yes | Low (display/canonical_text is diagnostic only) |
| route/statement classification | SQL statement text | `SqlStatement`, `SqlStatementRoute`, `RouteShapeKind` | n/a | lowered statement mode + logical plan mode | route-shape dispatch contracts | replay path does not reclassify SQL statements | explain reflects route/plan projection | Yes | No |
| projection labels | SQL projection aliases and expressions | planner `Expr`, `FieldId`, `Alias` | schema fields for resolution | projection spec in planned query | runtime projection payload | replay not label-driven | deterministic projection labels in session/explain surfaces | Yes | Low (`expr_{ordinal}` fallback is presentation-only) |
| expression forms | SQL aggregate/field expressions + fluent expressions | planner expression AST | schema field/type metadata | type inference + validation over `Expr` | runtime evaluation uses validated expression contracts | replay unaffected | explain serializes canonical expression projection | Yes | No |
| order keys | SQL order terms + fluent sort calls | `OrderSpec` with canonical PK tie-break | schema field validation | `normalize_order_terms(...)` + `canonicalize_order_spec(...)` | executor consumes canonical order contract | replay ordering not reparsed from text | explain/fingerprint consume order contract | Yes | No |
| entity/index identity | SQL entity names + typed API entity generic | `EntityModel`, `IndexModel` identity contracts | schema entity/index declarations | planner resolves entity/index contracts by typed model | runtime executes on typed entity/index identity | replay applies stored operations keyed to model identity | diagnostics render stable names | Yes | Low (raw SQL entity names are ingress-only) |

## STEP 2 — Authority Count

Evidence mode: `semi-mechanical`

| Concept Family [M] | Canonical Owner Modules [C] | Boundary Mirror Modules [C] | Semantic Consumer/Deriver Modules [C] | Role Participants [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Count [D] | Canonical Bypass IDs [C] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ---- | ---- | ----: | ---- | ----: | ----: | ---- | ----: | ---- |
| identifiers | `db/sql/identifier.rs` | `-` | `db/predicate/identifiers.rs` | 2 | SQL identifier scope normalization + predicate field rewrite | 2 | 0 | `-` | N/A | Medium-Low |
| predicates | `db/predicate/normalize.rs`, `db/predicate/semantics.rs` | `-` | `db/query/plan/access_planner.rs`, `db/predicate/coercion.rs` | 4 | parser/builder ingress -> canonical normalize boundary | 2 | 0 | `-` | N/A | Medium |
| index key items | `model/index.rs` | `schema/node/index.rs` | `db/index/key/expression.rs` | 3 | schema metadata -> model metadata -> runtime expression derivation | 2 | 1 | `display/canonical_text` | N/A | Medium |
| route/statement classification | `db/sql/parser/mod.rs`, `db/executor/route/contracts/shape.rs` | `-` | `db/sql/lowering.rs`, `db/session/sql.rs` | 4 | parser classification -> lowering -> route shape selection | 2 | 0 | `-` | N/A | Medium |
| projection labels | `db/query/plan/expr/ast.rs` | `-` | `db/session/sql.rs`, `db/query/explain/plan.rs` | 3 | planner projection AST -> label rendering | 2 | 1 | `expr_{ordinal}` | N/A | Medium-Low |
| expression forms | `db/query/plan/expr/ast.rs` | `-` | `db/sql/parser/mod.rs`, `db/sql/lowering.rs` | 3 | SQL/frontend expression parse -> planner expression AST | 2 | 0 | `-` | N/A | Medium-Low |
| order keys | `db/query/plan/logical_builder.rs`, `db/query/plan/model.rs` | `-` | `db/sql/parser/mod.rs`, `db/sql/lowering.rs` | 4 | normalized SQL order terms -> canonical order spec | 2 | 0 | `-` | N/A | Medium |
| entity/index identity | `model/entity.rs`, `model/index.rs` | `-` | `db/sql/lowering.rs`, `db/session/sql.rs` | 4 | SQL entity match boundary + schema-to-model index identity boundary | 2 | 0 | `-` | N/A | Medium |

## STEP 3 — Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Sites [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |
| identifier normalization | `db/sql/identifier.rs::normalize_identifier_to_scope` | 17 | 16 | No (ingress-only) | 2 | Medium-Low |
| predicate reparsing/normalization | `db/predicate/normalize.rs::normalize` + `db/query/plan/access_planner.rs::normalize_query_predicate` | 43 | 41 | No (structural guard blocks planner/explain re-normalization) | 3 | Medium |
| index key-item interpretation | `db/index/key/expression.rs::derive_index_expression_value` | 55 | 54 | No | 2 | Medium |
| route/statement classification derivation | `db/sql/parser/mod.rs::parse_statement` + `db/executor/route/contracts/shape.rs::RouteShapeKind` | 48 | 46 | No | 3 | Medium |
| projection/expression tokenization | `db/query/plan/expr/ast.rs::{FieldId,Alias,Expr}` + `db/session/sql.rs::projection_label_from_expr` | 258 | 256 | No (labels are render-only) | 3 | Medium |
| order-key normalization | `db/sql/lowering.rs::normalize_order_terms` + `db/query/plan/logical_builder.rs::canonicalize_order_spec` | 13 | 11 | No | 2 | Medium-Low |

## STEP 4 — Cross-Surface Convergence

Evidence mode: `classified` anchored by mechanical surface mapping

| Concept Family [M] | Builder/Fluent Path [M] | SQL/Frontend Path [M] | Schema Lowering Path [M] | Planner Owner [C] | Runtime Owner [C] | Replay/Recovery Owner [C] | EXPLAIN Source [C] | Converged to One Canonical Model? [C] | Parity Gaps Count [D] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| identifiers | fluent filter/order field names | qualified SQL identifier normalization | entity/schema field resolution | query intent + planner access ingress | predicate/access runtime contracts | replay avoids identifier reparsing | explain uses normalized fields | Yes | 0 | Low |
| predicates | fluent predicate builder | SQL predicate parser | schema enum/index predicate metadata gates | predicate normalize + planner ingress | predicate semantics + compiled predicate program | replay uses prepared ops/state machine | explain predicate model hashing contract | Yes | 0 | Low |
| index key items | typed index model | SQL does not define key-item syntax directly | schema `IndexKeyItem` lowers to runtime model | planner/access key-item projections | index key derivation + ordered encoding | replay uses stored key bytes | explain access projection | Yes | 1 | Medium-Low |
| route/statement classification | typed query mode and aggregate APIs | reduced SQL statement parser/lowering | n/a | planner logical mode and route-shape hints | route contracts/dispatch | replay path is orthogonal (no SQL reclassification) | explain plan/descriptor projection | Yes | 0 | Low |
| projection labels + expression forms | typed projection and aggregate builders | SQL projection terms and aliases | schema field/type lookups | planner expression AST + projection spec | execution projection payload | replay not label-driven | explain logical plan render + session label helpers | Yes | 1 | Medium-Low |
| order keys | fluent `order_by` / `order_by_desc` | SQL `ORDER BY` terms | schema field validation | canonical order spec with PK tie-break | executor ordering contract | replay not text-order driven | explain/fingerprint use canonical order | Yes | 0 | Low |
| entity/index identity | typed entity generic and model declarations | SQL entity route + entity match guard | schema entity/index metadata | planner/model identity resolution | runtime index/entity contracts | replay keyed to persisted model identity | diagnostics render model identity | Yes | 0 | Low |

### Parity Gap Evidence (Concrete Seams)

| Concept Family [M] | Exact Seam [C] | Why Non-Authoritative [C] | Invariant/Test Blocking Promotion to Authority [C] |
| ---- | ---- | ---- | ---- |
| index key items | `model/index.rs::IndexKeyItem::canonical_text` -> `db/index/key/build.rs` diagnostic strings (`display/canonical_text`) | Semantic key behavior is owned by structured `IndexKeyItem` + `derive_index_expression_value(...)`, then encoded key bytes. Display text is render-only. | `schema_fingerprint_preserves_field_only_parity_for_key_item_metadata` and `schema_fingerprint_changes_when_expression_key_item_semantics_change` in `db/schema/fingerprint.rs` lock semantic behavior to typed key-item structure, not render text. |
| projection labels + expression forms | `db/session/sql.rs::projection_label_from_expr` fallback `expr_{ordinal}` used by `sql_projection_columns(...)` | Labels are derived after plan construction and are not consumed by planner/runtime execution paths. | `compile_sql_command_select_field_projection_parity_matches_query_and_executable_identity` in `db/sql/lowering.rs` locks semantics on canonical plan/executable identity; `sql_projection_columns_select_field_list_returns_canonical_labels` in `db/session/tests.rs` locks deterministic presentation output. |

## STEP 5 — Drift Risk Table

Evidence mode: `semi-mechanical`

| Concept Family [M] | Risk Class [C] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicates | Observed | planner/access/EXPLAIN semantic forks | `normalize_query_predicate(...)`, structural guard tests, explain predicate-model assertion | predicate normalize + predicate model | none observed | inconsistent query filtering and unstable fingerprints | High | Medium-Low |
| identifiers | Observed | duplicated lowering boundaries | SQL identifier normalization + predicate field rewrite helper | `normalize_identifier_to_scope(...)` + `rewrite_field_identifiers(...)` | none observed | mismatched field resolution between SQL and fluent surfaces | Medium-High | Medium-Low |
| index key items | Observed | schema/build/runtime representational mismatch | schema `IndexKeyItem` mirrored in runtime model and index key expression derivation | runtime `IndexKeyItem` + expression derivation boundary | raw display text if accidentally reused for semantics | index lookup mismatches and explain/index-key drift | Medium | Medium |
| route/statement classification | Observed | planner/access/EXPLAIN semantic forks | SQL statement route metadata + route shape contracts | parser/lowering + route shape enums | none observed | wrong execution path selection for equivalent queries | Medium-High | Medium-Low |
| replay/live semantics | Observed | replay/recovery semantics derived differently than live execution | replay consumes prepared operations; live path consumes canonical plan contracts | commit/replay state machine boundaries | none observed | replay-vs-live behavior divergence under recovery | Medium | Low |
| index predicate text | Preventive (future-refactor risk) | future caller promotes raw index predicate SQL text to semantic authority | schema/model retains input predicate SQL text metadata | canonical lowered predicate contracts | potential misuse of raw predicate text in future callers | conditional index behavior drift under future refactors | Medium | Medium |

## STEP 6 — Missing Canonical Models

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| identifiers | SQL + fluent entry paths continue growing | Yes | 2 | 2 | None | keep SQL normalization + predicate field adaptation split and tested | Low |
| predicates | high policy/coercion surface (`46` signals, `11` files) | Yes | 2 | 4 | None | keep canonicalization ownership guard in structural tests | Medium-Low |
| route classification | parser + route contract usage (`55` signals, `15` files) | Yes | 2 | 4 | None | keep `RouteShapeKind` as shared canonical execution-shape contract | Medium-Low |
| index key items | schema/runtime key-item metadata is active | Yes | 1 | 3 | None | maintain schema/runtime parity tests for key-item metadata and expression derivation | Medium-Low |

Hard gate result: no growing concept is missing a canonical typed model.

## STEP 7 — Canonical Authority Risk Index

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| semantic owner multiplicity | 5 | 3 | 15 |
| lowering-boundary multiplicity | 5 | 3 | 15 |
| raw-string/side-channel authority | 4 | 3 | 12 |
| reparse/normalizer duplication | 5 | 2 | 10 |
| cross-surface parity gaps | 4 | 2 | 8 |
| missing canonical models in growing concepts | 3 | 3 | 9 |
| replay/live semantic mismatch | 2 | 3 | 6 |

`overall_index = 75 / 19 = 3.9`

Interpretation: low-to-moderate drift pressure; canonical semantic authority remains stable.

## STEP 8 — Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| High non-canonical site counts in broad pattern scans | e.g., projection/expression tokenization `256` non-canonical sites | structural-but-expected fanout | many call sites are consumers, not competing semantic authorities |
| Predicate owner multiplicity (`4`) | layered predicate ownership from invariants + plan ingress | expected defensive layering | maintain ownership guardrails, no active split-brain detected |
| Route/statement classification touchpoints (`48` sites) | parser/lowering/route/explain references | expected cross-surface propagation | classification remains enum-backed and converged |
| Index key-item schema/runtime dual definitions | schema + runtime model both define key-item enums | deliberate boundary mirror | keep parity checks to avoid representational drift |

## Required Summary

0. Run metadata + comparability note
- method manifest: `CSA-1.0` baseline with comparability status `comparable`; baseline report path is `N/A`.

1. Canonical concept inventory snapshot
- inventory covers `8` concept families; all `8/8` retain a canonical typed model authority.

2. Representation matrix highlights
- canonical path completeness is `8/8`; side-channel presence remains low (`2` low-risk diagnostic-only channels).

3. Owner/boundary count deltas
- role-participant count range is `2..4` and boundary count range is `2..2`; no `role_participants > 4` or `boundary_count > 2` outliers.

4. Reparse/reinterpretation findings
- broad scans show `426` non-canonical consumer/adapter sites across the six scan families, with no confirmed raw-string semantic reparse authority.

5. Cross-surface convergence gaps
- parity gaps total `2` with explicit seam IDs (`display/canonical_text`, `expr_{ordinal}`); both are non-authoritative and guarded by parity/fingerprint invariants.

6. Missing canonical model blockers
- missing canonical model count is `0` for growing concept families.

7. Drift risk table (high/medium/low)
- observed-risk rows: `5`; preventive-risk rows: `1` (`index predicate text` future-refactor misuse path).

8. Canonical Authority Risk Index
- risk index is `3.9/10` (`75/19` weighted), in the low-to-moderate band.

9. Noise-filter interpretation
- largest raw counts are attributable to consumer fanout, not new semantic-owner branches; no owner-count or boundary-count growth spike detected.

10. Follow-up actions with owner boundary + target run
- No mandatory follow-up actions for this run (index `< 6` and no high-risk drift trigger).
- Monitoring-only action: keep schema/runtime `IndexKeyItem` parity checks in the next crosscutting run (`target: next canonical-semantic-authority run`).

11. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)
- method comparability status: `comparable`.
- all mandatory CSA steps/tables are present in this report (`STEP 0` through `STEP 8` + required summary + verification).
- owner and boundary counts were computed from inspected source boundaries and helper tables (not mention counts alone).
- status: `PASS`.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
  - `AccessPath decision owners: 3`
  - `RouteShape decision owners: 3`
  - `Predicate coercion owners: 4`
  - `Canonicalization entrypoints: 1`
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core canonicalization_ownership_stays_in_access_and_predicate_layers -- --nocapture` -> PASS
