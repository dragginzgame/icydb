# Canonical Semantic Authority Audit - 2026-03-24

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering, planner, runtime, EXPLAIN, and replay-facing contracts in `crates/icydb-core/src`, with secondary spot checks in `crates/icydb/src`, `crates/icydb-build/src`, `crates/icydb-schema/src`, and `crates/icydb-schema-derive/src`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-15/canonical-semantic-authority.md`
- code snapshot identifier: `3f453012`
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
- comparability status: `comparable` (same CSA method manifest, same primary scope, and the same concept-family inventory as the 2026-03-15 baseline)

## Evidence Artifacts

- `docs/audits/reports/2026-03/2026-03-24/artifacts/canonical-semantic-authority/canonical-semantic-authority-concept-signals.tsv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/canonical-semantic-authority/canonical-semantic-authority-owner-boundaries.tsv`
- `docs/audits/reports/2026-03/2026-03-24/artifacts/canonical-semantic-authority/canonical-semantic-authority-reparse-sites.tsv`

## STEP 0 — Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Notes [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs`, `db/predicate/identifiers.rs` | SQL parser + SQL lowering identifier adaptation | `41` identifier-normalization hits across `6` files | Yes | Canonical normalization still lives in `db/sql/identifier.rs`; predicate identifier rewrite remains the adaptation boundary, not a competing authority. |
| predicates | `db/predicate/*` with planner ingress in `db/query/plan/access_planner.rs` | SQL predicate parse + builder filter APIs | `53` predicate-normalization/coercion hits across `14` files | Yes | Predicate semantics remain normalized once and consumed structurally; no new text-authoritative bypass surfaced in the `0.63` follow-through work. |
| index key items | `model/index.rs`, `db/index/key/expression.rs` | schema-declared index metadata | `168` key-item/expression hits across `21` files | Yes | The concept family grew mechanically with wider index cleanup, but semantic ownership is still split cleanly between typed key-item metadata and runtime derivation. |
| route/statement classification | `db/sql/parser/mod.rs`, `db/executor/route/contracts/shape.rs`, `db/sql/lowering.rs` | SQL statement parse/lower + route shape dispatch | `84` classification hits across `19` files | Yes | Reduced SQL statements still lower into typed route-shape contracts; no later surface reclassifies from raw SQL text. |
| projection labels | `db/query/plan/expr/ast.rs`, `db/session/sql.rs`, `db/query/explain/plan.rs` | SQL projection + query builder projection | `145` projection-label hits across `19` files | Yes | Label rendering remains a post-plan presentation concern; quickstart/Pocket-IC test cleanup did not add a second label authority. |
| expression forms | `db/query/plan/expr/ast.rs` | SQL projection/aggregate clauses + fluent builder expression surfaces | `1061` expression-token hits across `89` files | Yes | Planner expression AST is still the only semantic owner even though consumer fanout is large. |
| order keys | `db/sql/lowering.rs`, `db/query/plan/logical_builder.rs`, `db/query/plan/model.rs` | SQL `ORDER BY` + fluent sort surfaces | `358` order-key hits across `68` files | Yes | Canonical PK tie-break insertion remains explicit and deterministic; no extra lowering boundary appeared in the `0.63` cleanup line. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity/index match boundaries | SQL entity route and typed entity/index model surfaces | `712` entity/index identity hits across `142` files | Yes | This family still has the largest fanout, but the fanout is usage/validation fanout rather than multiple semantic owners. |

## STEP 1 — Representation Matrix

Evidence mode: `semi-mechanical`

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | SQL qualified identifiers | normalized field names from `normalize_identifier_to_scope(...)` and `rewrite_field_identifiers(...)` | field names in schema/entity models | normalized predicate/order field names | normalized execution field references | replay does not reparse identifiers | explain uses already-normalized fields | Yes | No |
| predicates | SQL predicate text + fluent predicate builders | `Predicate` + canonical normalized predicate from `normalize(...)` | index predicate metadata remains input-only text until lowered | `normalize_query_predicate(...)` consumes canonical predicate | executor consumes compiled predicate program | replay consumes prepared operations, not text predicates | explain consumes predicate model/fingerprint surfaces | Yes | Low (input-only schema predicate text) |
| index key items | schema index declarations | `IndexKeyItem` + `IndexExpression` | `schema::node::index::{IndexKeyItem, IndexExpression}` | planner/access consume model key items | runtime derives expression values via `derive_index_expression_value(...)` | replay uses stored key bytes, not textual expressions | explain uses lowered index access surfaces | Yes | Low (`display` / `canonical_text` remain diagnostic only) |
| route/statement classification | SQL statement text | `SqlStatement`, `SqlStatementRoute`, `RouteShapeKind` | n/a | lowered statement mode + logical plan mode | route-shape dispatch contracts | replay path does not reclassify SQL statements | explain reflects route/plan projection | Yes | No |
| projection labels | SQL projection aliases and expressions | planner `Expr`, `FieldId`, `Alias` | schema fields for resolution | projection spec in planned query | runtime projection payload | replay not label-driven | deterministic projection labels in session/explain surfaces | Yes | Low (`expr_{ordinal}` fallback is presentation-only) |
| expression forms | SQL aggregate/field expressions + fluent expressions | planner expression AST | schema field/type metadata | type inference + validation over `Expr` | runtime evaluation uses validated expression contracts | replay unaffected | explain serializes canonical expression projection | Yes | No |
| order keys | SQL order terms + fluent sort calls | `OrderSpec` with canonical PK tie-break | schema field validation | `normalize_order_terms(...)` + `canonicalize_order_spec(...)` | executor consumes canonical order contract | replay ordering is not reparsed from text | explain/fingerprint consume order contract | Yes | No |
| entity/index identity | SQL entity names + typed API entity generic | `EntityModel`, `IndexModel` identity contracts | schema entity/index declarations | planner resolves entity/index contracts by typed model | runtime executes on typed entity/index identity | replay applies stored operations keyed to model identity | diagnostics render stable names | Yes | Low (raw SQL names remain ingress-only) |

## STEP 2 — Authority Count

Evidence mode: `semi-mechanical`

| Concept Family [M] | Semantic Owner Modules [M] | Owner Count [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Paths [M] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ----: | ---- | ----: | ----: | ----: | ---- |
| identifiers | `db/sql/identifier.rs`; `db/predicate/identifiers.rs` | 2 | SQL identifier scope normalization; predicate field rewrite adapter | 2 | 0 | 0 | Medium-Low |
| predicates | `db/predicate/normalize.rs`; `db/predicate/semantics.rs`; `db/query/plan/access_planner.rs`; `db/predicate/coercion.rs` | 4 | parser/builder ingress -> canonical normalize boundary | 2 | 0 | 0 | Medium |
| index key items | `model/index.rs`; `schema/node/index.rs`; `db/index/key/expression.rs` | 3 | schema metadata -> runtime model; runtime model -> derived value | 2 | 1 | 0 | Medium |
| route/statement classification | `db/sql/parser/mod.rs`; `db/executor/route/contracts/shape.rs`; `db/sql/lowering.rs` | 3 | parser classification -> lowering; lowering -> route shape selection | 2 | 0 | -1 | Medium-Low |
| projection labels | `db/query/plan/expr/ast.rs`; `db/session/sql.rs`; `db/query/explain/plan.rs` | 3 | planner projection AST -> label rendering | 1 | 1 | 0 | Medium-Low |
| expression forms | `db/query/plan/expr/ast.rs`; `db/sql/parser/mod.rs`; `db/sql/lowering.rs` | 3 | frontend expression parse -> planner expression AST | 2 | 0 | 0 | Medium-Low |
| order keys | `db/query/plan/logical_builder.rs`; `db/query/plan/model.rs`; `db/sql/parser/mod.rs`; `db/sql/lowering.rs` | 4 | normalized SQL order terms -> canonical order spec | 2 | 0 | 0 | Medium |
| entity/index identity | `model/entity.rs`; `model/index.rs`; `db/sql/lowering.rs`; `db/session/sql.rs` | 4 | SQL entity match boundary; schema/runtime index identity boundary | 2 | 0 | 0 | Medium |

## STEP 3 — Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Sites [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |
| identifier normalization | `db/sql/identifier.rs::normalize_identifier_to_scope` | 25 | 24 | No (ingress-only) | 2 | Medium-Low |
| predicate reparsing/normalization | `db/predicate/normalize.rs::normalize` + `db/query/plan/access_planner.rs::normalize_query_predicate` | 52 | 50 | No | 3 | Medium |
| index key-item interpretation | `db/index/key/expression.rs::derive_index_expression_value` | 107 | 106 | No | 2 | Medium |
| route/statement classification derivation | `db/sql/parser/mod.rs::parse_statement` + `db/executor/route/contracts/shape.rs::RouteShapeKind` | 58 | 56 | No | 3 | Medium-Low |
| projection/expression tokenization | `db/query/plan/expr/ast.rs::{FieldId,Alias,Expr}` + `db/session/sql.rs::projection_label_from_expr` | 493 | 491 | No | 3 | Medium |
| order-key normalization | `db/sql/lowering.rs::normalize_order_terms` + `db/query/plan/logical_builder.rs::canonicalize_order_spec` | 9 | 7 | No | 2 | Medium-Low |

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
| index key items | `model/index.rs::IndexKeyItem::canonical_text` -> index planning/build diagnostics | Semantic key behavior is owned by structured `IndexKeyItem` plus `derive_index_expression_value(...)`, then encoded key bytes. Display text is render-only. | Schema/runtime parity tests in fingerprint and key-derivation paths lock semantics to the typed key-item structure rather than render text. |
| projection labels + expression forms | `db/session/sql.rs::projection_label_from_expr` fallback `expr_{ordinal}` used by SQL projection label rendering | Labels are derived after plan construction and are not consumed by planner/runtime execution paths. | SQL lowering and session projection-label regression tests continue to lock semantic identity to canonical plan/executable shape, not fallback label text. |

## STEP 5 — Drift Risk Table

Evidence mode: `semi-mechanical`

| Concept Family [M] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicates | planner/access/EXPLAIN semantic forks | `normalize_query_predicate(...)`, structural guard tests, explain predicate-model assertion | predicate normalize + predicate model | none observed | inconsistent query filtering and unstable fingerprints | High | Medium-Low |
| identifiers | duplicated lowering boundaries | SQL identifier normalization + predicate field rewrite helper | `normalize_identifier_to_scope(...)` + `rewrite_field_identifiers(...)` | none observed | mismatched field resolution between SQL and fluent surfaces | Medium-High | Medium-Low |
| index key items | schema/build/runtime representational mismatch | schema `IndexKeyItem` mirrored in runtime model and index key expression derivation | runtime `IndexKeyItem` + expression derivation boundary | raw display text if accidentally reused for semantics | index lookup mismatches and explain/index-key drift | Medium | Medium |
| route/statement classification | planner/access/EXPLAIN semantic forks | SQL statement route metadata + route shape contracts | parser/lowering + route shape enums | none observed | wrong execution path selection for equivalent queries | Medium-High | Medium-Low |
| replay/live semantics | replay/recovery semantics derived differently than live execution | replay consumes prepared operations; live path consumes canonical plan contracts | commit/replay state machine boundaries | none observed | replay-vs-live behavior divergence under recovery | Medium | Low |
| index predicate text | future caller promotes raw index predicate SQL text to semantic authority | schema/model retains input predicate SQL text metadata | canonical lowered predicate contracts | potential misuse of raw predicate text in future callers | conditional index behavior drift under future refactors | Medium | Medium |

## STEP 6 — Missing Canonical Models

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| identifiers | SQL + fluent entry paths continue growing | Yes | 2 | 2 | None | keep SQL normalization + predicate field adaptation split and tested | Low |
| predicates | high policy/coercion surface (`53` signals, `14` files) | Yes | 2 | 4 | None | keep canonicalization ownership guard in structural tests | Medium-Low |
| route classification | parser + route contract usage (`84` signals, `19` files) | Yes | 2 | 3 | None | keep `RouteShapeKind` as the shared canonical execution-shape contract | Medium-Low |
| index key items | schema/runtime key-item metadata remains active | Yes | 1 | 3 | None | maintain schema/runtime parity tests for key-item metadata and expression derivation | Medium-Low |

Hard gate result: no growing concept is missing a canonical typed model.

## STEP 7 — Canonical Authority Risk Index

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| semantic owner multiplicity | 5 | 3 | 15 |
| lowering-boundary multiplicity | 4 | 3 | 12 |
| raw-string/side-channel authority | 4 | 3 | 12 |
| reparse/normalizer duplication | 5 | 2 | 10 |
| cross-surface parity gaps | 4 | 2 | 8 |
| missing canonical models in growing concepts | 3 | 3 | 9 |
| replay/live semantic mismatch | 2 | 3 | 6 |

`overall_index = 72 / 19 = 3.8`

Interpretation: low-to-moderate drift pressure; canonical semantic authority remains stable.

## STEP 8 — Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| High non-canonical site counts in broad pattern scans | e.g. projection/expression tokenization `491` non-canonical sites | structural-but-expected fanout | Most hits are downstream consumers, not competing semantic authorities. |
| Predicate owner multiplicity (`4`) | layered predicate ownership from invariants + plan ingress | expected defensive layering | Keep ownership guardrails, but no split-brain semantic authority was found. |
| Route/statement classification references (`58` sites) | parser/lowering/route/explain references | expected cross-surface propagation | Classification remains enum-backed and converged rather than text-driven. |
| Test/harness cleanup outside primary scope | `0.63.7` and `0.63.8` removed local wrappers in Pocket-IC and wasm-audit crates | transient-noise reduction | The recent cleanup reduced support-surface noise without changing any canonical semantic owner in `icydb-core`. |

## Required Summary

0. Run metadata + comparability note
- method manifest remains `CSA-1.0`, and this run is `comparable` to the 2026-03-15 baseline.

1. Canonical concept inventory snapshot
- inventory still covers `8` concept families, and all `8/8` retain a canonical typed model authority.

2. Representation matrix highlights
- canonical path completeness remains `8/8`; side-channel presence remains low and diagnostic-only (`2` low-risk seams).

3. Owner/boundary count deltas
- owner-count range is still `2..4`, boundary-count range is now `1..2`, and no concept exceeded the prior baseline owner-count ceiling.

4. Reparse/reinterpretation findings
- broad scans show `734` non-canonical consumer/adapter sites across the six scan families, with no confirmed raw-string semantic reparse authority.

5. Cross-surface convergence gaps
- parity gaps total `2`, both previously known diagnostic-only seams (`canonical_text`, `expr_{ordinal}`).

6. Missing canonical model blockers
- missing canonical model count is `0` for growing concept families.

7. Drift risk table (high/medium/low)
- observed-risk rows: `5`; preventive-risk rows: `1`; high-risk rows: `0`.

8. Canonical Authority Risk Index
- risk index is `3.8/10` (`72/19` weighted), slightly lower than the `3.9/10` baseline and still in the low-to-moderate band.

9. Noise-filter interpretation
- the largest raw counts remain consumer fanout, not new semantic-owner branches; the recent `0.63` test/harness cleanup did not add any new canonical semantic authority.

10. Follow-up actions with owner boundary + target run
- No mandatory follow-up actions for this run (`index < 6` and no high-risk drift trigger).
- Monitoring-only: keep schema/runtime `IndexKeyItem` parity and route-shape lowering ownership in the next canonical-semantic-authority run.

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
