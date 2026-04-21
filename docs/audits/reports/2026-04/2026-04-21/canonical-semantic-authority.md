# Canonical Semantic Authority Audit - 2026-04-21

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering, planner, runtime, EXPLAIN, and replay-facing contracts in `crates/icydb-core/src`, with secondary spot checks in `crates/icydb/src`, `crates/icydb-build/src`, `crates/icydb-schema/src`, and `crates/icydb-schema-derive/src`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-13/canonical-semantic-authority.md`
- code snapshot identifier: `7c1946c04`
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
- comparability status: `non-comparable` (the method manifest is unchanged, but this run refreshes the semi-mechanical owner-token scans around the current prepared-SQL and execution-structure owners rather than reusing the narrower 2026-04-13 scan anchors verbatim)

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-21/artifacts/canonical-semantic-authority/canonical-semantic-authority-concept-signals.tsv`
- `docs/audits/reports/2026-04/2026-04-21/artifacts/canonical-semantic-authority/canonical-semantic-authority-owner-boundaries.tsv`
- `docs/audits/reports/2026-04/2026-04-21/artifacts/canonical-semantic-authority/canonical-semantic-authority-reparse-sites.tsv`

## STEP 0 - Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Notes [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs`, `db/predicate/identifiers.rs` | SQL parser + SQL lowering identifier adaptation | `31` direct owner-token hits across `5` files | Yes | Canonical normalization still lives at the SQL/predicate edge before planner/runtime use; the current tree did not introduce a second identifier semantic owner. |
| predicates | `db/predicate/*` with planner ingress in `db/query/plan/access_planner.rs` | SQL predicate parse + builder filter APIs | `118` direct owner-token hits across `8` files | Yes | Predicate normalization plus grouped boolean canonicalization remain typed and centralized despite the parser/runtime reshaping underneath them. |
| index key items | `model/index.rs`, `db/index/key/expression.rs` | schema-declared index metadata | `398` key-item/expression hits across `33` files | Yes | Schema/runtime key-item parity remains the main monitored seam; `canonical_text(...)` is still render-only. |
| route/statement classification | `db/sql/parser/*`, `db/executor/planning/route/contracts/shape.rs`, `db/sql/lowering/mod.rs` | SQL statement parse/lower + route shape dispatch | `106` classification hits across `17` files | Yes | Statement classification still lowers once into shared route-shape contracts rather than branching again in the session or public facade. |
| projection labels | `db/query/plan/expr/ast.rs`, `db/query/builder/text_projection.rs`, `db/session/sql/projection/labels.rs` | SQL projection + builder projection + session projection label rendering | `69` label-oriented hits across `22` files | Yes | Structural labels and text projection helpers remain separate presentation owners, but both still derive from canonical expression nodes. |
| expression forms | `db/query/plan/expr/ast.rs`, `db/sql/lowering/prepare.rs`, `db/session/sql/parameter.rs` | SQL clauses + fluent builder expressions + prepared fallback parameter contracts | `229` expression-typing hits across `46` files | Yes | Prepared fallback typing is now the main monitored seam, but the current tree still points back to planner-owned coarse type families instead of a free-standing second semantic model. |
| order keys | `db/sql/lowering/mod.rs`, `db/query/plan/logical_builder.rs`, `db/query/plan/model.rs` | SQL `ORDER BY` + fluent sort surfaces | `515` order-key hits across `74` files | Yes | Order normalization still converges on typed `OrderSpec` plus canonical tie-break insertion. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity/index match boundaries | SQL entity route and typed entity/index model surfaces | `1220` entity/index identity hits across `164` files | Yes | Fanout remains broad usage rather than multiple semantic owners. |

## STEP 1 - Representation Matrix

Evidence mode: `semi-mechanical`

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | SQL qualified identifiers | normalized field names from `normalize_identifier_to_scope(...)` and `rewrite_field_identifiers(...)` | field names in schema/entity models | normalized predicate/order field names | normalized execution field references | replay does not reparse identifiers | explain uses already-normalized fields | Yes | No |
| predicates | SQL predicate text + fluent predicate builders | `Predicate` plus canonical normalized predicate and grouped boolean form | index predicate metadata remains input-only text until lowered | `normalize_query_predicate(...)` and grouped boolean canonicalization consume the canonical predicate model | executor consumes compiled predicate or grouped boolean contracts | replay consumes prepared operations, not text predicates | explain consumes predicate model and fingerprint surfaces | Yes | Low (input-only schema predicate text) |
| index key items | schema index declarations | `IndexKeyItem` plus `IndexExpression` | `schema::node::index::{IndexKeyItem, IndexExpression}` | planner/access consume runtime-model key items | runtime derives expression values via `derive_index_expression_value(...)` | replay uses stored key bytes, not textual expressions | explain uses lowered index-access surfaces | Yes | Low (`display` / `canonical_text` remain diagnostic only) |
| route/statement classification | SQL statement text | `SqlStatement`, `LoweredSqlCommand`, `LoweredSqlLaneKind`, `RouteShapeKind` | n/a | lowered statement mode plus logical plan mode | route-shape dispatch contracts | replay does not reclassify SQL statements | explain reflects route and plan projection | Yes | No |
| projection labels | SQL aliases and builder text-projection selections | planner `Expr`, `FieldId`, and `Alias` rendered through projection label helpers | schema fields for field resolution | projection spec in planned query | runtime projection payload plus session label renderers | replay is not label-driven | deterministic structural labels in session/explain surfaces | Yes | Low (`expr_{ordinal}` fallback remains presentation-only) |
| expression forms | SQL aggregate/field expressions, fluent expressions, and prepared fallback parameters | planner expression AST and planner-owned coarse type families | schema field/type metadata | type inference and validation over `Expr`; prepared fallback should project from planner-owned coarse families | executor expression contracts and scalar function evaluation | replay unaffected | explain renders canonical projection/filter/grouped shapes directly | Yes | Medium-Low (prepared template identity is intentionally syntax-bound, but prepared fallback coarse-family inference is now a real monitoring seam) |
| order keys | SQL order terms + fluent sort calls | `OrderSpec` with canonical primary-key tie-break | schema field validation | `normalize_order_terms(...)` plus `canonicalize_order_spec(...)` | executor consumes canonical order contracts | replay ordering is not reparsed from text | explain and fingerprint consume the same order contract | Yes | No |
| entity/index identity | SQL entity names + typed API generics | `EntityModel`, `IndexModel` identity contracts | schema entity/index declarations | planner resolves entity/index contracts by typed model | runtime executes on typed entity/index identity | replay applies stored operations keyed to model identity | diagnostics render stable names | Yes | Low (raw SQL names remain ingress-only) |

## STEP 2 - Authority Count

Evidence mode: `semi-mechanical`

| Concept Family [M] | Semantic Owner Modules [M] | Owner Count [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Paths [M] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ----: | ---- | ----: | ----: | ----: | ---- |
| identifiers | `db/sql/identifier.rs`; `db/predicate/identifiers.rs` | 2 | SQL lowering identifier scope normalization; predicate field rewrite adapter | 2 | 0 | 0 | Medium-Low |
| predicates | `db/predicate/normalize.rs`; `db/predicate/semantics.rs`; `db/predicate/bool_expr.rs`; `db/query/plan/access_planner.rs` | 4 | SQL predicate ingress; planner normalize ingress | 2 | 0 | 0 | Medium |
| index key items | `model/index.rs`; `schema/node/index.rs`; `db/index/key/expression.rs` | 3 | schema key-item metadata to runtime model; runtime expression to derived key value | 2 | 1 | 0 | Medium |
| route/statement classification | `db/sql/parser/*`; `db/executor/planning/route/contracts/shape.rs`; `db/sql/lowering/mod.rs` | 3 | parser statement classification to lowering; lowering plan mode to route shape | 2 | 0 | 0 | Medium-Low |
| projection labels | `db/query/plan/expr/ast.rs`; `db/query/builder/text_projection.rs`; `db/session/sql/projection/labels.rs`; `db/query/explain/plan.rs` | 4 | planner projection AST to structural SQL presentation labels; builder/session projection helpers to public output label | 2 | 1 | 0 | Medium-Low |
| expression forms | `db/query/plan/expr/ast.rs`; `db/sql/lowering/mod.rs`; `db/sql/lowering/prepare.rs`; `db/session/sql/parameter.rs`; `db/executor/projection/eval/scalar_function.rs` | 5 | SQL parsed fields and functions to planner `Expr` AST; prepared fallback parameter contracts to coarse type families; executor scalar function evaluation on canonical expression contracts | 3 | 0 | 1 | Medium |
| order keys | `db/query/plan/logical_builder.rs`; `db/query/plan/model.rs`; `db/sql/parser/mod.rs`; `db/sql/lowering/mod.rs` | 4 | SQL order terms normalization to query order spec; order spec canonicalization with primary-key tie-break | 2 | 0 | 0 | Medium-Low |
| entity/index identity | `model/entity.rs`; `model/index.rs`; `db/sql/lowering/mod.rs`; `db/session/sql/dispatch/lowered.rs` | 4 | SQL entity name to typed entity check; schema index metadata to runtime `IndexModel` identity | 2 | 0 | 0 | Medium-Low |

Current readout: owner-count range widened to `2..5`, boundary-count range remains `2..3`, and the only explicit owner-drift signal is the now-visible prepared fallback typing seam inside the expression-family line.

## STEP 3 - Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Sites [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |
| identifier normalization | `db/sql/identifier.rs::normalize_identifier_to_scope` | 42 | 17 | No | 2 | Medium-Low |
| predicate normalization | `db/query/plan/access_planner.rs::normalize_query_predicate` + `db/predicate/bool_expr.rs::canonicalize_grouped_having_bool_expr` | 118 | 110 | No | 4 | Medium |
| index key-item interpretation | `db/index/key/expression.rs::derive_index_expression_value` | 398 | 384 | No | 2 | Medium |
| route/statement classification | `db/sql/parser/mod.rs::parse_statement` + `db/executor/planning/route/contracts/shape.rs::RouteShapeKind` | 106 | 103 | No | 3 | Medium-Low |
| projection/expression tokenization | `db/query/plan/expr/ast.rs::{Expr, FieldId, Alias}` + `db/session/sql/projection/labels.rs::projection_label_from_expr` | 298 | 292 | No | 4 | Medium |
| order-key normalization | `db/sql/lowering/mod.rs::normalize_order_terms` + `db/query/plan/logical_builder.rs::canonicalize_order_spec` | 228 | 220 | No | 2 | Low |

Interpretation: the broad pattern scans still show large consumer and adapter fanout, but inspection did not find a new raw-string semantic reparse authority. The main change since the last run is structural visibility of prepared fallback typing, not a return to text-driven semantic decisions.

## STEP 4 - Cross-Surface Convergence

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Builder/Fluent Path [M] | SQL/Frontend Path [M] | Schema Lowering Path [M] | Planner Owner [C] | Runtime Owner [C] | Replay/Recovery Owner [C] | EXPLAIN Source [C] | Converged to One Canonical Model? [C] | Parity Gaps Count [D] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| identifiers | fluent field references | SQL identifier normalization | schema field names | planner consumes normalized field names | runtime consumes resolved slots | replay does not reparse identifiers | explain uses normalized names | Yes | 0 | Low |
| predicates | fluent filter builders | SQL predicate lowering + grouped boolean canonicalization | index predicate metadata | canonical predicate normalization | compiled predicate program and grouped boolean contracts | replay consumes prepared operations | explain uses predicate model/fingerprint | Yes | 0 | Medium-Low |
| index key items | typed model usage | SQL/index diagnostics only | schema key-item declarations | planner consumes runtime `IndexKeyItem` | runtime derives expression keys from typed items | replay consumes stored key bytes | explain renders typed index access | Yes | 1 | Medium |
| route/statement classification | typed query API and unified result surfaces | SQL parse/lower to `LoweredSqlCommand` and `RouteShapeKind` | n/a | planner and route-shape contracts | runtime route dispatch | replay does not reclassify SQL | session-owned explain lowering | Yes | 0 | Medium-Low |
| projection labels | fluent projection selection | SQL aliases + session projection labels | schema field names | planner `Expr` / `Alias` | runtime projection payload | replay not label-driven | structural label renderers | Yes | 1 | Medium-Low |
| expression forms | fluent `Expr` and projection builders | SQL planner expressions + prepared fallback parameters | schema field/type metadata | planner `Expr` AST and coarse-family typing | executor expression contracts and scalar function evaluation | replay unaffected | canonical projection/filter/grouped shapes render directly | Yes | 2 | Medium |

Current parity-gap readout: `4` monitored gaps remain active, and all `4` are still bounded typed mirrors or presentation helpers rather than competing raw-string runtime authorities. The new monitored pair is prepared template identity versus prepared fallback coarse-family inference, which remains bounded but now deserves explicit monitoring.

## STEP 5 - Drift Risk Table

Evidence mode: `semi-mechanical`

| Concept Family [M] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| prepared fallback typing | `prepare.rs` / `parameter.rs` drift from planner-owned coarse-family truth while broadening parameter contracts | prepared SQL fallback typing, dynamic-family inference, and compare-family reconciliation | planner expression AST plus planner-owned coarse type families | no confirmed second owner, but the fallback layer now carries the clearest drift pressure | prepared SQL could classify equivalent expressions differently from non-prepared execution | Medium-High | Medium |
| projection labels | builder/session label helpers drift semantically | structural label helpers plus builder text-projection helpers | canonical planner expression labels | presentation-only fallback label text | unstable SQL column names and confusing explain output | Medium | Medium-Low |
| index key items | schema/build/runtime representational mismatch | schema `IndexKeyItem`, runtime `IndexKeyItem`, runtime expression derivation | runtime typed key-item metadata plus expression derivation | raw display text if reused accidentally | index lookup mismatches and explain/index-key drift | Medium | Medium |
| predicates | planner/runtime/explain semantic forks | `normalize_query_predicate(...)`, grouped boolean canonicalization, structural guard tests, explain assertions | predicate normalize plus predicate model | none observed | inconsistent query filtering and unstable fingerprints | High | Medium-Low |
| route/statement classification | public/session surfaces drift away from route-shape authority | unified query results, `LoweredSqlCommand`, route-shape contracts | parser/lowering plus `RouteShapeKind` | none observed | wrong execution-path selection for equivalent query shapes | Medium-High | Medium-Low |
| identifiers | duplicated lowering boundaries | SQL identifier normalization plus predicate field rewrite helper | `normalize_identifier_to_scope(...)` plus `rewrite_field_identifiers(...)` | none observed | mismatched field resolution between SQL and fluent surfaces | Medium-High | Medium-Low |

## STEP 6 - Missing Canonical Models

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| identifiers | SQL plus fluent entry paths remain active | Yes | 2 | 2 | None | keep SQL normalization and predicate field adaptation split and tested | Low |
| predicates | normalization/coercion surface remains active (`118` direct owner-token hits) | Yes | 2 | 4 | None | keep predicate normalization and grouped boolean ownership guarded in structural tests | Medium-Low |
| route classification | parser plus route-contract usage remains active (`106` hits across `17` files) | Yes | 2 | 3 | None | keep `RouteShapeKind` and `LoweredSqlLaneKind` as the shared execution-shape contracts | Medium-Low |
| index key items | schema/runtime key-item metadata remains active (`398` hits across `33` files) | Yes | 1 | 3 | None | maintain schema/runtime parity tests for key-item metadata and expression derivation | Medium-Low |
| prepared fallback typing | prepared query parameter surface grew materially in the current tree | Yes | 2 | 5 | None | keep planner-owned coarse-family truth as the semantic authority and keep fallback inference policy-only | Medium |

Hard gate result: no growing concept is missing a canonical typed model.

## STEP 7 - Canonical Authority Risk Index

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| semantic owner multiplicity | 6 | 3 | 18 |
| lowering-boundary multiplicity | 5 | 3 | 15 |
| raw-string/side-channel authority | 4 | 3 | 12 |
| reparse/normalizer duplication | 5 | 2 | 10 |
| cross-surface parity gaps | 5 | 2 | 10 |
| missing canonical models in growing concepts | 2 | 3 | 6 |
| replay/live semantic mismatch | 2 | 3 | 6 |

`overall_index = 77 / 19 = 4.1`

Interpretation: low-to-moderate drift pressure; canonical semantic authority remains stable, but the current tree does have one clearer monitoring seam than the 2026-04-13 run: prepared fallback typing is now structurally large enough that it needs explicit canonical-owner discipline.

## STEP 8 - Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| High non-canonical site counts in broad pattern scans | `1126` non-canonical consumer/adapter hits across the six scan families | structural-but-expected fanout | Most hits are downstream consumers or lane-local adapters, not competing raw-string semantic authorities. |
| Major execution and session restructuring since the last baseline | `567` source files changed in the primary/secondary audit scope since `d23cd2cf5` | broad architectural motion | This raises monitoring pressure, but the layer-authority invariants still report `0` upward imports, `0` predicate duplication, and `1` canonicalization entrypoint. |
| Prepared fallback parameter surface is much more visible | new large `db/session/sql/parameter.rs` and expanded `db/sql/lowering/prepare.rs` surfaces | meaningful semantic-owner seam | This is not evidence of split-brain semantics by itself; it is the main area where future drift would become expensive if planner-owned coarse-family truth weakens. |
| Recent docs/version retargeting work in the worktree | local docs-only changes under `docs/design` and `docs/changelog` | out-of-scope noise | These edits do not affect the semantic-owner path and were ignored for primary audit conclusions. |

## Required Summary

0. Run metadata + comparability note
- method manifest remains `CSA-1.0`, but this run is `non-comparable` to the 2026-04-13 raw-count tables because the semi-mechanical scan anchors were refreshed around the current prepared-SQL owners.

1. Canonical concept inventory snapshot
- inventory still covers `8` core concept families, and all `8/8` retain a canonical typed model authority.

2. Representation matrix highlights
- canonical path completeness remains `8/8`; low-risk side-channel presence remains bounded to diagnostic renderers, presentation helpers, and the intentionally syntax-bound prepared template identity surface.

3. Owner/boundary count deltas
- owner-count range widened from `2..4` to `2..5`; boundary-count range remains `2..3`; confirmed owner drift count is `1`, and that one drift signal is the prepared fallback typing seam inside the expression-family line.

4. Reparse/reinterpretation findings
- broad scans show `1126` non-canonical consumer or adapter hits across six scan families, with `0` confirmed raw-string semantic reparse authorities.

5. Cross-surface convergence gaps
- parity gaps total `4`: diagnostic `canonical_text(...)`, structural `expr_{ordinal}` fallback labels, builder/session projection-label helper split, and the prepared template vs prepared fallback boundary now worth explicit monitoring.

6. Missing canonical model blockers
- missing canonical model count is `0` for growing concept families, including the prepared fallback path.

7. Drift risk table (high/medium/low)
- observed-risk rows: `6`; high-risk rows: `0`; highest active seam is prepared fallback typing rather than a raw-string or facade-level semantic fork.

8. Canonical Authority Risk Index
- risk index is `4.1/10` (`77/19` weighted), which keeps the current tree in the low-to-moderate band.

9. Noise-filter interpretation
- the biggest raw counts remain consumer fanout, not new text-driven semantic owners; the main structural change is that prepared fallback typing is now large enough to warrant dedicated monitoring.

10. Follow-up actions with owner boundary + target run
- No mandatory follow-up actions for this run (`index < 6` and no high-risk drift trigger).
- Monitoring-only: keep planner-owned coarse-family truth as the semantic authority for prepared fallback typing, and recheck `db/sql/lowering/prepare.rs` plus `db/session/sql/parameter.rs` in the next canonical-semantic-authority run.
- Monitoring-only: keep schema/runtime `IndexKeyItem` parity and `canonical_text(...)` render-only status in the next canonical-semantic-authority run.
- Monitoring-only: keep builder/session projection-label helpers presentation-only, and do not let label fallback text become a decision input.

11. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)
- method comparability status: `non-comparable` for raw-count deltas, `PASS` for current-run CSA evidence collection.
- all mandatory CSA steps and tables are present in this report (`STEP 0` through `STEP 8` plus required summary and verification).
- owner and boundary counts were computed from inspected source boundaries and direct owner-token scans, not mention counts alone.
- status: `PASS`.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
  - `AccessPath decision owners: 2`
  - `RouteShape decision owners: 3`
  - `Predicate coercion owners: 4`
  - `Canonicalization entrypoints: 1`
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core canonicalization_ownership_stays_in_access_and_predicate_layers -- --nocapture` -> PASS
- direct owner-token artifact scans for `canonical-semantic-authority-*.tsv` -> PASS
