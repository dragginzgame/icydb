# Canonical Semantic Authority Audit - 2026-04-22

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering,
  planner, runtime, EXPLAIN, and replay-facing contracts in
  `crates/icydb-core/src`, with secondary spot checks in `crates/icydb/src`,
  `crates/icydb-build/src`, `crates/icydb-schema/src`, and
  `crates/icydb-schema-derive/src`
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-21/canonical-semantic-authority.md`
- code snapshot identifier: `c8462c78aa`
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
- comparability status: `comparable`
  - this run keeps the same concept-family inventory, semantic-owner rules,
    boundary-count rules, and reparse/convergence rules as the 2026-04-21 run
  - inspected owner boundaries did not show a new competing semantic authority
    in the current tree

## STEP 0 - Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Notes [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs`, `db/predicate/identifiers.rs` | SQL parser + SQL lowering identifier adaptation | stable vs baseline; no new identifier-owner surfaces observed in current tree | Yes | Canonical normalization still lives at the SQL/predicate edge before planner/runtime use. |
| predicates | `db/predicate/*` with planner ingress in `db/query/plan/access_planner.rs` | SQL predicate parse + builder filter APIs | stable owner surfaces; no new predicate semantic owner introduced | Yes | Predicate normalization and grouped/truth-condition handling remain typed, but this family is still part of the broad expression seam. |
| index key items | `model/index.rs`, `db/index/key/expression.rs` | schema-declared index metadata | stable representational spread | Yes | Schema/runtime key-item parity remains the main monitored seam; `canonical_text(...)` still reads render-only. |
| route/statement classification | `db/sql/parser/*`, `db/executor/planning/route/contracts/shape.rs`, `db/sql/lowering/mod.rs` | SQL statement parse/lower + route shape dispatch | stable | Yes | Statement classification still lowers once into shared route-shape contracts. |
| projection labels | `db/query/plan/expr/ast.rs`, `db/query/builder/text_projection.rs`, `db/session/sql/projection/labels.rs` | SQL projection + builder projection + session projection label rendering | stable | Yes | Structural labels and text projection helpers remain presentation-owned, not semantic decision owners. |
| expression forms | `db/query/plan/expr/ast.rs`, expression canonicalization/type-inference owners, expression-related lowering, and bounded prepared consumers | SQL clauses + fluent builder expressions + prepared parameter contracts | active structural pressure remains here; no missing canonical model, but owner count is still broader than desired | Yes | This remains the highest-value semantic-family contraction area. The next natural owner-tightening target inside it is truth-condition semantics. |
| order keys | `db/sql/lowering/mod.rs`, `db/query/plan/logical_builder.rs`, `db/query/plan/model.rs` | SQL `ORDER BY` + fluent sort surfaces | stable | Yes | Order normalization still converges on typed `OrderSpec` plus canonical tie-break insertion. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity/index match boundaries | SQL entity route and typed entity/index model surfaces | stable | Yes | Fanout remains broad usage rather than multiple semantic owners. |

## STEP 1 - Representation Matrix

Evidence mode: `semi-mechanical`

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | SQL qualified identifiers | normalized field names from `normalize_identifier_to_scope(...)` and `rewrite_field_identifiers(...)` | field names in schema/entity models | normalized predicate/order field names | normalized execution field references | replay does not reparse identifiers | explain uses already-normalized fields | Yes | No |
| predicates | SQL predicate text + fluent predicate builders | `Predicate` plus canonical normalized predicate and planner-facing truth forms | index predicate metadata remains input-only text until lowered | planner normalization plus truth-condition handling consume the canonical model | executor consumes compiled predicate / truth contracts | replay consumes prepared operations, not text predicates | explain consumes predicate model and fingerprint surfaces | Yes | Low |
| index key items | schema index declarations | `IndexKeyItem` plus `IndexExpression` | `schema::node::index::{IndexKeyItem, IndexExpression}` | planner/access consume runtime-model key items | runtime derives expression values via typed key-item owners | replay uses stored key bytes, not textual expressions | explain uses lowered index-access surfaces | Yes | Low |
| route/statement classification | SQL statement text | `SqlStatement`, `LoweredSqlCommand`, `LoweredSqlLaneKind`, `RouteShapeKind` | n/a | lowered statement mode plus logical plan mode | route-shape dispatch contracts | replay does not reclassify SQL statements | explain reflects route and plan projection | Yes | No |
| projection labels | SQL aliases and builder text-projection selections | planner `Expr`, `FieldId`, and `Alias` rendered through projection label helpers | schema fields for field resolution | projection spec in planned query | runtime projection payload plus session label renderers | replay is not label-driven | deterministic structural labels in session/explain surfaces | Yes | Low |
| expression forms | SQL aggregate/field/truth expressions, fluent expressions, and bounded prepared parameters | planner expression AST and planner-owned type/canonicalization model | schema field/type metadata | type inference and canonicalization over `Expr` | executor expression contracts and scalar function evaluation | replay unaffected | explain renders canonical projection/filter/grouped shapes directly | Yes | Medium-Low |
| order keys | SQL order terms + fluent sort calls | `OrderSpec` with canonical primary-key tie-break | schema field validation | `normalize_order_terms(...)` plus `canonicalize_order_spec(...)` | executor consumes canonical order contracts | replay ordering is not reparsed from text | explain and fingerprint consume the same order contract | Yes | No |
| entity/index identity | SQL entity names + typed API generics | `EntityModel`, `IndexModel` identity contracts | schema entity/index declarations | planner resolves entity/index contracts by typed model | runtime executes on typed entity/index identity | replay applies stored operations keyed to model identity | diagnostics render stable names | Yes | Low |

## STEP 2 - Authority Count

Evidence mode: `semi-mechanical`

| Concept Family [M] | Semantic Owner Modules [M] | Owner Count [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Paths [M] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ----: | ---- | ----: | ----: | ----: | ---- |
| identifiers | `db/sql/identifier.rs`; `db/predicate/identifiers.rs` | 2 | SQL lowering identifier scope normalization; predicate field rewrite adapter | 2 | 0 | 0 | Medium-Low |
| predicates | `db/predicate/normalize.rs`; `db/predicate/semantics.rs`; `db/predicate/bool_expr.rs`; `db/query/plan/access_planner.rs` | 4 | SQL predicate ingress; planner normalize ingress | 2 | 0 | 0 | Medium |
| index key items | `model/index.rs`; `schema/node/index.rs`; `db/index/key/expression.rs` | 3 | schema key-item metadata to runtime model; runtime expression to derived key value | 2 | 1 | 0 | Medium |
| route/statement classification | `db/sql/parser/*`; `db/executor/planning/route/contracts/shape.rs`; `db/sql/lowering/mod.rs` | 3 | parser statement classification to lowering; lowering plan mode to route shape | 2 | 0 | 0 | Medium-Low |
| projection labels | `db/query/plan/expr/ast.rs`; `db/query/builder/text_projection.rs`; `db/session/sql/projection/labels.rs`; `db/query/explain/plan.rs` | 4 | planner projection AST to structural SQL presentation labels; builder/session projection helpers to public output label | 2 | 1 | 0 | Medium-Low |
| expression forms | `db/query/plan/expr/ast.rs`; expression canonicalization/type inference owners; expression-related lowering; bounded prepared consumers; executor scalar function evaluation | 5 | SQL parsed fields/functions to planner `Expr` AST; prepared contracts to canonical expression families; executor evaluation on canonical expression contracts | 3 | 0 | 0 | Medium |
| order keys | `db/query/plan/logical_builder.rs`; `db/query/plan/model.rs`; `db/sql/parser/mod.rs`; `db/sql/lowering/mod.rs` | 4 | SQL order-term normalization to query order spec; order spec canonicalization with primary-key tie-break | 2 | 0 | 0 | Medium-Low |
| entity/index identity | `model/entity.rs`; `model/index.rs`; `db/sql/lowering/mod.rs`; `db/session/sql/dispatch/lowered.rs` | 4 | SQL entity name to typed entity check; schema index metadata to runtime `IndexModel` identity | 2 | 0 | 0 | Medium-Low |

Current readout: owner-count range remains effectively `2..5`, boundary-count
range remains `2..3`, and no new explicit owner-drift signal appeared in the
current tree.

## STEP 3 - Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Sites [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |
| identifier normalization | `db/sql/identifier.rs::normalize_identifier_to_scope` | stable broad consumer fanout | stable broad consumer fanout | No | 2 | Medium-Low |
| predicate normalization | planner/predicate normalization entrypoints plus truth-condition helpers | stable broad consumer fanout | stable broad consumer fanout | No | 4 | Medium |
| index key-item interpretation | typed index-key owners and runtime derivation helpers | stable broad consumer fanout | stable broad consumer fanout | No | 2 | Medium |
| route/statement classification | `db/sql/parser/mod.rs::parse_statement` + `RouteShapeKind` / `LoweredSqlLaneKind` | stable broad consumer fanout | stable broad consumer fanout | No | 3 | Medium-Low |
| projection/expression tokenization | planner `Expr` / projection-label helpers | stable broad consumer fanout | stable broad consumer fanout | No | 4 | Medium |
| order-key normalization | `normalize_order_terms(...)` + `canonicalize_order_spec(...)` | stable broad consumer fanout | stable broad consumer fanout | No | 2 | Low |

Interpretation: the broad scans still show large consumer and adapter fanout,
but the current tree did not introduce a new raw-string semantic reparse
authority.

## STEP 4 - Cross-Surface Convergence

Evidence mode: `classified` anchored by mechanical surface mapping

| Concept Family [M] | Builder/Fluent Path [M] | SQL/Frontend Path [M] | Schema Lowering Path [M] | Planner Owner [C] | Runtime Owner [C] | Replay/Recovery Owner [C] | EXPLAIN Source [C] | Converged to One Canonical Model? [C] | Parity Gaps Count [D] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| identifiers | fluent field references | SQL identifier normalization | schema field names | planner consumes normalized field names | runtime consumes resolved slots | replay does not reparse identifiers | explain uses normalized names | Yes | 0 | Low |
| predicates | fluent filter builders | SQL predicate lowering + truth-condition handling | index predicate metadata | canonical predicate/truth normalization | compiled predicate program and truth contracts | replay consumes prepared operations | explain uses predicate model/fingerprint | Yes | 0 | Medium-Low |
| index key items | typed model usage | SQL/index diagnostics only | schema key-item declarations | planner consumes runtime `IndexKeyItem` | runtime derives expression keys from typed items | replay consumes stored key bytes | explain renders typed index access | Yes | 1 | Medium |
| route/statement classification | typed query API and unified result surfaces | SQL parse/lower to `LoweredSqlCommand` and route-shape contracts | n/a | planner and route-shape contracts | runtime route dispatch | replay does not reclassify SQL | session-owned explain lowering | Yes | 0 | Medium-Low |
| projection labels | fluent projection selection | SQL aliases + session projection labels | schema field names | planner `Expr` / `Alias` | runtime projection payload | replay not label-driven | structural label renderers | Yes | 1 | Medium-Low |
| expression forms | fluent `Expr` and projection/filter builders | SQL planner expressions + bounded prepared parameters | schema field/type metadata | planner `Expr` AST and canonical typing | executor expression contracts and scalar function evaluation | replay unaffected | canonical projection/filter/grouped shapes render directly | Yes | 2 | Medium |

Current parity-gap readout: no new cross-surface parity gap appeared in the
current tree. The main active seam inside the expression-family line is still a
typed-owner contraction problem, not a missing canonical model problem.

## STEP 5 - Drift Risk Table

Evidence mode: `semi-mechanical`

| Concept Family [M] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| truth-condition semantics inside expression forms | predicate/lowering truth-condition handling drifts from planner-owned expression meaning | `db/predicate/bool_expr.rs`, planner expression canonicalization/type inference, and truth-related lowering adapters | planner-owned expression typing and canonicalization | no confirmed second owner, but adjacent typed owners still touch the lane | equivalent `WHERE` / `HAVING` / wrapper/null-test forms could drift across surfaces | Medium-High | Medium |
| projection labels | builder/session label helpers drift semantically | structural label helpers plus builder text-projection helpers | canonical planner expression labels | presentation-only fallback label text | unstable SQL column names and confusing explain output | Medium | Medium-Low |
| index key items | schema/build/runtime representational mismatch | schema `IndexKeyItem`, runtime `IndexKeyItem`, runtime expression derivation | runtime typed key-item metadata plus expression derivation | raw display text if reused accidentally | index lookup mismatches and explain/index-key drift | Medium | Medium |
| predicates | planner/runtime/explain semantic forks | normalize + truth-condition handling + structural guard tests + explain assertions | canonical predicate/truth normalization | none observed | inconsistent query filtering and unstable fingerprints | High | Medium-Low |
| route/statement classification | public/session surfaces drift away from route-shape authority | unified query results, `LoweredSqlCommand`, route-shape contracts | parser/lowering plus route-shape contracts | none observed | wrong execution-path selection for equivalent query shapes | Medium-High | Medium-Low |
| identifiers | duplicated lowering boundaries | SQL identifier normalization plus predicate field rewrite helper | canonical identifier normalization | none observed | mismatched field resolution between SQL and fluent surfaces | Medium-High | Medium-Low |

## STEP 6 - Missing Canonical Models

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| identifiers | SQL plus fluent entry paths remain active | Yes | 2 | 2 | None | keep SQL normalization and predicate field adaptation split and tested | Low |
| predicates | normalization/coercion/truth surface remains active | Yes | 2 | 4 | None | keep predicate adaptation narrowing toward planner-owned truth semantics | Medium-Low |
| route classification | parser plus route-contract usage remains active | Yes | 2 | 3 | None | keep `RouteShapeKind` and `LoweredSqlLaneKind` as the shared execution-shape contracts | Medium-Low |
| index key items | schema/runtime key-item metadata remains active | Yes | 1 | 3 | None | maintain schema/runtime parity tests for key-item metadata and expression derivation | Medium-Low |
| expression forms / truth-condition lane | expression-family pressure remains active | Yes | 2 | 5 | None | centralize truth-condition semantics behind planner-owned expression typing and canonicalization | Medium |

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

Interpretation: low-to-moderate drift pressure remains the right band. The
current tree did not add a new semantic fork. The highest-value active seam is
now best described as the truth-condition lane inside the broader
expression-family line.

## STEP 8 - Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| High non-canonical site counts in broad pattern scans | broad consumer/adapter fanout still dominates scan output | structural-but-expected fanout | Most hits are downstream consumers or lane-local adapters, not competing raw-string semantic authorities. |
| Current local changes are audit/design docs rather than semantic-runtime edits | docs-only work under `docs/design` and `docs/audits/reports` | out-of-scope noise | These changes sharpen prioritization, but they do not by themselves create a new semantic owner or remove an existing one. |
| Expression-family contraction planning is now more explicit | new `0.116` design docs describe truth-condition centralization | planning signal, not shipped semantic drift | This is useful for follow-through targeting, not evidence of current split-brain semantics. |

## Required Summary

0. Run metadata + comparability note
- method manifest remains `CSA-1.0`, and this run is comparable to the
  2026-04-21 baseline.

1. Canonical concept inventory snapshot
- inventory still covers `8` core concept families, and all `8/8` retain a
  canonical typed model authority.

2. Representation matrix highlights
- canonical path completeness remains `8/8`; side-channel presence remains
  bounded to diagnostics, presentation helpers, and other non-decision paths.

3. Owner/boundary count deltas
- owner-count range remains effectively `2..5`; boundary-count range remains
  `2..3`; no new owner drift appeared in this run.

4. Reparse/reinterpretation findings
- no confirmed raw-string semantic reparse authority was found in the current
  tree.

5. Cross-surface convergence gaps
- no new parity gaps were introduced; the main live seam remains typed-owner
  contraction inside expression forms rather than missing canonical models.

6. Missing canonical model blockers
- missing canonical model count remains `0` for growing concept families.

7. Drift risk table (high/medium/low)
- observed-risk rows remain bounded; high-risk rows remain `0`; the clearest
  first contraction target is the truth-condition lane inside the broader
  expression-family cluster.

8. Canonical Authority Risk Index
- risk index remains `4.1/10` (`77/19` weighted), which keeps the current tree
  in the low-to-moderate band.

9. Noise-filter interpretation
- the main new signal is prioritization, not fresh semantic drift: `0.116`
  identifies the next natural owner-tightening target, but the current shipped
  tree still has one canonical model per audited concept family.

10. Follow-up actions with owner boundary + target run
- No mandatory follow-up actions for this run (`index < 6` and no high-risk
  drift trigger).
- Monitoring-only: keep planner-owned expression typing and canonicalization as
  the semantic authority for the truth-condition lane, especially across
  `db/predicate/*`, planner expression owners, and truth-related lowering
  adapters.
- Monitoring-only: keep schema/runtime `IndexKeyItem` parity and
  `canonical_text(...)` render-only status in the next canonical-semantic-authority run.
- Monitoring-only: keep builder/session projection-label helpers
  presentation-only, and do not let label fallback text become a decision
  input.

11. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)
- method comparability status: `comparable`
- all mandatory CSA steps and tables are present in this report
- owner and boundary conclusions were grounded in inspected source boundaries,
  the layer-authority invariant script, and direct source scans
- status: `PASS`

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
  - `AccessPath decision owners: 2`
  - `RouteShape decision owners: 3`
  - `Predicate coercion owners: 4`
  - `Canonicalization entrypoints: 1`
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core canonicalization_ownership_stays_in_access_and_predicate_layers -- --nocapture` -> PASS
- direct source scan for current owner boundaries -> PASS
