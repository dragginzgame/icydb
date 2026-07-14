# Canonical Semantic Authority Audit - 2026-04-22 (Same-Day Rerun 3)

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering,
  planner, runtime, EXPLAIN, and replay-facing contracts in
  `crates/icydb-core/src`, with secondary spot checks in `crates/icydb/src`,
  `crates/icydb-build/src`, `crates/icydb-schema/src`, and
  `crates/icydb-schema-derive/src`
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-22/canonical-semantic-authority-2.md`
- code snapshot identifier: `c2482bb619`
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
  - this rerun keeps the same concept-family inventory, semantic-owner rules,
    boundary-count rules, and reparse/convergence rules as the earlier same-day
    reruns
  - current verification and focused source scans did not show a new competing
    semantic authority in the tree
  - the main purpose of this rerun is to confirm whether newer sequencing work
    changes semantic ownership, rather than just planning the next flow slice

## STEP 0 - Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Current Read [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs` plus predicate-facing identifier adapters | SQL parser + SQL lowering identifier adaptation | stable vs same-day baseline | Yes | Identifier normalization still reads as one reduced-SQL owner plus structural adapters only. |
| predicates | `db/predicate/*` with planner ingress in `db/query/plan/*` | SQL predicate parse + builder filter APIs | stable owner surfaces; no new predicate semantic owner introduced | Yes | Predicate remains typed and canonical, but still sits adjacent to the truth lane rather than owning a separate semantic model. |
| index key items | `model/index.rs`, `db/index/key/*`, `db/scalar_expr.rs` | schema-declared index metadata | stable representational spread | Yes | `IndexKeyItem` remains canonical typed metadata and `canonical_text(...)` still reads as render-only. |
| route/statement classification | `db/sql/parser/*`, `db/sql/lowering/mod.rs`, `db/executor/planning/route/contracts/shape.rs` | SQL statement parse/lower + route shape dispatch | stable | Yes | Statement shape still converges through typed route contracts. |
| projection labels | planner expression AST plus session projection label renderers | SQL projection + builder projection + session projection label rendering | stable | Yes | Projection labels remain presentation-owned rather than semantic-family owners. |
| expression forms | planner expression AST, type inference, canonicalization, bounded lowering/prepared consumers | SQL clauses + fluent builder expressions + prepared parameter contracts | active structural pressure remains here | Yes | Planner AST and typing remain the semantic center; the active seam is still adjacent owner breadth inside expression follow-through. |
| order keys | `db/sql/lowering/mod.rs`, `db/query/plan/*` | SQL `ORDER BY` + fluent sort surfaces | stable | Yes | Order normalization remains converged on typed planner contracts. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity/index match boundaries | SQL entity route and typed entity/index model surfaces | stable | Yes | Fanout remains broad usage rather than competing ownership. |

## STEP 1 - Representation Matrix

Evidence mode: `semi-mechanical`

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | SQL qualified identifiers | normalized field names from `normalize_identifier_to_scope(...)` and field-rewrite helpers | field names in schema/entity models | normalized predicate/order field names | normalized execution field references | replay does not reparse identifiers | explain uses already-normalized fields | Yes | No |
| predicates | SQL predicate text + fluent predicate builders | `Predicate` plus canonical normalized predicate and planner-facing truth forms | index predicate metadata remains input-only text until lowered | planner normalization plus truth-condition handling consume the canonical model | executor consumes compiled predicate / truth contracts | replay consumes prepared operations, not text predicates | explain consumes predicate model and fingerprint surfaces | Yes | Low |
| index key items | schema index declarations | `IndexKeyItem` plus `IndexExpression` | `schema::node::index::{IndexKeyItem, IndexExpression}` | planner/access consume runtime-model key items | runtime derives expression values via typed key-item owners | replay uses stored key bytes, not textual expressions | explain uses lowered index-access surfaces | Yes | Low |
| route/statement classification | SQL statement text | `SqlStatement`, `LoweredSqlCommand`, `LoweredSqlLaneKind`, `RouteShapeKind` | n/a | lowered statement mode plus logical plan mode | route-shape dispatch contracts | replay does not reclassify SQL statements | explain reflects route and plan projection | Yes | No |
| projection labels | SQL aliases and builder text-projection selections | planner `Expr`, `FieldId`, and `Alias` rendered through label helpers | schema fields for field resolution | projection spec in planned query | runtime projection payload plus session label renderers | replay is not label-driven | deterministic structural labels in session/explain surfaces | Yes | Low |
| expression forms | SQL aggregate/field/truth expressions, fluent expressions, and bounded prepared parameters | planner expression AST and planner-owned type/canonicalization model | schema field/type metadata | type inference and canonicalization over `Expr` | executor expression contracts and scalar function evaluation | replay unaffected | explain renders canonical projection/filter/grouped shapes directly | Yes | Medium-Low |
| order keys | SQL order terms + fluent sort calls | `OrderSpec` with canonical tie-break handling | schema field validation | normalized planner order contracts | executor consumes canonical order contracts | replay ordering is not reparsed from text | explain and fingerprint consume the same order contract | Yes | No |
| entity/index identity | SQL entity names + typed API generics | `EntityModel`, `IndexModel` identity contracts | schema entity/index declarations | planner resolves entity/index contracts by typed model | runtime executes on typed entity/index identity | replay applies stored operations keyed to model identity | diagnostics render stable names | Yes | Low |

Interpretation:

- Canonical path completeness remains `8/8`.
- No current concept family is missing its canonical typed representation.
- The current rerun still reads as one canonical model per audited concept
  family, with the live risk concentrated in adjacent flow breadth rather than
  missing semantic ownership.

## STEP 2 - Authority Count

Evidence mode: `semi-mechanical`

| Concept Family [M] | Semantic Owner Modules [M] | Owner Count [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Paths [M] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ----: | ---- | ----: | ----: | ----: | ---- |
| identifiers | `db/sql/identifier.rs`; predicate-facing field-rewrite adapter use | 2 | SQL parser/lowering identifier normalization; predicate field rewrite adapter | 2 | 0 | 0 | Medium-Low |
| predicates | `db/predicate/normalize.rs`; `db/predicate/semantics.rs`; `db/predicate/bool_expr.rs`; planner ingress/semantic checks | 4 | SQL predicate ingress; planner normalize ingress | 2 | 0 | 0 | Medium |
| index key items | `model/index.rs`; `schema/node/index.rs`; `db/index/key/*`; `db/scalar_expr.rs` | 4 | schema key-item metadata to runtime model; runtime expression to derived key value | 2 | 1 | 0 | Medium |
| route/statement classification | `db/sql/parser/*`; `db/sql/lowering/mod.rs`; `db/executor/planning/route/contracts/shape.rs` | 3 | parser statement classification to lowering; lowering plan mode to route shape | 2 | 0 | 0 | Medium-Low |
| projection labels | planner expression AST; builder text-projection helpers; session label renderers; explain property adapters | 4 | planner projection AST to outward labels; builder/session/explain structural label rendering | 2 | 1 | 0 | Medium-Low |
| expression forms | planner expression AST; planner type inference/canonicalization; expression-related lowering; bounded prepared consumers; executor scalar evaluation | 5 | SQL parsed fields/functions to planner `Expr`; prepared contracts to planner-owned coarse families; executor evaluation on canonical contracts | 3 | 0 | 0 | Medium |
| order keys | planner order contracts plus SQL lowering normalization | 4 | SQL order normalization to shared planner order contracts | 2 | 0 | 0 | Medium-Low |
| entity/index identity | `model/entity.rs`; `model/index.rs`; SQL lowering match surfaces; SQL dispatch | 4 | SQL entity name to typed entity check; schema index metadata to runtime `IndexModel` identity | 2 | 0 | 0 | Medium-Low |

Current readout:

- Owner-count range remains effectively `2..5`.
- Boundary-count range remains `2..3`.
- No new owner drift appeared in this rerun.
- The same densest cluster remains expression forms, but the new design work
  reads as flow-targeting rather than an additional semantic owner.

## STEP 3 - Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Sites [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |
| identifier normalization | `db/sql/identifier.rs::normalize_identifier_to_scope` | stable broad consumer fanout | stable broad consumer fanout | No | 2 | Medium-Low |
| predicate normalization | planner/predicate normalization entrypoints plus truth-condition helpers | stable broad consumer fanout | stable broad consumer fanout | No | 4 | Medium |
| index key-item interpretation | typed index-key owners and runtime derivation helpers | stable broad consumer fanout | stable broad consumer fanout | No | 2 | Medium |
| route/statement classification | `db/sql/parser/mod.rs::parse_statement` plus shared route-shape contracts | stable broad consumer fanout | stable broad consumer fanout | No | 3 | Medium-Low |
| projection/expression tokenization | planner `Expr` and projection-label helpers | stable broad consumer fanout | stable broad consumer fanout | No | 4 | Medium |
| order-key normalization | planner order normalization entrypoints | stable broad consumer fanout | stable broad consumer fanout | No | 2 | Low |

Interpretation:

- The broad scans still show substantial consumer and adapter fanout.
- They still do not show a new raw-string semantic reparse authority.
- The current diff since the earlier same-day CSA reruns is not a reparse
  problem; it is still best described as sequencing and flow planning.

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

Detailed convergence notes:

- Prepared fallback still reads as bounded contract wiring rather than a
  semantic owner. Prepared family inference continues to route through
  planner-owned coarse-family inference.
- `bool_expr.rs` remains a large adjacent structural participant, but the
  current source still reads as planner-consuming truth adaptation rather than a
  confirmed independent semantic engine.
- Projection labels and route-shape contracts remain converged and typed.

## STEP 5 - Detailed Owner-Boundary Review

Evidence mode: `classified` from direct source inspection and focused scans

### 5.1 Semantic Center Still Lives In Planner

- Planner canonicalization remains centered in
  [canonicalize.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/canonicalize.rs:1).
- Planner type classification remains centered in
  [type_inference/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/type_inference/mod.rs:1).
- Truth-condition compare bridging and boolean normalization still live in the
  planner expression seam rather than being reintroduced elsewhere.

### 5.2 Predicate Is Still The Loudest Adjacent Seam

- [bool_expr.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/bool_expr.rs:1)
  remains the clearest adjacent seam.
- It still rebuilds or transports substantial truth-lane structure.
- But the current source still routes through planner-owned normalization and
  compare mapping rather than claiming an independent semantic taxonomy.
- So the CSA diagnosis remains:
  - no confirmed second owner
  - still too much adjacent machinery for the desired end state

### 5.3 Lowering And Prepared Still Read As Consumers

- Lowering still appears structurally scoped rather than semantically
  free-standing.
- Prepared-family inference in
  [prepare.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/prepare.rs:1)
  still defers to planner-owned expression-family inference for general
  expressions, searched `CASE`, and dynamic-result functions.
- That means the current prepared/session lane still reads as a consumer of
  planner-owned semantic answers, even if it remains part of the broader flow
  hotspot for velocity reasons.

### 5.4 The New Sequencing Work Does Not Change Semantic Ownership

- The newer `0.118` design direction introduces a flow-contraction thesis, not a
  new semantic thesis.
- That distinction matters for CSA:
  - it does not add a new concept family
  - it does not add a new canonical model
  - it does not add a second semantic owner
- It therefore changes prioritization, not current semantic authority state.

## STEP 6 - Drift Risk Table

Evidence mode: `semi-mechanical`

| Concept Family [M] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| truth-condition semantics inside expression forms | predicate/lowering truth-condition handling drifts from planner-owned expression meaning | `db/predicate/bool_expr.rs`, planner expression canonicalization/type inference, and truth-related lowering adapters | planner-owned expression typing and canonicalization | no confirmed second owner, but adjacent typed owners still touch the lane | equivalent `WHERE` / `HAVING` / wrapper/null-test forms could drift across surfaces | Medium-High | Medium |
| function/result-family semantics inside expression forms | expression-family classification outside planner grows faster than current contraction work | planner type inference, prepared-family consumers, expression-related lowering | planner-owned expression typing/classification | no confirmed second owner, but adjacent boundary-local family consumers remain | expression result-family drift across prepared, lowering, and planner surfaces | Medium | Medium |
| projection labels | builder/session label helpers drift semantically | structural label helpers plus builder text-projection helpers | canonical planner expression labels | presentation-only fallback label text | unstable SQL column names and confusing explain output | Medium | Medium-Low |
| index key items | schema/build/runtime representational mismatch | schema `IndexKeyItem`, runtime `IndexKeyItem`, runtime expression derivation | runtime typed key-item metadata plus expression derivation | raw display text if reused accidentally | index lookup mismatches and explain/index-key drift | Medium | Medium |
| predicates | planner/runtime/explain semantic forks | normalize + truth-condition handling + structural guard tests + explain assertions | canonical predicate/truth normalization | none observed | inconsistent query filtering and unstable fingerprints | High | Medium-Low |
| route/statement classification | public/session surfaces drift away from route-shape authority | unified query results, `LoweredSqlCommand`, route-shape contracts | parser/lowering plus route-shape contracts | none observed | wrong execution-path selection for equivalent query shapes | Medium-High | Medium-Low |
| identifiers | duplicated lowering boundaries | SQL identifier normalization plus predicate field rewrite helper | canonical identifier normalization | none observed | mismatched field resolution between SQL and fluent surfaces | Medium-High | Medium-Low |

## STEP 7 - Missing Canonical Models

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| identifiers | SQL plus fluent entry paths remain active | Yes | 2 | 2 | None | keep reduced-SQL normalization and structural field adaptation split and tested | Low |
| predicates | normalization/coercion/truth surface remains active | Yes | 2 | 4 | None | keep truth semantics contracting toward planner-owned expression normalization | Medium-Low |
| route classification | parser plus route-contract usage remains active | Yes | 2 | 3 | None | keep `RouteShapeKind` and lowered statement-mode contracts as the shared execution-shape boundary | Medium-Low |
| index key items | schema/runtime key-item metadata remains active | Yes | 1 | 4 | None | maintain schema/runtime parity tests and keep canonical text render-only | Medium-Low |
| expression forms / truth-condition lane | expression-family pressure remains active | Yes | 2 | 5 | None | centralize truth-condition semantics behind planner-owned expression typing and canonicalization | Medium |
| expression forms / result-family lane | planner result-family pressure remains active | Yes | 2 | 5 | None | keep CASE/function/result-family meaning converging on planner-owned typing/classification | Medium |

Hard gate result: no growing concept is missing a canonical typed model.

## STEP 8 - Canonical Authority Risk Index

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

Interpretation:

- The tree remains in the same low-to-moderate drift band.
- No new competing semantic authority appeared in this rerun.
- The newer sequencing around `0.118` does not change the canonical authority
  score because it is targeting flow collapse, not semantic ownership change.

## STEP 9 - Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| same-day design and audit work continues to move | visible docs churn | docs-only noise | this does not itself create a new semantic owner or remove one |
| current planning now distinguishes semantic collapse from flow collapse | clearer sequencing language | planning signal, not shipped semantic drift | this sharpens prioritization but does not alter current semantic authority |
| broad scans still show heavy expression fanout | many consumer hits remain visible | structural-but-expected fanout | the remaining concern is still adjacent flow/adapter breadth, not raw-string semantic ambiguity |

## Required Summary

0. Run metadata + comparability note
- method manifest remains `CSA-1.0`, and this rerun is comparable to the
  earlier 2026-04-22 CSA reruns.

1. Canonical concept inventory snapshot
- inventory still covers `8` core concept families, and all `8/8` retain a
  canonical typed model authority.

2. Representation matrix highlights
- canonical path completeness remains `8/8`; side-channel presence remains
  bounded to diagnostics, presentation helpers, and other non-decision paths.

3. Owner/boundary count deltas
- owner-count range remains effectively `2..5`; boundary-count range remains
  `2..3`; no new owner drift appeared in this rerun.

4. Reparse/reinterpretation findings
- no confirmed raw-string semantic reparse authority was found in the current
  tree.

5. Cross-surface convergence gaps
- no new parity gaps were introduced; prepared and session surfaces still read
  as planner-consuming expression contract boundaries rather than competing
  semantic owners.

6. Missing canonical model blockers
- missing canonical model count remains `0` for growing concept families.

7. Drift risk table (high/medium/low)
- observed-risk rows remain bounded; high-risk rows remain `0`; the active seam
  is still the expression-family/truth-condition follow-through lane, not a new
  split-brain semantic authority.

8. Canonical Authority Risk Index
- risk index remains `4.1/10` (`77/19` weighted), which keeps the current tree
  in the low-to-moderate band.

9. Noise-filter interpretation
- the main new signal is sequencing clarity: semantic centralization and flow
  collapse are now being distinguished more explicitly, but the shipped semantic
  authority picture itself remains unchanged.

10. Follow-up actions with owner boundary + target run
- No mandatory follow-up actions for this run (`index < 6` and no high-risk
  drift trigger).
- Monitoring-only: keep planner-owned expression typing and canonicalization as
  the semantic authority for truth-condition and result-family meaning.
- Monitoring-only: keep prepared-family inference dependent on planner-owned
  coarse-family inference and prevent prepared/session flow cleanup from
  reintroducing local semantic classification.
- Monitoring-only: keep `bool_expr.rs` on the adapter side of future work and
  avoid re-expanding it into an independent semantic engine during flow
  contraction.

11. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)
- method comparability status: `comparable`
- all mandatory CSA steps and tables are present in this report
- owner and boundary conclusions were grounded in inspected source boundaries,
  the layer-authority invariant script, focused source scans, and direct compile
  / test verification
- status: `PASS`

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
  - `AccessPath decision owners: 2`
  - `RouteShape decision owners: 3`
  - `Predicate coercion owners: 4`
  - `Canonicalization entrypoints: 1`
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core canonicalization_ownership_stays_in_access_and_predicate_layers -- --nocapture` -> PASS
- direct source scans and focused owner-boundary inspection -> PASS
