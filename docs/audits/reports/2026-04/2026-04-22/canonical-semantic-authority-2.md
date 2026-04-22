# Canonical Semantic Authority Audit - 2026-04-22 (Detailed Rerun)

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering,
  planner, runtime, EXPLAIN, and replay-facing contracts in
  `crates/icydb-core/src`, with secondary spot checks in `crates/icydb/src`,
  `crates/icydb-build/src`, `crates/icydb-schema/src`, and
  `crates/icydb-schema-derive/src`
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-22/canonical-semantic-authority.md`
- code snapshot identifier: `9130b8756e`
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
    boundary-count rules, and reparse/convergence rules as the earlier
    2026-04-22 baseline
  - the inspected owner boundaries did not show a new competing semantic
    authority in the current tree
  - this rerun adds deeper owner-boundary evidence rather than changing the
    scoring model

## STEP 0 - Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Detailed Read [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs` plus predicate-facing identifier adapters | SQL parser + SQL lowering identifier adaptation | stable vs same-day baseline | Yes | `normalize_identifier_to_scope(...)` and `rewrite_field_identifiers(...)` remain explicit reduced-SQL normalization helpers, with comments that they do not own parser policy or execution semantics. |
| predicates | `db/predicate/*` with planner ingress in `db/query/plan/*` | SQL predicate parse + builder filter APIs | stable owner surfaces; no new predicate semantic owner introduced | Yes | Predicate ownership is still typed and canonical, but the active seam remains the truth-condition lane where predicate helpers adapt planner expression semantics rather than fully disappearing. |
| index key items | `model/index.rs`, `db/index/key/*`, `db/scalar_expr.rs` | schema-declared index metadata | stable representational spread | Yes | `IndexKeyItem` remains the canonical typed key-item contract; `canonical_text(...)` still reads as diagnostics/display rather than execution authority. |
| route/statement classification | `db/sql/parser/*`, `db/sql/lowering/mod.rs`, `db/executor/planning/route/contracts/shape.rs` | SQL statement parse/lower + route shape dispatch | stable | Yes | Statement shape still lowers once into shared route contracts, with `RouteShapeKind` as the stable semantic execution-shape enum. |
| projection labels | planner expression AST plus session projection label renderers | SQL projection + builder projection + session projection label rendering | stable | Yes | Label helpers remain outward naming policy, not semantic type/classification owners. |
| expression forms | planner expression AST, type inference, canonicalization, bounded lowering/prepared consumers | SQL clauses + fluent builder expressions + prepared parameter contracts | active structural pressure remains here | Yes | The canonical AST and coarse-family/type inference are present and explicit, but expression-family ownership is still broader than the desired single-owner shape. |
| order keys | `db/sql/lowering/mod.rs`, `db/query/plan/*` | SQL `ORDER BY` + fluent sort surfaces | stable | Yes | Order normalization remains converged on typed planner contracts rather than duplicated text handling. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity/index match boundaries | SQL entity route and typed entity/index model surfaces | stable | Yes | Fanout remains broad usage, not competing semantic ownership. |

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

Detailed interpretation:

- The audited tree still has one canonical typed model per concept family.
- The active issue remains owner multiplicity inside the expression lane, not a
  missing canonical representation.
- No audited concept currently requires raw-string text to be reparsed after its
  canonical typed handoff.

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

Detailed boundary notes:

- The identifier lane remains narrow and healthy. The comments in
  `db/sql/identifier.rs` explicitly declare a reduced-SQL normalization
  boundary, and the helper only rewrites field strings while preserving
  predicate semantics.
- The planner expression lane remains the densest semantic cluster. The AST
  (`FieldId`, `Alias`, `BinaryOp`, `Function`, `CaseWhenArm`) and the typing
  seam (`ExprType`, `ExprCoarseTypeFamily`, `infer_expr_type(...)`) clearly
  exist, but adjacent layers still consume those semantics in ways that keep
  owner count broader than ideal.
- Predicate boolean handling is more disciplined than an independent semantic
  fork: `db/predicate/bool_expr.rs` imports planner-owned boolean-normalization
  helpers and routes predicates through that shared seam before rebuilding the
  canonical runtime predicate tree. This is good convergence, but it still
  marks the truth lane as the clearest contraction target.
- Lowering remains mostly structural. The module-level contract in
  `db/sql/lowering/mod.rs` still states that lowering does not own parser
  tokenization, planner validation policy, or executor semantics.

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

- The broad scans still show many consumers and adapters.
- Those scans do not show a new raw-string semantic reparse authority.
- The important distinction is that broad fanout is not automatically semantic
  duplication. In the current tree, most of that fanout still reads as typed
  consumption, rendering, or structural adaptation.

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

- Prepared fallback remains bounded rather than semantically free-standing. In
  `db/sql/lowering/prepare.rs`, prepared-family inference for general
  expressions, searched `CASE`, and dynamic-result functions routes back through
  planner-owned coarse-family inference (`infer_expr_coarse_family(...)`,
  `infer_case_result_exprs_coarse_family(...)`,
  `infer_dynamic_function_result_exprs_coarse_family(...)`).
- This is significant for canonical semantic authority because prepared SQL is
  no longer the main ambiguous owner in the expression lane. It now reads as a
  planner consumer with some local contract wiring.
- Projection labels remain converged and presentation-owned. The session label
  renderer derives outward names from planner expressions and aliases instead of
  reclassifying expression meaning.
- Route-shape convergence remains strong. `RouteShapeKind` still acts as the
  planner-to-router semantic execution-shape contract, which prevents SQL
  statement classification from splintering inside execution code.

## STEP 5 - Detailed Owner-Boundary Review

Evidence mode: `classified` from direct source inspection

### 5.1 Identifier Semantics

- `db/sql/identifier.rs` still reads as a clean canonical authority for reduced
  SQL identifier normalization.
- `normalize_identifier_to_scope(...)` only strips a qualifier when the current
  entity scope matches by tail equivalence.
- `rewrite_field_identifiers(...)` is explicitly structural. Its doc comment
  says predicate shape, compare operators, literals, and coercions are
  preserved while only field strings are transformed.
- This is the shape CSA wants: one semantic rule owner plus boundary-local
  structural adapters.

### 5.2 Planner Expression Semantics

- `db/query/plan/expr/ast.rs` still provides the canonical typed expression
  model:
  - `FieldId` for canonical field identity
  - `Alias` for presentation metadata
  - `UnaryOp` / `BinaryOp` for admitted operator taxonomy
  - `Function` for the bounded scalar-function taxonomy
  - `CaseWhenArm` for planner-owned searched-`CASE` structure
- `db/query/plan/expr/type_inference/mod.rs` still provides the strongest
  explicit evidence of planner semantic authority:
  - `ExprType`
  - `ExprCoarseTypeFamily`
  - `infer_expr_type(...)`
  - `infer_expr_coarse_family(...)`
  - `infer_case_result_exprs_coarse_family(...)`
  - `infer_dynamic_function_result_exprs_coarse_family(...)`
- The module comments are important: they explicitly say this seam owns
  deterministic planner expression type classes and does not own runtime
  execution behavior.
- This means the canonical model is present and the intended semantic owner is
  already visible in code shape.

### 5.3 Predicate / Truth Lane

- `db/predicate/bool_expr.rs` remains the clearest adjacent semantic seam.
- The file no longer reads as a fully independent truth owner. It imports
  planner-owned helpers such as `normalize_bool_expr`,
  `truth_condition_compare_binary_op(...)`, and
  `truth_condition_binary_compare_op(...)`.
- `canonicalize_predicate_via_bool_expr(...)` routes a runtime predicate
  through planner-owned boolean expression normalization and then rebuilds the
  runtime predicate form.
- That is materially better than having a separate predicate-local truth engine.
- Even so, the file still contains substantial truth-lane structural shaping:
  predicate-to-expression conversion, compare-family adaptation, membership
  collapse, function-shell construction, and rebuild logic.
- The CSA read here is precise:
  - there is not a confirmed second semantic authority
  - there is still more adjacent truth-lane machinery than the desired
    single-owner end state
  - this is why `0.116` is the natural next contraction target

### 5.4 SQL Lowering

- `db/sql/lowering/mod.rs` still declares a frontend-only translation boundary.
- The current tree does not show lowering becoming a broad second planner.
- The remaining semantic pressure is narrower:
  - truth-shaped adapters
  - prepared template shapes
  - some local family wiring needed for prepared parameter contracts
- The prepared-family helpers in `db/sql/lowering/prepare.rs` are now
  noteworthy mainly because they defer to planner-owned coarse-family inference.
- That is an architectural improvement from a CSA perspective. The prepared
  lane now reads as bounded contract wiring around planner-owned expression
  meaning instead of a competing semantic authority.

### 5.5 Session / Presentation

- `db/session/sql/projection/labels.rs` remains clearly presentation-owned.
- The module comments explicitly place the boundary at outward SQL projection
  naming policy.
- `projection_label_from_expr(...)` renders labels from planner expressions. It
  does not classify result families, truth meaning, or planner legality.
- This lane remains a good example of acceptable side-channel logic: outward
  naming and debug annotation without semantic ownership drift.

### 5.6 Route and Index Contracts

- `db/executor/planning/route/contracts/shape.rs` still provides a stable
  route-shape contract.
- `RouteShapeKind` remains typed, compact, and explicit, which continues to
  prevent statement-shape meaning from leaking into ad-hoc executor branching.
- `model/index.rs` still provides the canonical typed key-item model.
- `IndexKeyItem::canonical_text(...)` remains render-only and therefore does
  not currently read as a semantic drift source.
- The main remaining watchpoint here is still schema/runtime parity, not a
  missing canonical authority.

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

- The tree remains in the low-to-moderate drift-pressure band.
- No new competing semantic authority appeared in this rerun.
- The more detailed read does not change the score. It increases confidence in
  the diagnosis:
  - the canonical model is present
  - the prepared lane is no longer the clearest authority problem
  - the live seam is now the expression-family cluster, with truth-condition
    semantics still the highest-leverage first contraction

## STEP 9 - Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| High non-canonical site counts in broad pattern scans | broad consumer/adapter fanout still dominates scan output | structural-but-expected fanout | Most hits are downstream consumers or boundary-local adapters, not competing semantic authorities. |
| Same-day design-doc work around `0.116` and `0.117` is present in the tree | explicit design planning for expression contraction is now clearer | planning signal, not shipped semantic drift | The docs sharpen follow-through targeting, but do not by themselves add or remove semantic owners. |
| Prepared family helpers still appear in broad scans | broad scan can over-read prepared helper references as semantic duplication | bounded consumer noise | The current code shows prepared fallback routing through planner-owned coarse-family inference rather than standing up a parallel type authority. |

## Required Summary

0. Run metadata + comparability note
- method manifest remains `CSA-1.0`, and this detailed rerun is comparable to
  the earlier 2026-04-22 baseline.

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
- no new parity gaps were introduced; prepared fallback continues to read as a
  bounded planner consumer rather than a separate semantic owner.

6. Missing canonical model blockers
- missing canonical model count remains `0` for growing concept families.

7. Drift risk table (high/medium/low)
- observed-risk rows remain bounded; high-risk rows remain `0`; the clearest
  first contraction target remains the truth-condition lane inside the broader
  expression-family cluster.

8. Canonical Authority Risk Index
- risk index remains `4.1/10` (`77/19` weighted), which keeps the current tree
  in the low-to-moderate band.

9. Noise-filter interpretation
- the main new signal in this rerun is confidence, not a new state change: the
  code still shows one canonical typed model per audited concept family, and
  the active follow-through seam is now more clearly narrowed to planner-owned
  expression authority.

10. Follow-up actions with owner boundary + target run
- No mandatory follow-up actions for this run (`index < 6` and no high-risk
  drift trigger).
- Monitoring-only: keep planner-owned expression typing and canonicalization as
  the semantic authority for the truth-condition lane, especially across
  `db/predicate/*`, planner expression owners, and truth-related lowering
  adapters.
- Monitoring-only: keep prepared-family inference dependent on planner-owned
  coarse-family inference and prevent prepared fallback from re-expanding into a
  semantic owner.
- Monitoring-only: keep schema/runtime `IndexKeyItem` parity and
  `canonical_text(...)` render-only status in the next CSA run.
- Monitoring-only: keep builder/session projection-label helpers
  presentation-only, and do not let outward label fallback text become a
  decision input.

11. Verification Readout (`PASS` / `FAIL` / `BLOCKED`)
- method comparability status: `comparable`
- all mandatory CSA steps and tables are present in this report
- owner and boundary conclusions were grounded in inspected source boundaries,
  the layer-authority invariant script, direct source scans, and focused
  compile/test verification
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
