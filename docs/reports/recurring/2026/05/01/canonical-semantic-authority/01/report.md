# Canonical Semantic Authority Audit - 2026-05-01

## Report Preamble

- scope: semantic authority continuity across schema, parser/lowering,
  planner, runtime, EXPLAIN, fingerprint/cache, and replay-facing contracts in
  `crates/icydb-core/src`, with spot checks in `crates/icydb-schema/src` and
  `crates/icydb-schema-derive/src`
- compared baseline report path:
  `docs/audits/reports/2026-04/2026-04-22/canonical-semantic-authority-3.md`
- code snapshot identifier: `c3329642b`
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
  - top-level concept-family inventory remains unchanged from the April 22
    baseline
  - this run adds focused evidence for aggregate identity, expression-stage
    artifacts, and blob SQL/value handling as sub-surfaces under the existing
    concept families
  - owner/boundary counting rules and side-channel classification rules are
    unchanged

## Artifacts

- `docs/audits/reports/2026-05/2026-05-01/artifacts/canonical-semantic-authority/aggregate-identity-signals.txt`
- `docs/audits/reports/2026-05/2026-05-01/artifacts/canonical-semantic-authority/expression-stage-signals.txt`
- `docs/audits/reports/2026-05/2026-05-01/artifacts/canonical-semantic-authority/broad-normalization-signals.txt`
- `docs/audits/reports/2026-05/2026-05-01/artifacts/canonical-semantic-authority/explain-fingerprint-signals.txt`

Artifact line counts:

| Artifact [M] | Signal Lines [M] |
| ---- | ----: |
| aggregate identity signals | 102 |
| expression stage signals | 55 |
| broad normalization signals | 3391 |
| explain/fingerprint signals | 1987 |

## STEP 0 - Canonical Concept Inventory

Evidence mode: `semi-mechanical`

| Concept Family [M] | Primary Owner Boundary [C] | Frontend Entry Surfaces [M] | Growth Signal [M/C] | Canonical Model Exists? [C] | Current Read [C] |
| ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | `db/sql/identifier.rs` plus predicate-facing identifier adapters | SQL parser + SQL lowering identifier adaptation | stable | Yes | Identifier normalization remains ingress-owned; no later raw-string field reparse authority found. |
| predicates | `db/predicate/*` with planner expression ingress in `db/query/plan/expr/*` | SQL predicate parse + builder filter APIs | stable-to-contracting | Yes | Predicate runtime remains a typed consumer. TRUE-only admission is now explicitly owned by `truth_value`. |
| index key items | `model/index.rs`, `db/index/key/*`, `db/scalar_expr.rs` | schema-declared index metadata | stable | Yes | Runtime key derivation still uses typed `IndexKeyItem` and scalar expression contracts; `canonical_text(...)` remains render/name generation only. |
| route/statement classification | SQL parser/lowering plus query/executor route descriptors | SQL statement parse/lower + typed route shape dispatch | stable | Yes | Route and grouped dispatch now read as explicit plan artifacts rather than executor-side semantic recombination. |
| projection labels | planner expressions plus session projection label renderers | SQL projection + builder projection + explain labels | stable | Yes | Labels remain presentation-owned and do not drive execution or cache identity. |
| expression forms | planner `Expr`, `CanonicalExpr`, `TypedExpr`, `PredicateCompilation`, function registry, and aggregate identity sub-surface | SQL clauses + fluent expressions + prepared parameters + scalar function names | active but structurally improved | Yes | This remains the largest family, but stage artifacts and aggregate identity reduce the old owner-breadth risk. |
| order keys | SQL/order parser plus planner `OrderSpec` contracts | SQL `ORDER BY` + fluent sort surfaces | stable | Yes | Blob order rejection and function order admission still lower through typed expression/function contracts. |
| entity/index identity | `model/entity.rs`, `model/index.rs`, SQL entity/index match boundaries | SQL entity names + typed API generics + schema metadata | stable | Yes | Broad fanout remains usage, not competing identity ownership. |

## STEP 1 - Representation Matrix

Evidence mode: `semi-mechanical`

| Concept Family [M] | Frontend Text Inputs [M] | Canonical Token/Model/AST [C] | Schema Metadata Form [C] | Planner Form [C] | Runtime Form [C] | Replay/Recovery Form [C] | EXPLAIN/Diagnostic Form [C] | Canonical Path Complete? [C] | Side Channels Present? [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| identifiers | SQL qualified identifiers | normalized names from `normalize_identifier_to_scope(...)` and local SQL normalization | schema field names | resolved field/slot names | normalized execution field/slot references | replay does not reparse identifiers | explain renders normalized fields | Yes | No |
| predicates | SQL predicate text + fluent predicate builders | `Predicate`, planner `Expr`, `CanonicalExpr`, `PredicateCompilation` | generated index predicates enter as metadata then lower | canonical predicate/truth expression forms | compiled predicate program and TRUE-only truth contract | replay consumes prepared operations | explain consumes predicate model/fingerprint surfaces | Yes | Low |
| index key items | schema index declarations | `IndexKeyItem`, `IndexExpression`, scalar expression key programs | schema node key items | planner/access consume runtime key items | runtime derives canonical encoded key components | replay uses stored key bytes | explain renders typed index access | Yes | Low |
| route/statement classification | SQL statement text | `SqlStatement`, `LoweredSqlCommand`, route/grouped execution descriptors | n/a | lowered statement and grouped route contracts | executor matches explicit plan artifacts | replay does not classify SQL | explain reflects cached prepared plan route facts | Yes | No |
| projection labels | SQL aliases and builder selections | planner `Expr`, `FieldId`, `Alias`, projection specs | schema fields | projection spec in planned query | projection payload plus label renderers | replay is not label-driven | deterministic structural labels | Yes | Low |
| expression forms | SQL expressions/functions, aggregate terminals, fluent expressions, prepared parameters | `Expr`, `CanonicalExpr`, `TypedExpr`, `FunctionSpec`, `AggregateIdentity`, `AggregateSemanticKey` | field/type metadata | type inference, canonicalization, aggregate identity, and function registry | scalar/projection evaluation consumes compiled contracts | replay unaffected | explain/fingerprint consume canonical shape/key artifacts | Yes | Low |
| order keys | SQL order terms + fluent sort calls | `OrderSpec` and function order shapes | schema field validation | normalized planner order contracts | executor consumes order contracts | replay ordering is not reparsed | explain/fingerprint consume same order contract | Yes | No |
| entity/index identity | SQL entity/index text | `EntityModel`, `IndexModel` | schema declarations | planner resolves model contracts | runtime executes typed model identity | replay applies stored typed operations | diagnostics render stable names | Yes | Low |

Representation readout:

- canonical path completeness remains `8/8`.
- missing canonical model count remains `0`.
- side-channel authority count remains `0`; diagnostic/presentation channels are
  still classified as non-authoritative.

## STEP 2 - Authority Count

Evidence mode: `semi-mechanical`

| Concept Family [M] | Semantic Owner Modules [M] | Owner Count [D] | Lowering Boundaries [M] | Boundary Count [D] | Canonical Bypass Paths [M] | Owner Drift vs Baseline [D] | Risk [C] |
| ---- | ---- | ----: | ---- | ----: | ----: | ----: | ---- |
| identifiers | `db/sql/identifier.rs`; predicate-facing field rewrite adapters | 2 | SQL parser/lowering identifier normalization; predicate field rewrite adapter | 2 | 0 | 0 | Medium-Low |
| predicates | `db/predicate/normalize.rs`; `db/predicate/semantics.rs`; planner `expr::truth_value`; predicate compile bridge | 4 | SQL predicate ingress; planner canonical expression ingress | 2 | 0 | 0 | Medium-Low |
| index key items | `model/index.rs`; `schema/node/index.rs`; `db/index/key/*`; `db/scalar_expr.rs` | 4 | schema key-item metadata to runtime model; runtime scalar expression to derived key value | 2 | 1 | 0 | Medium |
| route/statement classification | SQL parser/lowering; query plan route/grouped descriptors; executor route-shape consumers | 3 | parser statement classification to lowering; lowering/plan artifacts to route shape | 2 | 0 | 0 | Medium-Low |
| projection labels | planner expression AST; builder text-projection helpers; session label renderers; explain adapters | 4 | planner projection AST to outward labels; builder/session/explain structural label rendering | 2 | 1 | 0 | Medium-Low |
| expression forms | planner `Expr`/stage artifacts; function registry; aggregate identity/key; expression-related lowering; bounded executor evaluation | 5 | SQL parsed expressions to planner `Expr`; prepared contracts to planner coarse families; executor evaluation over compiled contracts | 3 | 0 | 0 | Medium-Low |
| order keys | planner order contracts plus SQL order lowering/parser support | 4 | SQL order normalization to planner order contracts | 2 | 0 | 0 | Medium-Low |
| entity/index identity | `model/entity.rs`; `model/index.rs`; SQL route match surfaces; runtime typed model consumers | 4 | SQL entity name to typed entity check; schema index metadata to runtime `IndexModel` identity | 2 | 0 | 0 | Medium-Low |

Current deltas:

- owner-count range remains `2..5`.
- boundary-count range remains `2..3`.
- canonical bypass paths remain `2` total and are classified as diagnostic or
  representation-adapter surfaces, not semantic side-channel authorities.
- aggregate identity is now stronger than the April 22 baseline: global and
  grouped aggregates share `AggregateIdentity` / `AggregateSemanticKey`.

## STEP 3 - Reparse / Reinterpretation Scan

Evidence mode: `mechanical` with `classified` triage

| Concept Family [M] | Canonical Parse/Normalize Site [M] | Total Parse/Normalize Signal Lines [M] | Non-Canonical Sites [D] | Reparse From Raw String? [C] | Duplicated Matcher/Normalizer Families [M] | Drift Risk [C] |
| ---- | ---- | ----: | ----: | ---- | ----: | ---- |
| identifier normalization | `db/sql/identifier.rs::normalize_identifier_to_scope` and SQL normalization helpers | 240 | 0 confirmed | No | 2 | Medium-Low |
| predicate normalization | predicate normalize + planner canonical expression/truth boundaries | 3362 | 0 confirmed | No | 4 | Medium-Low |
| index key-item interpretation | runtime `IndexKeyItem` and canonical index key encode/build modules | 484 | 1 render/name-generation side channel | No | 2 | Medium |
| route/statement classification | SQL parser `parse_statement` plus typed route/grouped descriptors | 574 | 0 confirmed | No | 3 | Medium-Low |
| projection/expression tokenization | planner `Expr`, function registry, aggregate identity, label helpers | 2900 | 0 confirmed | No | 4 | Medium-Low |
| order-key normalization | planner `OrderSpec` plus SQL order parser/lowering support | 2800 | 0 confirmed | No | 2 | Low |

Focused findings:

- `AggregateIdentity::normalize_distinct_for_kind(...)` is called outside
  `identity.rs` only by grouped aggregate hashing, which delegates to the
  canonical owner instead of reimplementing the rule.
- No `MIN/MAX DISTINCT` stripping logic was found outside the identity owner
  and tests.
- SQL blob literals are parsed once at ingress into `Value::Blob`; later blob
  equality, hashing, storage, and `OCTET_LENGTH` operate on typed value or
  field-kind contracts.

## STEP 4 - Cross-Surface Convergence

Evidence mode: `classified` anchored by mechanical surface mapping

| Concept Family [M] | Builder/Fluent Path [M] | SQL/Frontend Path [M] | Schema Lowering Path [M] | Planner Owner [C] | Runtime Owner [C] | Replay/Recovery Owner [C] | EXPLAIN Source [C] | Converged to One Canonical Model? [C] | Parity Gaps Count [D] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| identifiers | fluent field references | SQL identifier normalization | schema field names | planner consumes normalized fields | runtime consumes resolved slots | replay does not reparse | explain uses normalized names | Yes | 0 | Low |
| predicates | fluent filter builders | SQL predicate lowering | index predicate metadata | canonical predicate/expression truth model | compiled predicates + TRUE-only truth contract | prepared operations | predicate/fingerprint surfaces | Yes | 0 | Medium-Low |
| index key items | typed model usage | SQL/index diagnostics only | schema key-item declarations | planner consumes runtime key items | runtime derives canonical key bytes | stored key bytes | typed index access renderers | Yes | 1 | Medium |
| route/statement classification | fluent descriptors | SQL `LoweredSqlCommand` + structural query routes | n/a | plan route/group descriptors | executor matches explicit route artifacts | no SQL reclassification | cached prepared-plan explain | Yes | 0 | Medium-Low |
| projection labels | fluent projection selection | SQL aliases/session labels | schema field names | planner `Expr` / `Alias` | projection payload | not label-driven | structural labels | Yes | 1 | Medium-Low |
| expression forms | fluent expressions | SQL expressions/functions/aggregates/blob literals | schema type metadata | `Expr`, `CanonicalExpr`, `TypedExpr`, `FunctionSpec`, `AggregateIdentity` | compiled scalar/projection evaluation | unaffected | canonical expression/fingerprint shape | Yes | 1 | Medium-Low |
| order keys | fluent sort calls | SQL order terms | schema fields | planner order contracts | executor ordering contracts | not reparsed | explain/fingerprint order contract | Yes | 0 | Low |
| entity/index identity | typed generics | SQL entity/index names | schema declarations | model contracts | typed runtime identity | stored model operations | diagnostics render names | Yes | 0 | Low |

Convergence notes:

- SQL global aggregate semantics and grouped aggregate semantics now converge on
  `AggregateIdentity` plus filter-aware `AggregateSemanticKey`.
- Expression-stage flow has explicit artifacts. Some broader APIs still expose
  `Expr`, but the owned stage artifacts exist and downstream contracts no
  longer need to invent parallel semantic carriers.
- SQL blob support added one frontend syntax family, but its canonical model is
  existing `Value::Blob` plus planner `Function::OctetLength`/`ExprType::Blob`.

## STEP 5 - Drift Risk Table

Evidence mode: `semi-mechanical`

| Concept Family [M] | Drift Trigger [C] | Current Surface [M/C] | Canonical Authority [C] | Competing Authority [C] | User Impact if Drift Activates [C] | Detection Confidence [C] | Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| expression truth semantics | predicate compile or projection evaluation redefines TRUE/NULL handling | `truth_value`, `predicate_compile`, `projection_eval`, compiled grouped filters | `expr::truth_value` for evaluated values plus canonical boolean expression artifacts for shape | none confirmed | `WHERE`, `HAVING`, `CASE`, and aggregate `FILTER` could diverge | High | Medium-Low |
| aggregate identity | hash/dedup/runtime reinterprets DISTINCT or filter equality | `AggregateIdentity`, `AggregateSemanticKey`, grouped specs, SQL global semantics, fingerprint hashing | `query::plan::semantics::identity` | none confirmed; grouped hash delegates to owner | `MIN(DISTINCT x)` and `MIN(x)` could split cache/dedup behavior, or filtered aggregates could alias | High | Low |
| scalar function taxonomy | parser/order/lowering/eval ladders diverge as functions grow | SQL parser function list, planner function registry, type inference, projection evaluation | planner `Function` + `FunctionSpec` / shape enums | parser syntax tables before lowering | unsupported/accepted function mismatch or wrong type/result semantics | Medium-High | Medium-Low |
| blob values | SQL blob syntax or byte-length handling becomes separate from value semantics | SQL lexer/parser, `Value::Blob`, value compare/hash, `OCTET_LENGTH`, field-kind typing | value layer for representation; planner function registry for byte-length expression semantics | none confirmed | byte comparison/order/hash drift or payload materialization surprises | Medium-High | Low |
| index key items | schema/build/runtime representational mismatch | schema `IndexKeyItem`, derive `canonical_text`, runtime key build/encode | runtime typed key item + canonical key encoding | name-generation text if reused as logic | index lookup/key drift | Medium | Medium |
| projection labels | label helpers accidentally drive identity | session label renderers, builder label text, explain renderers | planner projection expression/alias model | presentation text | unstable or misleading output labels | Medium | Medium-Low |
| route/statement classification | execution path re-derived from raw flags instead of route descriptors | structural query route/grouped descriptors, session/executor entrypoints | query plan and prepared route artifacts | none confirmed | wrong execution path or explain/hash mismatch | High | Low |

## STEP 6 - Missing Canonical Models

Evidence mode: `classified` with mechanical support

| Concept Family [M] | Feature Growth Signal [M/C] | Canonical Typed Model Present? [C] | Frontend Paths Count [M] | Semantic Owners Count [D] | Blocking Gap [C] | Required Canonicalization Action [C] | Risk [C] |
| ---- | ---- | ---- | ----: | ----: | ---- | ---- | ---- |
| identifiers | stable SQL/fluent field paths | Yes | 2 | 2 | None | keep ingress normalization and field rewrite adapter split tested | Low |
| predicates | truth/filter paths active | Yes | 2 | 4 | None | keep truth-value policy and predicate compilation aligned | Medium-Low |
| index key items | schema/runtime key-item metadata active | Yes | 1 | 4 | None | keep `canonical_text(...)` render/name-generation only | Medium-Low |
| route classification | SQL/fluent/session route surfaces active | Yes | 2 | 3 | None | keep grouped/execution route descriptors as the boundary | Low |
| expression forms / functions | numeric/text/blob scalar function growth | Yes | 2 | 5 | None | keep planner `FunctionSpec` as canonical function registry | Medium-Low |
| expression forms / aggregates | global/grouped aggregate identity growth | Yes | 2 | 1 for DISTINCT identity | None | keep all aggregate equality/hash/dedup through `AggregateIdentity` / `AggregateSemanticKey` | Low |
| blob values | new SQL blob literals and byte-length function | Yes | 1 SQL syntax plus typed value callers | 2 | None | keep blob representation in `Value::Blob`; keep SQL hex as ingress-only syntax | Low |

Hard gate result: missing canonical model count is `0`, so there is no high-risk
missing-model blocker in this run.

## STEP 7 - Canonical Authority Risk Index

Evidence mode: `semi-mechanical`

| Area [M] | Score (1-10) [C] | Weight [M] | Weighted Score [D] |
| ---- | ----: | ----: | ----: |
| semantic owner multiplicity | 5 | 3 | 15 |
| lowering-boundary multiplicity | 5 | 3 | 15 |
| raw-string/side-channel authority | 3 | 3 | 9 |
| reparse/normalizer duplication | 4 | 2 | 8 |
| cross-surface parity gaps | 4 | 2 | 8 |
| missing canonical models in growing concepts | 1 | 3 | 3 |
| replay/live semantic mismatch | 2 | 3 | 6 |

`overall_index = 64 / 19 = 3.4`

Interpretation:

- The tree is now at the top of the low-risk band, down from the April 22
  `4.1` low-to-moderate baseline.
- The aggregate identity and expression-stage sealing work reduced the clearest
  semantic drift risks.
- Remaining risk is mostly breadth: expression/function/predicate consumers are
  still numerous, but they now mostly point at named owner artifacts.

## STEP 8 - Noise Filter

Evidence mode: `classified`

| Signal [M/C] | Raw Trend [M/D] | Noise Classification [C] | Adjusted Interpretation [C] |
| ---- | ---- | ---- | ---- |
| aggregate identity signal lines | 102 focused hits | structural improvement | new hits represent shared identity adoption, not new semantic owners |
| expression-stage signal lines | 55 focused hits | structural improvement | stage artifacts reduce drift even if `Expr` remains exposed at broader APIs |
| broad normalization signal lines | 3391 broad hits | expected consumer fanout | most hits are consumers/tests/adapters; no raw-string semantic reparse authority confirmed |
| explain/fingerprint signal lines | 1987 broad hits | expected consumer fanout | fingerprint/explain now include more canonical-artifact references, not more owners |
| SQL blob support | new parser/value/function hits | new ingress syntax, not new value owner | blob literals lower to `Value::Blob`; `OCTET_LENGTH` uses planner function identity |

## Required Summary

0. Run metadata + comparability note
- method manifest remains `CSA-1.0`, with `0` method-tag changes and a
  comparable top-level concept inventory.

1. Canonical concept inventory snapshot
- inventory remains `8` concept families, and `8/8` retain a canonical typed
  model.

2. Representation matrix highlights
- canonical path completeness remains `8/8`; side-channel authority count is
  `0`.

3. Owner/boundary count deltas
- owner-count range remains `2..5`; boundary-count range remains `2..3`; no
  owner drift was confirmed versus baseline.

4. Reparse/reinterpretation findings
- broad scan artifacts contain `5535` signal lines; confirmed raw-string
  semantic reparse sites remain `0`.

5. Cross-surface convergence gaps
- parity gaps count is `3` low/medium classified gaps: index key item
  render/name-generation, projection labels, and broad expression surface fanout.

6. Missing canonical model blockers
- missing canonical typed model count is `0`; no growing concept fails the hard
  gate.

7. Drift risk table
- highest current classified risk is `Medium` for index key item schema/runtime
  parity; aggregate identity, route classification, and blob values are `Low`.

8. Canonical Authority Risk Index
- risk index is `3.4/10`, which is low-risk and improved from the April 22
  baseline `4.1/10`.

9. Noise-filter interpretation
- current movement is mostly canonical-artifact adoption and consumer fanout,
  not semantic-owner multiplication.

10. Follow-up actions
- No mandatory follow-up is required by the CSA rule because the risk index is
  `< 6` and no high-risk drift trigger was confirmed.
- Monitoring-only: keep `AggregateIdentity::normalize_distinct_for_kind(...)`
  as the only aggregate DISTINCT normalizer; delegate-only calls are acceptable.
- Monitoring-only: keep SQL blob syntax as ingress-only and avoid any later
  raw hex/string interpretation after lowering to `Value::Blob`.

11. Verification Readout
- method comparability status: `comparable`
- mandatory tables present: yes
- owner and boundary counts derived from inspected source surfaces and focused
  scans, not mention counts only: yes
- explicit status: `PASS`
