# DRY Consolidation Audit - 2026-05-03

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded unless explicitly called out)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-30/dry-consolidation.md`
- rerun basis: current working tree after index-store handle naming/readers edits
- code snapshot identifier: `0eaa42bf5` with dirty working tree at scan time
- method tag/version: `DRY-1.2`
- method manifest:
  - `method_version = DRY-1.2`
  - `duplication_taxonomy = DT-1`
  - `owner_layer_taxonomy = OL-1`
  - `invariant_role_model = IR-1`
  - `facade_inclusion_rule = FI-1`
  - `consolidation_safety_model = CS-1`
- comparability status: `comparable` by method, snapshot-qualified because the working tree was dirty

## Evidence Artifacts

- `docs/audits/reports/2026-05/2026-05-03/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-05/2026-05-03/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`

## STEP 0 - Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-04/2026-04-30/dry-consolidation.md` | same | none | yes |
| method version | `DRY-1.2` | `DRY-1.2` | none | yes |
| duplication taxonomy | `DT-1` | `DT-1` | none | yes |
| owner-layer taxonomy | `OL-1` | `OL-1` | none | yes |
| invariant role model | `IR-1` | `IR-1` | none | yes |
| facade inclusion rule | `FI-1` | `FI-1` | none | yes |
| consolidation safety model | `CS-1` | `CS-1` | none | yes |
| in-scope roots | `crates/icydb-core/src` | same | none | yes |
| exclusions | tests/bench/examples/generated | same | none | yes |
| snapshot state | clean tree at `d07575370` | dirty tree at `0eaa42bf5` | working-tree edits included | method comparable, snapshot qualified |

## STEP 1A - Structural Duplication Scan

Evidence mode: `mechanical`

| Pattern [M] | Files [M] | Lines [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Safety Critical? [C] | Behavioral Equivalence Confidence [C] | Drift Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression IR vs planner-to-IR lowering | `db/query/plan/expr/compiled_expr/mod.rs`; `db/query/plan/expr/compiled_expr/compile.rs`; `db/query/plan/expr/compiled_expr/evaluate.rs`; `db/query/plan/expr/scalar.rs` | `compiled_expr/mod.rs:34-554`; `compile.rs:15-338`; `evaluate.rs:32-701`; `scalar.rs:160-303` | Intentional boundary duplication | yes | yes | yes | yes | high | low | low |
| predicate capability classification + consumption | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | `predicate/capability.rs:122-334`; `predicate/runtime/mod.rs:19-20,215-216`; `index/predicate/compile.rs:14,78-85,173-180,319` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | `cursor/mod.rs:58-172`; `cursor/spine.rs:164-331`; `query/plan/continuation.rs:21-573`; `executor/planning/continuation/scalar.rs:30-214` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | `route/capability.rs:75-310`; `route/contracts/capabilities.rs:24`; `hints/load.rs:18-150`; `hints/aggregate.rs:20-108` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope + size guards | `db/commit/store/mod.rs`; `db/commit/marker.rs` | `commit/store/mod.rs:53-363`; `commit/marker.rs:247-571` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| index-store reader bridge and preflight overlay access | `db/index/readers.rs`; `db/registry/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/executor/mutation/commit_window.rs`; `db/commit/prepare.rs` | `index/readers.rs:85-157`; `registry/readers.rs:29-51`; `runtime_context/index_readers.rs:54-116`; `commit_window.rs:300-394`; `commit/prepare.rs:128-170` | Boilerplate duplication | no | yes | yes | yes | high | low-medium | low-medium |
| fluent non-paged terminal public wrappers | `db/query/fluent/load/terminals.rs` | `terminals.rs:600-1040` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |
| SQL projection/result finalization shell | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs`; `db/session/sql/compiled.rs` | `execute/mod.rs:116-242,351-504`; `sql/mod.rs:882-903`; `compiled.rs:47-67` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |

## STEP 1B - Closed High-Risk Seam Verification

Evidence mode: `mechanical`

| Former Risk [M] | Current Evidence [M] | Classification [C] | Residual Risk [C] |
| ---- | ---- | ---- | ---- |
| row/scalar projection evaluating `ScalarProjectionExpr` directly at runtime | `rg "ScalarProjectionExpr\|eval_scalar_projection_expr\|eval_canonical_scalar_projection_expr\|ScalarProjectionExpr::\|match expr" crates/icydb-core/src/db/executor -n` returns only projection tests plus `expr.evaluate(&reader)` adapter calls in `db/executor/projection/eval/scalar.rs` | closed in production | low |
| duplicate scalar operator evaluator under executor projection | `rg "GroupedCompiledExpr\|GroupedProjectionExpr\|eval_grouped_projection_expr\|eval_binary_expr\|eval_unary_expr\|projection/eval/operators\|ScalarProjectionExpr" crates/icydb-core/src/db -g '!**/tests/**' -g '!**/tests.rs' -n` returns planner compile surfaces only for `ScalarProjectionExpr` | closed | low |
| runtime expression paths bypassing `CompiledExpr::evaluate` | production `rg "\.evaluate\(" crates/icydb-core/src/db/executor crates/icydb-core/src/db/query/plan/expr -g '!**/tests/**' -g '!**/tests.rs' -n` shows executor projection/grouped/aggregate paths calling `CompiledExpr::evaluate` through reader adapters | closed | low |

## STEP 2A - Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression evaluation vs compile lowering | 3 coordinated surfaces | planner scalar compile tree, compile lowering, compiled evaluator | no | yes | yes | yes | yes (`CompiledExpr::evaluate`, with lowering in `compiled_expr/compile.rs`) | high | low | keep split between compile and evaluate modules | low |
| predicate capability classification | 3 | predicate authority, runtime compilation, index predicate compile | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 4 | cursor, planner continuation, executor continuation, session transports | yes | no | yes | yes | yes (`db/cursor/mod.rs` plus planner contract) | high | medium | cursor/planner continuation boundary | medium-low |
| route capability snapshot propagation | 4 | route capability, route contracts, load hints, aggregate hints | yes | no | yes | yes | yes (`db/executor/planning/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store/mod.rs`) | high | low | commit store boundary | low-medium |
| index-store reader bridge and preflight overlay access | 5 | index reader trait, registry handle reader, runtime context reader, commit preflight overlay, commit preparation | yes | no | yes | yes | yes (`db/index/readers.rs` trait boundary) | high | medium | keep reader contract in index; keep overlay in mutation commit-window | low-medium |
| fluent non-paged terminal public wrappers | 1 owner family | fluent load terminal public methods and projection output shaping | no | yes | yes | no | yes (`db/query/fluent/load/terminals.rs`) | high | low | fluent load boundary | low-medium |
| SQL projection execution and response finalization shells | 1 owner family | SQL session compile contract, plan-cache binding, statement-result shaping | no | yes | yes | no | yes (`db/session/sql`) | high | low | session SQL boundary | low-medium |

## STEP 3A - Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/query/plan/expr/compiled_expr/mod.rs` | `554` | `1` | yes | boundary-owned growth | expression taxonomy, reader trait, and IR shape remain the single compiled-expression contract | medium |
| `crates/icydb-core/src/db/query/plan/expr/compiled_expr/evaluate.rs` | `701` | `1` | yes | boundary-owned growth | evaluator owns runtime expression semantics; production executor paths still delegate to it | medium |
| `crates/icydb-core/src/db/query/plan/expr/compiled_expr/compile.rs` | `465` | `1` | yes | boundary-owned growth | planner-to-IR lowering is intentionally split from evaluator semantics | medium |
| `crates/icydb-core/src/db/executor/projection/eval/scalar.rs` | `494` | `0` | no | contraction-complete | executor scalar projection remains a reader-adapter surface around `CompiledExpr::evaluate` | low |
| `crates/icydb-core/src/db/query/fluent/load/terminals.rs` | `1073` | `1` | yes | partially-compressed | remaining repetition is public method shape, slot resolution, and output conversion | low-medium |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | `668` | `1` | yes | safety-neutral | cached and bypass paths still repeat prepared-plan/projection-contract shells | low-medium |
| `crates/icydb-core/src/db/predicate/runtime/mod.rs` | `1034` | `1` | yes | safety-neutral | runtime predicate evaluation remains dense, but capability duplication is subordinate to predicate authority | low-medium |
| `crates/icydb-core/src/db/commit/store/mod.rs` | `363` | `1` | no | safety-neutral | stable envelope validation and store orchestration remain boundary-protected | low-medium |
| `crates/icydb-core/src/db/commit/marker.rs` | `574` | `1` | no | safety-neutral | marker payload codec owns row-op shape and length checks distinct from store envelope persistence | low-medium |
| `crates/icydb-core/src/db/executor/mutation/commit_window.rs` | `1027` | `2` | no | boundary-bridge growth | preflight overlay repeats index-reader bridge plumbing and single/multi-row guard collection; repetition is local to commit-window safety | medium-low |
| `crates/icydb-core/src/db/index/readers.rs` | `178` | `1` | no | boundary-bridge | trait bridge centralizes typed and structural index reader contracts | low-medium |
| `crates/icydb-core/src/db/executor/runtime_context/index_readers.rs` | `121` | `1` | no | boundary-bridge | runtime context implements the index reader contract with direct store access | low-medium |
| `crates/icydb-core/src/db/registry/readers.rs` | `61` | `1` | no | boundary-bridge | store handles implement the structural reader contract for recovery/preflight readers | low-medium |
| `crates/icydb-core/src/db/executor/planning/route/hints/load.rs` + `aggregate.rs` | `292` | `1` | yes | safety-neutral | bounded-fetch and capability-hint helpers repeat small same-owner call shells | low-medium |

## STEP 4A - Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| scalar expression evaluation semantics | `db/query/plan/expr/compiled_expr/evaluate.rs` | yes | `compiled_expr/evaluate.rs`; executor reader adapters | defining + value-source adaptation | no | yes | 2 | Boundary-protected | low | low |
| predicate capability meaning | `db/predicate/capability.rs` | yes | `predicate/capability.rs`; `predicate/runtime/mod.rs`; `index/predicate/compile.rs` | defining + runtime admission + index admission | no | yes | 3 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` and `db/query/plan/continuation.rs` | yes | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `executor/planning/continuation/scalar.rs` | defining + validating + transport + defensive re-checking | no | yes | 4 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/planning/route/capability.rs` | yes | `route/capability.rs`; `route/contracts/capabilities.rs`; `hints/load.rs`; `hints/aggregate.rs` | defining + transport + application | no | yes | 4 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store/mod.rs` | yes | `commit/store/mod.rs`; `commit/marker.rs` | defining + defensive re-checking | no | yes | 2 | Safety-enhancing | low-medium | low-medium |
| index-store reader authority | `db/index/readers.rs` | yes | `index/readers.rs`; `runtime_context/index_readers.rs`; `registry/readers.rs`; `commit_window.rs`; `commit/prepare.rs` | defining + runtime implementation + registry implementation + preflight overlay + preparation consumer | no | yes | 5 | Safety-enhancing | low-medium | low-medium |
| fluent non-paged terminal wrapper contract | `db/query/fluent/load/terminals.rs` | yes | `with_non_paged`; typed field terminals; projection terminal families; explain terminal families | typed adaptation + payload shaping + explain forwarding | yes | no | 4 | Partially consolidated | low-medium | low-medium |
| prepared SQL parameter admission | `db/sql/lowering/prepare.rs` | yes | SELECT, DELETE, INSERT, UPDATE, EXPLAIN, aggregate, ORDER BY, expression scanners | defining + owner-local AST traversal | yes | no | 8 | Safety-neutral | low | low |

## STEP 5A - Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| projection expression evaluation failures | `db/query/plan/expr/compiled_expr/mod.rs`; `db/executor/projection/eval/scalar.rs` | low-medium | no | no | no | yes | high | boundary-sensitive | medium-low |
| commit marker corruption constructors and envelope decode guards | `db/commit/marker.rs`; `db/commit/store/mod.rs` | low | no | no | no | yes | high | protective duplication | low-medium |
| index-store reader failures | `db/index/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/registry/readers.rs`; `db/executor/mutation/commit_window.rs` | low | no | no | no | yes | high | boundary-sensitive | low-medium |
| prepared SQL parameter rejection | `db/sql/lowering/prepare.rs`; `db/sql/lowering/expr.rs` | low | no | no | yes | yes | high | safe local ownership | low |

## STEP 6B - Protective Redundancy Review

Evidence mode: `classified`

| Pattern Family [M] | Files [M] | Same Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Why It Must Stay Split [C] | Behavioral Equivalence Confidence [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression IR vs planner-to-IR compile lowering | `compiled_expr/mod.rs`; `compiled_expr/compile.rs`; `compiled_expr/evaluate.rs`; `scalar.rs` | yes | yes | yes | evaluator purity depends on keeping planner structures out of runtime evaluation while compile modules own late lowering from planner trees into resolved IR leaves | high |
| predicate capability meaning vs runtime/index application | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | no | yes | yes | capability ownership stays in predicate authority while runtime and index compilation consume classified results locally | high |
| cursor contract definition vs planner/runtime transports | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, and executor revalidation distinct | high |
| route capability derivation vs route hint consumption | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/*` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and downstream execution contracts | high |
| commit marker stable envelope vs payload codec | `db/commit/store/mod.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary while marker owns the row-op payload shape | high |
| index reader contract vs runtime/preflight implementations | `db/index/readers.rs`; `db/executor/runtime_context/index_readers.rs`; `db/registry/readers.rs`; `db/executor/mutation/commit_window.rs` | no | yes | yes | preserves one reader contract while letting runtime contexts and preflight overlays implement their own visibility and staging semantics | high |

## STEP 7B - Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| SQL projection execution shell compression | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | session SQL boundary | low | low-medium | 10-24 | low-medium |
| route capability snapshot call-site compression | `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | route boundary | low-medium | low | 4-8 | low-medium |

## STEP 8B - Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| `CompiledExpr` evaluator and planner compile lowering | evaluator purity depends on keeping planner structures out of evaluation; only compile modules should know planner expression shapes | yes | do not merge | high |
| executor row/value readers and compiled expression semantics | readers are the execution-context boundary; moving row decoding or aggregation mechanics into expression evaluation would break the single-IR layering contract | yes | do not merge | high |
| cursor contract definition and planner/runtime transports | split preserves one semantic owner while keeping decode, validation, and transport boundaries explicit | yes | do not merge | high |
| route capability derivation and planner/hint consumption | split preserves route-owned capability reasoning and prevents policy re-derivation in consumers | yes | do not merge | high |
| commit marker store envelope and payload codec | split preserves the persistence trust boundary and failure classification edge | yes | do not merge | high |
| predicate capability meaning and runtime/index application | split preserves one semantic authority while keeping consumer admission policy local | yes | do not merge | medium |
| index reader contract and preflight overlay implementation | the overlay must remain commit-window local because it observes staged mutations that the normal runtime/registry readers must not see | yes | do not merge | medium-high |

## STEP 9 - Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | `7` | `8` | `+1` | dirty-tree index-store reader bridge work adds one visible low-risk bridge family |
| total high-risk divergence patterns | `0` | `0` | `0` | no production duplicate evaluator or cross-layer policy seam reopened |
| same-layer accidental duplication count | `0` | `0` | `0` | remaining same-owner targets are boilerplate wrappers rather than semantic duplication |
| cross-layer intentional duplication count | `3` | `3` | `0` | predicate, continuation, and route duplication remain intentionally boundary-protective |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `5` | `6` | `+1` | index-store reader bridge repetition is boundary-protected by the reader trait and preflight overlay split |
| invariants with `>3` enforcement sites | `4` | `5` | `+1` | index-store reader authority joins continuation, route, fluent wrappers, and prepared parameter admission |
| error-construction families with `>3` custom mappings | `1` | `1` | `0` | projection evaluation mapping remains the main custom mapping family |
| drift surface reduction estimate | `medium-low` | `medium-low` | stable | current pressure is still local wrapper/bridge repetition, not divergent semantics |
| estimated LoC reduction range (conservative) | `14-32` | `14-32` | stable | high-confidence candidates remain small local shells only |

High-risk ledger:

| Pattern [M] | Primary Locations [M] | Owner Boundary [C] | Canonical Owner Known? [C] | Worth Fixing This Cycle? [C] | Consolidation Safety Class [C] | Rationale [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| none | n/a | n/a | n/a | n/a | n/a | production scalar, grouped, HAVING, ordering, and aggregate terminal expression evaluation still goes through `CompiledExpr::evaluate` |

## STEP 9A - Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 8 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 28 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 2 | STEP 7B candidates with high behavioral-equivalence confidence and safe local unification |
| boundary-protected findings count | 13 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; this run is method-comparable to the 2026-04-30 baseline.
- The scan used dirty working tree snapshot `0eaa42bf5`, so conclusions are valid for the current in-progress tree rather than a committed release snapshot.

## 2. Mode A summary: high-impact consolidation opportunities

- No high-risk production DRY seam is open.
- `db/executor/projection/eval/scalar.rs` still adapts value sources into `CompiledExprValueReader` and calls `CompiledExpr::evaluate`.
- `db/query/plan/expr/compiled_expr/evaluate.rs` remains the single runtime expression engine; `db/query/plan/expr/compiled_expr/compile.rs` remains the planner-to-IR lowering boundary.

## 3. Mode A summary: medium opportunities

- No new medium-risk consolidation candidate was found.
- The largest added signal is the index-store reader bridge family across `db/index/readers.rs`, runtime context readers, registry readers, commit preparation, and the commit-window preflight overlay. This is boundary-protected and should not be collapsed into one generic helper because the overlay observes staged mutations that normal readers must not see.

## 4. Mode A summary: low/cosmetic opportunities

- `db/session/sql/execute/mod.rs` still has safe SQL projection/result shell repetition.
- Route hint helpers in `db/executor/planning/route/hints/load.rs` and `aggregate.rs` still repeat small same-owner bounded-fetch plumbing.
- Fluent non-paged terminal public wrappers remain local boilerplate.

## 5. Mode B summary: protective redundancies (keep separate)

- Keep `CompiledExpr` evaluator purity separate from planner compile lowering.
- Keep executor readers separate from compiled-expression semantics.
- Keep cursor contract definition separate from planner/runtime/session transport code.
- Keep route capability derivation separate from hint consumers.
- Keep commit marker store-envelope checks separate from marker payload codec.
- Keep index reader contracts separate from runtime/preflight implementations.

## 6. Dangerous consolidations (do not merge)

- Do not move planner expression types into `compiled_expr/evaluate.rs`.
- Do not move executor row decoding, grouped row state, projection materialization, or aggregation mechanics into compiled-expression evaluation.
- Do not merge cursor semantic ownership into planner/runtime/session transport code.
- Do not merge route capability derivation into hint or execution consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not collapse `PreflightStoreOverlay` into the normal runtime/registry index readers.

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- patterns found: `8`
- high-risk divergence patterns: `0`
- boundary-protected patterns: `6`
- drift surface reduction estimate: `medium-low`
- conservative LoC reduction: `14-32`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `8`
- classified findings: `28`
- high-confidence candidates: `2`
- boundary-protected findings: `13`

## 9. Architectural risk summary

The current tree is still structurally healthy from a DRY perspective. The new visible repetition is mostly index-store reader bridge plumbing introduced by the in-progress handle naming/readers edits. That repetition is not accidental semantic duplication; it is a boundary-preserving implementation split between trait authority, normal readers, registry readers, commit preparation, and the commit-window overlay.

## 10. DRY risk index (1-10, lower is better)

**4/10**

Current DRY pressure remains moderate. The count increased by one visible family, but the added family is boundary-protected and low-to-medium risk. No high-risk duplicate evaluator or policy re-derivation seam reopened.

## 11. Verification readout

`PASS`

Validation commands:

- `rg "ScalarProjectionExpr|eval_scalar_projection_expr|eval_canonical_scalar_projection_expr|ScalarProjectionExpr::|match expr" crates/icydb-core/src/db/executor -n` -> production clean; only projection tests plus `expr.evaluate(&reader)` adapter matches
- `rg "GroupedCompiledExpr|GroupedProjectionExpr|eval_grouped_projection_expr|eval_binary_expr|eval_unary_expr|projection/eval/operators|ScalarProjectionExpr" crates/icydb-core/src/db -g '!**/tests/**' -g '!**/tests.rs' -n` -> no runtime duplicate evaluator; planner compile surfaces only for `ScalarProjectionExpr`
- `rg "\.evaluate\(" crates/icydb-core/src/db/executor crates/icydb-core/src/db/query/plan/expr -g '!**/tests/**' -g '!**/tests.rs' -n` -> production expression evaluation flows through `CompiledExpr::evaluate`
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
