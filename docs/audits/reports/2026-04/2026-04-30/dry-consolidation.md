# DRY Consolidation Audit - 2026-04-30

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded unless explicitly called out)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-21/dry-consolidation.md`
- same-day rerun basis: post-`0.144.2` compiled-expression consolidation
- code snapshot identifier: `d07575370` (`clean` working tree at scan time)
- method tag/version: `DRY-1.2`
- method manifest:
  - `method_version = DRY-1.2`
  - `duplication_taxonomy = DT-1`
  - `owner_layer_taxonomy = OL-1`
  - `invariant_role_model = IR-1`
  - `facade_inclusion_rule = FI-1`
  - `consolidation_safety_model = CS-1`
- comparability status: `comparable`

## Evidence Artifacts

- `docs/audits/reports/2026-04/2026-04-30/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-04/2026-04-30/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`

## STEP 0 - Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-04/2026-04-21/dry-consolidation.md` | same | none | yes |
| method version | `DRY-1.2` | `DRY-1.2` | none | yes |
| duplication taxonomy | `DT-1` | `DT-1` | none | yes |
| owner-layer taxonomy | `OL-1` | `OL-1` | none | yes |
| invariant role model | `IR-1` | `IR-1` | none | yes |
| facade inclusion rule | `FI-1` | `FI-1` | none | yes |
| consolidation safety model | `CS-1` | `CS-1` | none | yes |
| in-scope roots | `crates/icydb-core/src` | same | none | yes |
| exclusions | tests/bench/examples/generated | same | none | yes |

## STEP 1A - Structural Duplication Scan

Evidence mode: `mechanical`

| Pattern [M] | Files [M] | Lines [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Safety Critical? [C] | Behavioral Equivalence Confidence [C] | Drift Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression IR vs planner-to-IR lowering | `db/query/plan/expr/compiled_expr.rs`; `db/query/plan/expr/compiled_expr_compile.rs`; `db/query/plan/expr/scalar.rs` | `compiled_expr.rs:344-787,997-1168`; `compiled_expr_compile.rs:22-120,271-373`; `scalar.rs:160-303` | Intentional boundary duplication | yes | yes | yes | yes | high | low | low |
| predicate capability classification + consumption | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | `predicate/capability.rs:122-334`; `predicate/runtime/mod.rs:19-20,215-216`; `index/predicate/compile.rs:14,78-85,173-180,319` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | `cursor/mod.rs:58-172`; `cursor/spine.rs:164-331`; `query/plan/continuation.rs:21-573`; `executor/planning/continuation/scalar.rs:30-214` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | `route/capability.rs:75-310`; `route/contracts/capabilities.rs:24`; `route/hints/load.rs:18-150`; `route/hints/aggregate.rs:20-108` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope + size guards | `db/commit/store/mod.rs`; `db/commit/marker.rs` | `commit/store/mod.rs:51-350`; `commit/marker.rs:155-571` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| fluent non-paged terminal strategy/wrapper family | `db/query/fluent/load/terminals.rs` | `terminals.rs:605-1366` | Boilerplate duplication | yes | yes | no | no | high | medium | medium |
| SQL projection/result finalization shell | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs`; `db/session/sql/compiled.rs` | `execute/mod.rs:116-242,351-504`; `sql/mod.rs:882-903`; `compiled.rs:47-67` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |

## STEP 1B - Closed High-Risk Seam Verification

Evidence mode: `mechanical`

| Former Risk [M] | Current Evidence [M] | Classification [C] | Residual Risk [C] |
| ---- | ---- | ---- | ---- |
| row/scalar projection evaluating `ScalarProjectionExpr` directly at runtime | `rg "ScalarProjectionExpr\|eval_scalar_projection_expr\|eval_canonical_scalar_projection_expr\|ScalarProjectionExpr::\|match expr" crates/icydb-core/src/db/executor -n` returns only `db/executor/projection/tests/*` plus `expr.evaluate(&reader)` adapter calls in `db/executor/projection/eval/scalar.rs` | closed in production | low |
| duplicate scalar operator evaluator under executor projection | `rg "eval_binary_expr\|eval_unary_expr\|projection/eval/operators\|GroupedCompiledExpr\|GroupedProjectionExpr\|eval_grouped_projection_expr" crates/icydb-core/src/db -n` returns only unrelated `db/executor/pipeline/mod.rs:9` operator module wiring | closed | low |
| runtime expression paths bypassing `CompiledExpr::evaluate` | production `rg "\.evaluate\(" crates/icydb-core/src/db/executor crates/icydb-core/src/db/query/plan/expr -g '!**/tests/**' -g '!**/tests.rs' -n` shows executor projection/grouped/aggregate paths calling `CompiledExpr::evaluate` through reader adapters | closed | low |

## STEP 2A - Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression evaluation vs compile lowering | 3 coordinated surfaces | planner scalar compile tree, compile lowering, compiled evaluator | no | yes | yes | yes | yes (`CompiledExpr::evaluate`, with lowering in `compiled_expr_compile.rs`) | high | low | keep split between compile and evaluate modules | low |
| predicate capability classification | 3 | predicate authority, runtime compilation, index predicate compile | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 4 | cursor, planner continuation, executor continuation, session transports | yes | no | yes | yes | yes (`db/cursor/mod.rs` plus planner contract) | high | medium | cursor/planner continuation boundary | medium-low |
| route capability snapshot propagation | 4 | route capability, route contracts, load hints, aggregate hints | yes | no | yes | yes | yes (`db/executor/planning/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store/mod.rs`) | high | low | commit store boundary | low-medium |
| fluent non-paged terminal execution/projection wrappers | 1 owner family | fluent load terminal execution, explain, projection adaptation | no | yes | yes | no | yes (`db/query/fluent/load/terminals.rs`) | high | low-medium | fluent load boundary | medium |
| SQL projection execution and response finalization shells | 1 owner family | SQL session compile contract, plan-cache binding, statement-result shaping | no | yes | yes | no | yes (`db/session/sql`) | high | low | session SQL boundary | low-medium |

## STEP 3A - Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs` | `1550` | `1` | yes | boundary-owned growth | the evaluator is now the single expression engine and intentionally also carries reader contract, hot variants, CASE, function, numeric, and comparison semantics | medium |
| `crates/icydb-core/src/db/executor/projection/eval/scalar.rs` | `445` | `0` | no | contraction-complete | executor scalar projection no longer evaluates planner scalar trees; it only adapts value sources into `CompiledExprValueReader` and delegates to `CompiledExpr::evaluate` | low |
| `crates/icydb-core/src/db/query/fluent/load/terminals.rs` | `1420` | `1` | yes | under-splitting | non-paged terminal wrappers repeat slot resolution and execution shells across many typed terminal methods | medium |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | `668` | `1` | yes | safety-neutral | SQL projection and grouped-result helpers have been partially centralized, but cached and bypass paths still repeat the same prepared-plan/projection-contract shell | low-medium |
| `crates/icydb-core/src/db/predicate/runtime/mod.rs` | `1034` | `1` | yes | safety-neutral | runtime predicate evaluation remains dense, but capability duplication is still subordinate to the predicate authority boundary | low-medium |
| `crates/icydb-core/src/db/commit/store/mod.rs` | `427` | `1` | no | safety-neutral | stable envelope validation and store orchestration remain boundary-protected | low-medium |
| `crates/icydb-core/src/db/executor/planning/route/hints/load.rs` + `aggregate.rs` | `296` | `1` | yes | safety-neutral | bounded-fetch and capability-hint helpers repeat small same-owner call shells | low-medium |

## STEP 4A - Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| scalar expression evaluation semantics | `db/query/plan/expr/compiled_expr.rs` | yes | `compiled_expr.rs`; executor reader adapters | defining + value-source adaptation | no | yes | 2 | Boundary-protected | low | low |
| predicate capability meaning | `db/predicate/capability.rs` | yes | `predicate/capability.rs`; `predicate/runtime/mod.rs`; `index/predicate/compile.rs` | defining + runtime admission + index admission | no | yes | 3 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` and `db/query/plan/continuation.rs` | yes | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `executor/planning/continuation/scalar.rs` | defining + validating + transport + defensive re-checking | no | yes | 4 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/planning/route/capability.rs` | yes | `route/capability.rs`; `route/contracts/capabilities.rs`; `route/hints/load.rs`; `route/hints/aggregate.rs` | defining + transport + application | no | yes | 4 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store/mod.rs` | yes | `commit/store/mod.rs`; `commit/marker.rs` | defining + defensive re-checking | no | yes | 2 | Safety-enhancing | low-medium | low-medium |
| fluent non-paged terminal wrapper contract | `db/query/fluent/load/terminals.rs` | yes | `with_non_paged`; typed field terminals; projection terminal families; explain terminal families | typed adaptation + payload shaping + explain forwarding | yes | no | 4 | Consolidation candidate | medium | medium |
| prepared SQL parameter admission | `db/sql/lowering/prepare.rs` | yes | SELECT, DELETE, INSERT, UPDATE, EXPLAIN, aggregate, ORDER BY, expression scanners | defining + owner-local AST traversal | yes | no | 8 | Safety-neutral | low | low |

## STEP 5A - Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| projection expression evaluation failures | `db/query/plan/expr/compiled_expr.rs`; `db/executor/projection/eval/scalar.rs`; `db/query/plan/expr/projection_eval.rs` | low-medium | no | no | no | yes | high | boundary-sensitive | medium-low |
| commit marker corruption constructors and envelope decode guards | `db/commit/marker.rs`; `db/commit/store/mod.rs` | low | no | no | no | yes | high | protective duplication | low-medium |
| prepared SQL parameter rejection | `db/sql/lowering/prepare.rs`; `db/sql/lowering/expr.rs` | low | no | no | yes | yes | high | safe local ownership | low |

## STEP 6B - Protective Redundancy Review

Evidence mode: `classified`

| Pattern Family [M] | Files [M] | Same Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Why It Must Stay Split [C] | Behavioral Equivalence Confidence [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression IR vs planner-to-IR compile lowering | `compiled_expr.rs`; `compiled_expr_compile.rs`; `scalar.rs` | yes | yes | yes | `compiled_expr.rs` must remain planner/executor-free while compile modules own late lowering from planner expression structures into resolved IR leaves | high |
| predicate capability meaning vs runtime/index application | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | no | yes | yes | capability ownership stays in predicate authority while runtime and index compilation consume classified results locally | high |
| cursor contract definition vs planner/runtime transports | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, and executor revalidation distinct | high |
| route capability derivation vs route hint consumption | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/*` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and downstream execution contracts | high |
| commit marker stable envelope vs payload codec | `db/commit/store/mod.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary while marker owns the row-op payload shape | high |

## STEP 7B - Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| fluent non-paged terminal wrapper compression | `db/query/fluent/load/terminals.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | fluent load boundary | low-medium | medium | 20-40 | medium |
| SQL projection execution shell compression | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | session SQL boundary | low | low-medium | 10-24 | low-medium |
| route capability snapshot call-site compression | `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | route boundary | low-medium | low | 4-8 | low-medium |

## STEP 8B - Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| `CompiledExpr` evaluator and planner compile lowering | evaluator purity depends on keeping planner structures out of `compiled_expr.rs`; only compile modules should know planner expression shapes | yes | do not merge | high |
| executor row/value readers and compiled expression semantics | readers are the execution-context boundary; moving row decoding or aggregation mechanics into `compiled_expr.rs` would break the single-IR layering contract | yes | do not merge | high |
| cursor contract definition and planner/runtime transports | split preserves one semantic owner while keeping decode, validation, and transport boundaries explicit | yes | do not merge | high |
| route capability derivation and planner/hint consumption | split preserves route-owned capability reasoning and prevents policy re-derivation in consumers | yes | do not merge | high |
| commit marker store envelope and payload codec | split preserves the persistence trust boundary and failure classification edge | yes | do not merge | high |
| predicate capability meaning and runtime/index application | split preserves one semantic authority while keeping consumer admission policy local | yes | do not merge | medium |

## STEP 9 - Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Prior Same-Day Run [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | `7` | `7` | `0` | the expression family remains visible only as a protected compile/evaluate split, not a dual runtime engine |
| total high-risk divergence patterns | `1` | `0` | `-1` | the scalar projection evaluator duplication has been removed from production runtime |
| same-layer accidental duplication count | `0` | `0` | `0` | remaining same-owner targets are boilerplate wrappers rather than semantic duplication |
| cross-layer intentional duplication count | `3` | `3` | `0` | predicate, continuation, and route duplication remain intentionally boundary-protective |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `5` | `5` | `0` | the expression family moved from unsafe dual evaluation to protected compile/evaluate separation |
| invariants with `>3` enforcement sites | `4` | `4` | `0` | continuation, route, fluent wrappers, and prepared parameter admission remain broad but understood surfaces |
| error-construction families with `>3` custom mappings | `1` | `1` | `0` | projection evaluation mapping is still the main custom mapping family, now with preserved reader-owned taxonomy |
| drift surface reduction estimate | `high` | `medium-low` | improved | the only high-value semantic duplication seam was closed |
| estimated LoC reduction range (conservative) | `154-292` | `34-72` | improved | remaining candidates are mostly local wrapper compression |

High-risk ledger:

| Pattern [M] | Primary Locations [M] | Owner Boundary [C] | Canonical Owner Known? [C] | Worth Fixing This Cycle? [C] | Consolidation Safety Class [C] | Rationale [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| none | n/a | n/a | n/a | n/a | n/a | production scalar, grouped, HAVING, ordering, and aggregate terminal expression evaluation now goes through `CompiledExpr::evaluate` |

## STEP 9A - Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 7 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 24 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 3 | STEP 7B candidates with high behavioral-equivalence confidence and safe local unification |
| boundary-protected findings count | 11 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; this run is comparable to the 2026-04-21 baseline and supersedes the earlier same-day pre-consolidation readout.
- The code scan was run on clean snapshot `d07575370`.

## 2. Mode A summary: high-impact consolidation opportunities

- No high-risk production DRY seam remains in expression evaluation.
- [scalar.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/projection/eval/scalar.rs:316) now contains value-reader adapters that call `CompiledExpr::evaluate`; it no longer evaluates `ScalarProjectionExpr`.
- [compiled_expr.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs:344) is now the single runtime expression engine. [compiled_expr_compile.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/compiled_expr_compile.rs:22) remains the correct planner-to-IR lowering boundary.

## 3. Mode A summary: medium opportunities

- [terminals.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/fluent/load/terminals.rs:605) still carries owner-local non-paged terminal wrapper repetition.
- This is local boilerplate pressure, not a semantic authority split.

## 4. Mode A summary: low/cosmetic opportunities

- [execute/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs:116) has mostly safe SQL projection/result shell repetition after the prepared-plan contract consolidation.
- Route hint helpers in [load.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/hints/load.rs:18) and [aggregate.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/hints/aggregate.rs:20) still repeat small same-owner bounded-fetch plumbing.

## 5. Mode B summary: protective redundancies (keep separate)

- `CompiledExpr` evaluator purity vs planner compile lowering should remain split.
- Executor readers should stay separate from compiled-expression semantics.
- Cursor contract definition vs transport/application remains intentionally split.
- Route capability derivation vs route hint consumption remains intentionally split.
- Commit marker store-envelope checks vs marker payload codec remain intentionally split.
- Predicate capability meaning vs runtime/index application remains intentionally split.

## 6. Dangerous consolidations (do not merge)

- Do not move planner expression types into [compiled_expr.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs:1).
- Do not move executor row decoding, grouped row state, projection materialization, or aggregation mechanics into [compiled_expr.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs:1).
- Do not merge cursor semantic ownership into planner/runtime/session transport code.
- Do not merge route capability derivation into hint or execution consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not move predicate capability meaning out of [capability.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/capability.rs:122).

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- patterns found: `7`
- high-risk divergence patterns: `0`
- boundary-protected patterns: `5`
- drift surface reduction estimate: `medium-low`
- conservative LoC reduction: `34-72`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `7`
- classified findings: `24`
- high-confidence candidates: `3`
- boundary-protected findings: `11`

## 9. DRY risk index (1-10, lower is better)

**4/10**

Current DRY pressure is moderate and materially cooler than the earlier
same-day pre-consolidation readout. The remaining pressure is concentrated in
local wrappers and intentionally protective boundary splits rather than
divergence-prone duplicate evaluators.

## 10. Validation commands

- `rg "ScalarProjectionExpr|eval_scalar_projection_expr|eval_canonical_scalar_projection_expr|ScalarProjectionExpr::|match expr" crates/icydb-core/src/db/executor -n` -> production clean; only projection tests plus `expr.evaluate(&reader)` adapter matches
- `rg "GroupedCompiledExpr|GroupedProjectionExpr|eval_grouped_projection_expr|eval_binary_expr|eval_unary_expr|projection/eval/operators|ScalarProjectionExpr" crates/icydb-core/src/db -g '!**/tests/**' -g '!**/tests.rs' -n` -> no runtime duplicate evaluator; planner compile surfaces only for `ScalarProjectionExpr`
- `rg "\.evaluate\(" crates/icydb-core/src/db/executor crates/icydb-core/src/db/query/plan/expr -g '!**/tests/**' -g '!**/tests.rs' -n` -> production expression evaluation flows through `CompiledExpr::evaluate`
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
