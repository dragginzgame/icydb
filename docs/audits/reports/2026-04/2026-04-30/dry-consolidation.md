# DRY Consolidation Audit - 2026-04-30

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-21/dry-consolidation.md`
- code snapshot identifier: `7bc3d71ad` (`dirty` working tree)
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

## STEP 0 — Run Metadata + Scope Capture

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

## STEP 1A — Structural Duplication Scan

Evidence mode: `mechanical`

| Pattern [M] | Files [M] | Lines [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Safety Critical? [C] | Behavioral Equivalence Confidence [C] | Drift Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled scalar expression evaluator vs unified `CompiledExpr` evaluator | `db/executor/projection/eval/scalar.rs`; `db/query/plan/expr/compiled_expr.rs`; `db/query/plan/expr/compiled_expr_compile.rs` | `scalar.rs:277-345,491`; `compiled_expr.rs:267-446,634-911`; `compiled_expr_compile.rs:22-120` | Evolution drift duplication | no | yes | no | yes | high | high | high |
| predicate capability classification + consumption | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | `predicate/capability.rs:122-334`; `predicate/runtime/mod.rs:19-20,215-216`; `index/predicate/compile.rs:14,78-85,173-180,319` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | `cursor/mod.rs:58-172`; `cursor/spine.rs:164-331`; `query/plan/continuation.rs:21-573`; `executor/planning/continuation/scalar.rs:30-214` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | `route/capability.rs:75-310`; `route/contracts/capabilities.rs:24`; `route/hints/load.rs:18-150`; `route/hints/aggregate.rs:20-108` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope + size guards | `db/commit/store/mod.rs`; `db/commit/marker.rs` | `commit/store/mod.rs:51-350`; `commit/marker.rs:155-571` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| fluent non-paged terminal strategy/wrapper family | `db/query/fluent/load/terminals.rs` | `terminals.rs:605-1366` | Boilerplate duplication | yes | yes | no | no | high | medium | medium |
| SQL projection/result finalization shell | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs`; `db/session/sql/compiled.rs` | `execute/mod.rs:116-242,351-504`; `sql/mod.rs:882-903`; `compiled.rs:47-67` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |

## STEP 2A — Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| scalar expression semantics and runtime value access | 2 evaluator families | query compiled-expression IR, executor projection scalar evaluator | yes | no | yes | no | yes (`CompiledExpr`) | high | medium | query compiled-expression boundary, with executor kept as reader/materialization only | high |
| predicate capability classification | 3 | predicate authority, runtime compilation, index predicate compile | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 4 | cursor, planner continuation, executor continuation, session transports | yes | no | yes | yes | yes (`db/cursor/mod.rs` plus planner contract) | high | medium | cursor/planner continuation boundary | medium-low |
| route capability snapshot propagation | 4 | route capability, route contracts, load hints, aggregate hints | yes | no | yes | yes | yes (`db/executor/planning/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store/mod.rs`) | high | low | commit store boundary | low-medium |
| fluent non-paged terminal execution/projection wrappers | 1 owner family | fluent load terminal execution, explain, projection adaptation | no | yes | yes | no | yes (`db/query/fluent/load/terminals.rs`) | high | low-medium | fluent load boundary | medium |
| SQL projection execution and response finalization shells | 1 owner family | SQL session compile contract, plan-cache binding, statement-result shaping | no | yes | yes | no | yes (`db/session/sql`) | high | low | session SQL boundary | low-medium |

## STEP 3A — Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs` | `1294` | `1` | no | consolidation-incomplete | the unified grouped/HAVING path owns scalar semantics, but row/scalar projection still has a second evaluator that can diverge from `CompiledExpr::evaluate` | high |
| `crates/icydb-core/src/db/executor/projection/eval/scalar.rs` | `627` | `1` | no | consolidation-incomplete | executor projection still dispatches `ScalarProjectionExpr` directly, duplicating CASE, function-call, unary, binary, and missing-slot behavior instead of feeding a reader into `CompiledExpr` | high |
| `crates/icydb-core/src/db/query/fluent/load/terminals.rs` | `1420` | `1` | yes | under-splitting | non-paged terminal wrappers repeat slot resolution and execution shells across many typed terminal methods | medium |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | `668` | `1` | yes | safety-neutral | SQL projection and grouped-result helpers have been partially centralized, but cached and bypass paths still repeat the same prepared-plan/projection-contract shell | low-medium |
| `crates/icydb-core/src/db/predicate/runtime/mod.rs` | `1034` | `1` | yes | safety-neutral | runtime predicate evaluation remains dense, but capability duplication is still subordinate to the predicate authority boundary | low-medium |
| `crates/icydb-core/src/db/commit/store/mod.rs` | `427` | `1` | no | safety-neutral | stable envelope validation and store orchestration remain boundary-protected | low-medium |
| `crates/icydb-core/src/db/executor/planning/route/hints/load.rs` + `aggregate.rs` | `296` | `1` | yes | safety-neutral | bounded-fetch and capability-hint helpers repeat small same-owner call shells | low-medium |

## STEP 4A — Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| scalar expression evaluation semantics | `db/query/plan/expr/compiled_expr.rs` | yes | `compiled_expr.rs`; `executor/projection/eval/scalar.rs` | defining + parallel execution | no | no | 2 | Divergence-prone | high | high |
| predicate capability meaning | `db/predicate/capability.rs` | yes | `predicate/capability.rs`; `predicate/runtime/mod.rs`; `index/predicate/compile.rs` | defining + runtime admission + index admission | no | yes | 3 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` and `db/query/plan/continuation.rs` | yes | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `executor/planning/continuation/scalar.rs` | defining + validating + transport + defensive re-checking | no | yes | 4 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/planning/route/capability.rs` | yes | `route/capability.rs`; `route/contracts/capabilities.rs`; `route/hints/load.rs`; `route/hints/aggregate.rs` | defining + transport + application | no | yes | 4 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store/mod.rs` | yes | `commit/store/mod.rs`; `commit/marker.rs` | defining + defensive re-checking | no | yes | 2 | Safety-enhancing | low-medium | low-medium |
| fluent non-paged terminal wrapper contract | `db/query/fluent/load/terminals.rs` | yes | `with_non_paged`; typed field terminals; projection terminal families; explain terminal families | typed adaptation + payload shaping + explain forwarding | yes | no | 4 | Consolidation candidate | medium | medium |
| prepared SQL parameter admission | `db/sql/lowering/prepare.rs` | yes | SELECT, DELETE, INSERT, UPDATE, EXPLAIN, aggregate, ORDER BY, expression scanners | defining + owner-local AST traversal | yes | no | 8 | Safety-neutral | low | low |

## STEP 5A — Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| projection expression evaluation failures | `db/query/plan/expr/compiled_expr.rs`; `db/executor/projection/eval/scalar.rs`; `db/query/plan/expr/projection_eval.rs` | medium | no | yes | no | yes | high | boundary-sensitive | high |
| commit marker corruption constructors and envelope decode guards | `db/commit/marker.rs`; `db/commit/store/mod.rs` | low | no | no | no | yes | high | protective duplication | low-medium |
| prepared SQL parameter rejection | `db/sql/lowering/prepare.rs`; `db/sql/lowering/expr.rs` | low | no | no | yes | yes | high | safe local ownership | low |

## STEP 6B — Protective Redundancy Review

Evidence mode: `classified`

| Pattern Family [M] | Files [M] | Same Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Why It Must Stay Split [C] | Behavioral Equivalence Confidence [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled expression IR vs planner-to-IR compile lowering | `compiled_expr.rs`; `compiled_expr_compile.rs` | yes | yes | yes | `compiled_expr.rs` should remain planner/executor-free while `compiled_expr_compile.rs` owns late lowering from planner expression structures into IR leaves | high |
| predicate capability meaning vs runtime/index application | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | no | yes | yes | capability ownership stays in predicate authority while runtime and index compilation consume classified results locally | high |
| cursor contract definition vs planner/runtime transports | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/executor/planning/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, and executor revalidation distinct | high |
| route capability derivation vs route hint consumption | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/*` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and downstream execution contracts | high |
| commit marker stable envelope vs payload codec | `db/commit/store/mod.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary while marker owns the row-op payload shape | high |

## STEP 7B — Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| scalar projection evaluator contraction into `CompiledExpr` | `db/executor/projection/eval/scalar.rs`; `db/query/plan/expr/compiled_expr.rs`; `db/query/plan/expr/compiled_expr_compile.rs` | Evolution drift duplication | no | yes | no | yes | boundary-sensitive | compiled expression boundary; executor projection supplies readers and materialization shells only | medium | high | 120-220 | high |
| fluent non-paged terminal wrapper compression | `db/query/fluent/load/terminals.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | fluent load boundary | low-medium | medium | 20-40 | medium |
| SQL projection execution shell compression | `db/session/sql/execute/mod.rs`; `db/session/sql/mod.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | session SQL boundary | low | low-medium | 10-24 | low-medium |
| route capability snapshot call-site compression | `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | route boundary | low-medium | low | 4-8 | low-medium |

## STEP 8B — Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| `CompiledExpr` evaluator and planner compile lowering | evaluator purity depends on keeping planner structures out of `compiled_expr.rs`; only the compile module should know planner expression shapes | yes | do not merge | high |
| cursor contract definition and planner/runtime transports | split preserves one semantic owner while keeping decode, validation, and transport boundaries explicit | yes | do not merge | high |
| route capability derivation and planner/hint consumption | split preserves route-owned capability reasoning and prevents policy re-derivation in consumers | yes | do not merge | high |
| commit marker store envelope and payload codec | split preserves the persistence trust boundary and failure classification edge | yes | do not merge | high |
| predicate capability meaning and runtime/index application | split preserves one semantic authority while keeping consumer admission policy local | yes | do not merge | medium |

## STEP 9 — Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | `6` | `7` | `+1` | prepared fallback cooled, but the audit now counts the remaining scalar-expression evaluator split as a distinct structural duplication family |
| total high-risk divergence patterns | `0` | `1` | `+1` | row/scalar projection still has a parallel evaluator next to `CompiledExpr::evaluate` |
| same-layer accidental duplication count | `1` | `0` | `-1` | remaining same-owner targets are mostly boilerplate wrappers rather than accidental semantic duplication |
| cross-layer intentional duplication count | `3` | `3` | `0` | predicate, continuation, and route duplication remain intentionally boundary-protective |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `4` | `5` | `+1` | protective splits still dominate the cross-layer families |
| invariants with `>3` enforcement sites | `4` | `4` | `0` | continuation, route, fluent wrappers, and prepared parameter admission have broad but understood enforcement surfaces |
| error-construction families with `>3` custom mappings | `1` | `1` | `0` | projection evaluation error mapping is now the main custom mapping family to watch |
| drift surface reduction estimate | `medium` | `high` | worsened | the dual expression-engine seam is a higher-value contraction than the prior prepared fallback seam |
| estimated LoC reduction range (conservative) | `54-116` | `154-292` | increased | most estimated reduction comes from retiring or shrinking the scalar projection evaluator once row projection feeds `CompiledExpr` |

High-risk ledger:

| Pattern [M] | Primary Locations [M] | Owner Boundary [C] | Canonical Owner Known? [C] | Worth Fixing This Cycle? [C] | Consolidation Safety Class [C] | Rationale [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| compiled scalar expression evaluator vs unified `CompiledExpr` evaluator | `db/query/plan/expr/compiled_expr.rs`; `db/executor/projection/eval/scalar.rs`; `db/query/plan/expr/compiled_expr_compile.rs` | compiled expression boundary | yes | yes | boundary-sensitive | it duplicates CASE, function, unary, binary, and missing-slot semantics across row/scalar and grouped paths, weakening the single-engine invariant |

## STEP 9A — Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 7 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 23 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 3 | STEP 7B candidates with high behavioral-equivalence confidence and `safe local unification` |
| boundary-protected findings count | 10 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; this run is comparable to the 2026-04-21 baseline.

## 2. Mode A summary: high-impact consolidation opportunities

- The strongest live consolidation target is [scalar.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/projection/eval/scalar.rs:277) beside [compiled_expr.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs:267). Grouped/HAVING paths now use `CompiledExpr`, but row/scalar projection still evaluates `ScalarProjectionExpr` directly.
- This is not a recommendation to merge planner lowering into the evaluator. The safe target is to keep [compiled_expr_compile.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/compiled_expr_compile.rs:22) as the compile boundary and make execution paths feed readers into `CompiledExpr::evaluate`.

## 3. Mode A summary: medium opportunities

- [terminals.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/fluent/load/terminals.rs:605) still carries owner-local non-paged terminal wrapper repetition.

## 4. Mode A summary: low/cosmetic opportunities

- [execute/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/execute/mod.rs:116) has mostly safe SQL projection/result shell repetition after the recent prepared-plan contract consolidation.
- Route hint helpers in [load.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/hints/load.rs:18) and [aggregate.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/hints/aggregate.rs:20) still repeat small same-owner bounded-fetch plumbing.

## 5. Mode B summary: protective redundancies (keep separate)

- `CompiledExpr` evaluator purity vs planner compile lowering should remain split.
- Cursor contract definition vs transport/application remains intentionally split.
- Route capability derivation vs route hint consumption remains intentionally split.
- Commit marker store-envelope checks vs marker payload codec remain intentionally split.
- Predicate capability meaning vs runtime/index application remains intentionally split.

## 6. Dangerous consolidations (do not merge)

- Do not move planner expression types into [compiled_expr.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/plan/expr/compiled_expr.rs:1).
- Do not merge cursor semantic ownership into planner/runtime/session transport code.
- Do not merge route capability derivation into hint or execution consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not move predicate capability meaning out of [capability.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/capability.rs:122).

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- patterns found: `7`
- high-risk divergence patterns: `1`
- boundary-protected patterns: `5`
- drift surface reduction estimate: `high`
- conservative LoC reduction: `154-292`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `7`
- classified findings: `23`
- high-confidence candidates: `3`
- boundary-protected findings: `10`

## 9. Architectural risk summary

- Current DRY pressure is moderate-high and warmer than the April 21 run.
- The old prepared fallback hotspot cooled: prepared SQL parameters are rejected at the lowering boundary rather than rebinding through a separate session parameter lane.
- The remaining risk is sharper: scalar row projection still has a second expression evaluator while grouped/HAVING now use `CompiledExpr`.

## 10. DRY risk index (1-10, lower is better)

**6.4/10**

Interpretation:

- Risk stays below the high-risk band because most cross-layer duplication remains boundary-protective and validation passes.
- Risk is higher than the prior run because there is now one high-risk expression-evaluator drift pattern that directly conflicts with the single-engine direction.
- The most valuable next contraction is completing the row/scalar projection migration onto `CompiledExpr::evaluate` without collapsing the planner compile boundary into the runtime evaluator.

## 11. Verification Readout

- comparability status: `comparable`
- mandatory sections present: `yes`
- status: `PASS`

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
