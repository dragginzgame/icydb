# DRY Consolidation Audit - 2026-04-17

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-13/dry-consolidation.md`
- code snapshot identifier: `8ffba6a5c` (`dirty` working tree)
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

- `docs/audits/reports/2026-04/2026-04-17/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-04/2026-04-17/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`

## STEP 0 — Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-04/2026-04-13/dry-consolidation.md` | same | none | yes |
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
| predicate capability classification + consumption | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | `predicate/capability.rs:124-213,321,461-469`; `predicate/runtime/mod.rs:212`; `index/predicate/compile.rs:51-53,80,130-132,171` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs`; `db/executor/planning/continuation/scalar.rs` | `cursor/mod.rs:58-182,203-214`; `cursor/spine.rs:265-329`; `query/plan/continuation.rs:347-475`; `session/query.rs:954-958,1039-1043,1075-1078`; `executor/planning/continuation/scalar.rs:209` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | `route/capability.rs:139-162`; `route/contracts/capabilities.rs:31-39`; `route/hints/load.rs:37-75,138-145`; `route/hints/aggregate.rs:20-25,31-67,80-87` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope + size guards | `db/commit/store/mod.rs`; `db/commit/marker.rs` | `commit/store/mod.rs:67-105,145-214,344-448`; `commit/marker.rs:151,225-233,329,501` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| fluent non-paged terminal strategy/wrapper family | `db/query/fluent/load/terminals.rs` | `terminals.rs:59-230,862-985,1096-1184` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |
| SQL projection lowering operator + validation ladders | `db/sql/lowering/select/projection.rs` | `projection.rs:159-281,510-594,614-670` | Evolution drift duplication | yes | yes | no | yes | medium-high | low-medium | medium-low |

## STEP 2A — Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability classification | 3 | predicate authority, runtime compilation, index predicate compile | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 5 | cursor, planner continuation, session, executor continuation | yes | no | yes | yes | yes (`db/cursor/mod.rs`) | high | medium | cursor boundary | medium-low |
| route capability snapshot propagation | 4 | route capability, route contracts, route hints | yes | no | yes | yes | yes (`db/executor/planning/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store/mod.rs`) | high | low | commit store boundary | low-medium |
| fluent non-paged terminal execution/projection wrappers | 1 owner family | fluent load terminal execution, explain, projection adaptation | no | yes | yes | no | yes (`db/query/fluent/load/terminals.rs`) | high | low | fluent load boundary | low-medium |
| SQL projection lowering contracts | 1 owner family | text-function literal contracts, arithmetic operator lowering, round-input adaptation | no | yes | yes | no | yes (`db/sql/lowering/select/projection.rs`) | medium-high | low-medium | SQL lowering boundary | medium-low |

## STEP 3A — Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/query/fluent/load/terminals.rs` | `1184` | `1` | yes | under-splitting | execution, explain, and projection wrapper families now share one owner-local shell, so small repetitive adapters accumulate inside one already-large API boundary | medium |
| `crates/icydb-core/src/db/sql/lowering/select/projection.rs` | `680` | `1` | yes | under-splitting | text-function spec handling is better localized than before, but arithmetic/round lowering and projection-contract validation still sit in one owner and repeat small mapping ladders | medium-low |
| `crates/icydb-core/src/db/predicate/runtime/mod.rs` | `991` | `1` | yes | safety-neutral | runtime compilation and evaluation remain dense, but the recent compare split reduced the older same-owner duplication pressure | low-medium |
| `crates/icydb-core/src/db/commit/store/mod.rs` | `728` | `1` | no | safety-neutral | stable envelope validation and store orchestration remain dense but boundary-protected | low-medium |
| `crates/icydb-core/src/db/executor/planning/route/hints/load.rs` + `aggregate.rs` | `291` | `1` | yes | safety-neutral | bounded-fetch and capability-hint helpers still repeat small same-owner call shells, but the seam is narrow and localized | low-medium |

## STEP 4A — Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| predicate capability meaning | `db/predicate/capability.rs` | yes | `predicate/capability.rs`; `predicate/runtime/mod.rs`; `index/predicate/compile.rs` | defining + runtime admission + index admission | no | yes | 3 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` | yes | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `session/query.rs`; `executor/planning/continuation/scalar.rs` | defining + validating + transport + defensive re-checking | no | yes | 5 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/planning/route/capability.rs` | yes | `route/capability.rs`; `route/contracts/capabilities.rs`; `route/hints/load.rs`; `route/hints/aggregate.rs` | defining + transport + application | no | yes | 4 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store/mod.rs` | yes | `commit/store/mod.rs`; `commit/marker.rs` | defining + defensive re-checking | no | yes | 2 | Safety-enhancing | low-medium | low-medium |
| fluent non-paged terminal wrapper contract | `db/query/fluent/load/terminals.rs` | yes | `execute_scalar_non_paged_terminal`; `map_non_paged_query_output`; `execute_prepared_*_terminal_output`; projection wrapper terminals | typed adaptation + payload shaping + explain forwarding | yes | no | 6 | Consolidation candidate | low-medium | low-medium |
| SQL projection literal/operator contract | `db/sql/lowering/select/projection.rs` | yes | `TextFnSpec::validate`; `TextFnSpec::build_projection`; `lower_arithmetic_projection_expr`; `lower_round_projection_expr`; `validate_numeric_projection_literal` | defining + typed adaptation + wrapper validation | yes | no | 5 | Consolidation candidate | low-medium | medium-low |

## STEP 5A — Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability fallback to runtime/index policy | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | low | no | no | no | yes | high | already consolidated | low |
| cursor continuation mismatch mapping | `db/cursor/mod.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs` | low | no | low | no | yes | high | boundary-sensitive | low-medium |
| commit marker envelope failure mapping | `db/commit/store/mod.rs`; `db/commit/marker.rs` | low | no | no | no | yes | high | boundary-protected | low-medium |
| fluent terminal output conversion family | `db/query/fluent/load/terminals.rs` | low-medium | no | no | yes | yes | high | safe local unification | low-medium |
| SQL projection validation failure family | `db/sql/lowering/select/projection.rs` | low-medium | no | no | yes | yes | medium-high | safe local unification | low-medium |

## STEP 6B — Boundary-Protective Redundancy

Evidence mode: `classified`

| Area [M] | Duplication Sites [M] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Protective Rationale [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability classifier vs runtime/index application | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | no | yes | yes | classifier owns semantic meaning; runtime and index compile own admission/application only | medium |
| cursor contract definition vs planner/runtime/session transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs`; `db/executor/planning/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, executor revalidation, and session transport distinct | high |
| route capability derivation vs route hint consumption | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/*` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and downstream execution contracts | high |
| commit marker stable envelope vs payload codec | `db/commit/store/mod.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary; marker owns the row-op payload shape | high |

## STEP 7B — Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| fluent non-paged terminal wrapper compression | `db/query/fluent/load/terminals.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | fluent load boundary | low | low-medium | 10-18 | low-medium |
| SQL projection lowering contract compression | `db/sql/lowering/select/projection.rs` | Evolution drift duplication | yes | yes | no | yes | safe local unification | SQL lowering boundary | low-medium | medium | 10-18 | medium-low |
| route capability snapshot call-site compression | `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | route boundary | low-medium | low | 4-8 | low-medium |

## STEP 8B — Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| cursor contract definition and planner/runtime/session transports | split preserves one semantic owner while keeping decode, validation, and transport boundaries explicit | yes | do not merge | high |
| route capability derivation and planner/hint consumption | split preserves route-owned capability reasoning and prevents policy re-derivation in consumers | yes | do not merge | high |
| commit marker store envelope and payload codec | split preserves the persistence trust boundary and failure classification edge | yes | do not merge | high |
| predicate capability meaning and runtime/index application | split preserves one semantic authority while keeping consumer admission policy local | yes | do not merge | medium |

## STEP 9 — Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | `6` | `6` | `0` | duplication pressure stayed flat and remains concentrated in a small set of known families |
| total high-risk divergence patterns | `0` | `0` | `0` | no high-risk drift-triggering duplication |
| same-layer accidental duplication count | `2` | `2` | `0` | there are still two credible owner-local consolidation candidates, but neither requires reopening boundaries |
| cross-layer intentional duplication count | `3` | `3` | `0` | remaining cross-layer duplication is still protective transport/application separation |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `4` | `4` | `0` | dominant cross-layer pattern remains protective redundancy rather than accidental divergence |
| invariants with `>3` enforcement sites | `4` | `3` | `-1` | the old SQL text-function seam is less drift-prone after the new spec table, leaving continuation and route as the main broad repeated-invariant families plus the fluent owner-local wrapper seam |
| error-construction families with `>3` custom mappings | `1` | `1` | `0` | owner-local projection/lowering mapping remains the only material custom-mapping family |
| drift surface reduction estimate | `medium` | `medium-low` | improved | recent refactors cooled the old SQL duplication ladder without creating a new same-owner hotspot of comparable size |
| estimated LoC reduction range (conservative) | `26-46` | `24-44` | slightly decreased | remaining work is still local, but the highest-risk SQL seam is smaller than it was on April 13 |

High-risk ledger not required (`total high-risk divergence patterns = 0`).

## STEP 9A — Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 6 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 20 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 2 | two same-owner consolidation candidates with high behavioral-equivalence confidence and safe local extraction/unification boundaries |
| boundary-protected findings count | 8 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; this run is comparable to the 2026-04-13 baseline.

## 2. Mode A summary: high-impact consolidation opportunities

- There is no new high-risk divergence pattern in the current tree.
- The strongest owner-local target is still [projection.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/select/projection.rs), but it is less urgent than it was on April 13 because the new `TextFnSpec` table already collapsed a chunk of the older text-function drift surface.
- The most practical cleanup target is now [terminals.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/fluent/load/terminals.rs), where the non-paged execution/explain/projection shells still repeat small owner-local adapter patterns over a large terminal API surface.

## 3. Mode A summary: medium opportunities

- Route hint helpers in [load.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/hints/load.rs) and [aggregate.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/hints/aggregate.rs) still repeat small same-owner capability/fetch-hint plumbing.
- [projection.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/select/projection.rs) still carries a local arithmetic/round lowering ladder even after the text-function cleanup.

## 4. Mode A summary: low/cosmetic opportunities

- [runtime/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/runtime/mod.rs) is still large, but the recent compare split converted most of the earlier DRY pressure into straightforward owner-local execution code rather than a live duplication seam.
- [store/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/store/mod.rs) remains dense but structurally contained behind the persistence boundary.

## 5. Mode B summary: protective redundancies (keep separate)

- Cursor contract definition vs transport/application remains intentionally split.
- Route capability derivation vs route hint consumption remains intentionally split.
- Commit marker store-envelope checks vs marker payload codec remain intentionally split.
- Predicate capability meaning vs runtime/index application remains intentionally split.

## 6. Dangerous consolidations (do not merge)

- Do not merge cursor semantic ownership into planner/runtime/session transport code.
- Do not merge route capability derivation into hint or execution consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not move predicate capability meaning out of [capability.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/capability.rs).

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- patterns found: `6`
- high-risk divergence patterns: `0`
- boundary-protected patterns: `4`
- drift surface reduction estimate: `medium-low`
- conservative LoC reduction: `24-44`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `6`
- classified findings: `20`
- high-confidence candidates: `2`
- boundary-protected findings: `8`

## 9. Architectural risk summary

- Current DRY pressure is moderate but improving.
- The dangerous cross-layer duplication families are still protective, not signs of owner drift.
- The remaining credible consolidation work is owner-local and does not require reopening planner/executor/session/persistence boundaries.

## 10. DRY risk index (1-10, lower is better)

**4.3/10**

Interpretation:

- Risk remains in the moderate band because the runtime still has two meaningful owner-local consolidation candidates and several intentionally repeated invariant families.
- Risk is lower than the earlier SQL-heavy read because the new projection-lowering table already removed part of the former text-function drift surface.
- Risk stays well below the high band because there are still `0` high-risk divergence patterns and the cross-layer duplication that remains is boundary-protective.

## 11. Verification Readout

- comparability status: `comparable`
- mandatory sections present: `yes`
- status: `PASS`

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
