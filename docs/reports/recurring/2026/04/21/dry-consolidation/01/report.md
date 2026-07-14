# DRY Consolidation Audit - 2026-04-21

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded)
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-17/dry-consolidation.md`
- code snapshot identifier: `7c1946c04` (`dirty` working tree)
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

- `docs/audits/reports/2026-04/2026-04-21/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-04/2026-04-21/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`

## STEP 0 — Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-04/2026-04-17/dry-consolidation.md` | same | none | yes |
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
| predicate capability classification + consumption | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | `predicate/capability.rs:122-365,480`; `predicate/runtime/mod.rs:19-20,206-207,724`; `index/predicate/compile.rs:14,53-60,82-86,132-139,173-177` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs`; `db/executor/planning/continuation/scalar.rs` | `cursor/mod.rs:57-171`; `cursor/spine.rs:265-329`; `query/plan/continuation.rs:19-573`; `session/query.rs:881-919,954-958,1039-1043,1075-1078`; `executor/planning/continuation/scalar.rs:67-224` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | `route/capability.rs:21-161`; `route/contracts/capabilities.rs:15-39`; `route/hints/load.rs:37-75,138`; `route/hints/aggregate.rs:20-25,31-67,74-108` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope + size guards | `db/commit/store/mod.rs`; `db/commit/marker.rs` | `commit/store/mod.rs:67-105,145-214,344-476,555-708`; `commit/marker.rs:26-35,151-154,180-329,501` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| fluent non-paged terminal strategy/wrapper family | `db/query/fluent/load/terminals.rs` | `terminals.rs:59-256,495-576,679-685,926-932,976-983,1029-1036` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |
| prepared fallback contract inference + template rebinding | `db/sql/lowering/prepare.rs`; `db/session/sql/parameter.rs` | `prepare.rs:21-23,423-442,510-577,640-721,753-907,1095-1489,1519-1648`; `session/sql/parameter.rs:306-331,359,804-906,1021-1256,1894-2079` | Evolution drift duplication | no | yes | no | yes | medium-high | medium-high | medium-high |

## STEP 2A — Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability classification | 3 | predicate authority, runtime compilation, index predicate compile | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 5 | cursor, planner continuation, session, executor continuation | yes | no | yes | yes | yes (`db/cursor/mod.rs`) | high | medium | cursor boundary | medium-low |
| route capability snapshot propagation | 4 | route capability, route contracts, route hints | yes | no | yes | yes | yes (`db/executor/planning/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store/mod.rs`) | high | low | commit store boundary | low-medium |
| fluent non-paged terminal execution/projection wrappers | 1 owner family | fluent load terminal execution, explain, projection adaptation | no | yes | yes | no | yes (`db/query/fluent/load/terminals.rs`) | high | low | fluent load boundary | low-medium |
| prepared fallback parameter contract reasoning | 4 | SQL fallback lowering, prepared template binding, session execution, planner-adjacent expression typing | yes | no | yes | no | yes (`planner` / typed lowering outputs) | medium-high | high | planner-owned semantic outputs with prepared binding kept structural-only | medium-high |

## STEP 3A — Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/sql/lowering/prepare.rs` | `1881` | `1` | no | under-splitting | fallback inference, WHERE/HAVING contract collection, and compare/function-specific reasoning still accumulate inside one very large lowering file, which makes authority contraction hard to localize | high |
| `crates/icydb-core/src/db/session/sql/parameter.rs` | `2128` | `1` | no | under-splitting | binding validation, symbolic-template instantiation, legacy-template rebinding, and expression/predicate walkers share one prepared execution lane, so structural binding work is mixed with semantic-adjacent rebuild logic | high |
| `crates/icydb-core/src/db/query/fluent/load/terminals.rs` | `1184` | `1` | yes | under-splitting | execution, explain, and projection wrapper families still share one owner-local shell, so small repetitive adapters accumulate inside one already-large API boundary | medium |
| `crates/icydb-core/src/db/predicate/runtime/mod.rs` | `985` | `1` | yes | safety-neutral | runtime compilation and evaluation remain dense, but the duplication pressure is still subordinate to the canonical capability boundary | low-medium |
| `crates/icydb-core/src/db/commit/store/mod.rs` | `721` | `1` | no | safety-neutral | stable envelope validation and store orchestration remain dense but boundary-protected | low-medium |
| `crates/icydb-core/src/db/executor/planning/route/hints/load.rs` + `aggregate.rs` | `291` | `1` | yes | safety-neutral | bounded-fetch and capability-hint helpers still repeat small same-owner call shells, but the seam is narrow and localized | low-medium |

## STEP 4A — Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| predicate capability meaning | `db/predicate/capability.rs` | yes | `predicate/capability.rs`; `predicate/runtime/mod.rs`; `index/predicate/compile.rs` | defining + runtime admission + index admission | no | yes | 3 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` | yes | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `session/query.rs`; `executor/planning/continuation/scalar.rs` | defining + validating + transport + defensive re-checking | no | yes | 5 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/planning/route/capability.rs` | yes | `route/capability.rs`; `route/contracts/capabilities.rs`; `route/hints/load.rs`; `route/hints/aggregate.rs` | defining + transport + application | no | yes | 4 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store/mod.rs` | yes | `commit/store/mod.rs`; `commit/marker.rs` | defining + defensive re-checking | no | yes | 2 | Safety-enhancing | low-medium | low-medium |
| fluent non-paged terminal wrapper contract | `db/query/fluent/load/terminals.rs` | yes | `map_non_paged_query_output`; `execute_prepared_numeric_field_terminal`; `execute_prepared_projection_terminal_output`; projection wrapper terminal families | typed adaptation + payload shaping + explain forwarding | yes | no | 4 | Consolidation candidate | low-medium | low-medium |
| prepared parameter contract meaning | planner-owned semantic outputs consumed through prepared lowering/binding | yes | prepare.rs WHERE fallback contract collection; prepare.rs HAVING fallback contract collection; session/sql/parameter.rs template-plan binding; session/sql/parameter.rs predicate/expression rebinding | defining + structural instantiation + defensive binding validation + runtime substitution | no | no | 4 | Consolidation candidate | medium-high | medium-high |

## STEP 5A — Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| commit marker corruption constructors and envelope decode guards | `db/commit/marker.rs`; `db/commit/store/mod.rs` | low | no | no | no | yes | high | protective duplication | low-medium |
| prepared SQL parameter contract and binding failures | `db/sql/lowering/prepare.rs`; `db/session/sql/parameter.rs` | medium | no | yes | no | yes | medium | boundary-sensitive authority contraction | medium |

## STEP 6B — Protective Redundancy Review

Evidence mode: `classified`

| Pattern Family [M] | Files [M] | Same Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Why It Must Stay Split [C] | Behavioral Equivalence Confidence [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability meaning vs runtime/index application | `db/predicate/capability.rs`; `db/predicate/runtime/mod.rs`; `db/index/predicate/compile.rs` | no | yes | yes | capability ownership stays in predicate authority while runtime and index compilation consume the classified result locally | high |
| cursor contract definition vs planner/runtime/session transports | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs`; `db/executor/planning/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, executor revalidation, and session transport distinct | high |
| route capability derivation vs route hint consumption | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/*` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and downstream execution contracts | high |
| commit marker stable envelope vs payload codec | `db/commit/store/mod.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary while marker owns the row-op payload shape | high |

## STEP 7B — Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| prepared fallback authority contraction | `db/sql/lowering/prepare.rs`; `db/session/sql/parameter.rs` | Evolution drift duplication | no | yes | no | yes | boundary-safe authority contraction | planner-owned semantic outputs; prepared execution reduced to binding/substitution/wiring | high | high | 40-90 | medium-high |
| fluent non-paged terminal wrapper compression | `db/query/fluent/load/terminals.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | fluent load boundary | low | low-medium | 10-18 | low-medium |
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
| total duplication patterns found | `6` | `6` | `0` | duplication pressure stayed concentrated in a small set of known families, but one hotspot rotated from projection lowering into prepared fallback |
| total high-risk divergence patterns | `0` | `0` | `0` | no high-risk drift-triggering duplication, but the prepared fallback seam is now the clearest medium-high concern |
| same-layer accidental duplication count | `2` | `1` | `-1` | the old projection-lowering seam cooled, leaving the fluent terminal wrappers as the remaining straightforward owner-local target |
| cross-layer intentional duplication count | `3` | `3` | `0` | the main cross-layer duplication families are still protective transport/application splits |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `4` | `4` | `0` | dominant cross-layer pattern is still protective redundancy rather than accidental layer collapse |
| invariants with `>3` enforcement sites | `3` | `4` | `+1` | prepared parameter contract handling now joins continuation, route, and fluent wrappers as a broad repeated-invariant family |
| error-construction families with `>3` custom mappings | `1` | `1` | `0` | owner-local projection mapping cooled, but prepared parameter failure wording now accounts for most of the remaining boundary-sensitive duplication energy |
| drift surface reduction estimate | `medium-low` | `medium` | worsened | the old projection ladder improved, but the prepared fallback seam now creates a larger and more semantically sensitive drift surface across lowering and session binding |
| estimated LoC reduction range (conservative) | `24-44` | `54-116` | increased | the highest-payoff contraction target is now larger because the prepared fallback seam spans two large files instead of one narrower local ladder |

High-risk ledger not required (`total high-risk divergence patterns = 0`).

## STEP 9A — Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 6 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 20 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 1 | one consolidation target has both meaningful drift-surface reduction and a clearly articulated authority destination |
| boundary-protected findings count | 8 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; this run is comparable to the 2026-04-17 baseline.

## 2. Mode A summary: high-impact consolidation opportunities

- The strongest live consolidation target is now [prepare.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/sql/lowering/prepare.rs:21) plus [parameter.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/session/sql/parameter.rs:306), where prepared fallback contract inference and prepared-template rebinding still duplicate semantic-adjacent reasoning across lowering and session execution.
- This is not a generic layer merge recommendation. The safe contraction is to push semantic truth back behind planner-owned outputs and leave prepared execution structural-only.

## 3. Mode A summary: medium opportunities

- [terminals.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/query/fluent/load/terminals.rs:59) still carries a practical owner-local wrapper compression opportunity around non-paged prepared terminal execution and projection adaptation.
- Route hint helpers in [load.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/hints/load.rs:37) and [aggregate.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/executor/planning/route/hints/aggregate.rs:20) still repeat small same-owner capability/fetch-hint plumbing.

## 4. Mode A summary: low/cosmetic opportunities

- [runtime/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/runtime/mod.rs:206) remains large but its duplication stays subordinate to the predicate capability boundary.
- [store/mod.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/commit/store/mod.rs:423) remains dense but structurally contained behind the persistence boundary.

## 5. Mode B summary: protective redundancies (keep separate)

- Cursor contract definition vs transport/application remains intentionally split.
- Route capability derivation vs route hint consumption remains intentionally split.
- Commit marker store-envelope checks vs marker payload codec remain intentionally split.
- Predicate capability meaning vs runtime/index application remains intentionally split.

## 6. Dangerous consolidations (do not merge)

- Do not merge cursor semantic ownership into planner/runtime/session transport code.
- Do not merge route capability derivation into hint or execution consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not move predicate capability meaning out of [capability.rs](/home/adam/projects/icydb/crates/icydb-core/src/db/predicate/capability.rs:122).

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- patterns found: `6`
- high-risk divergence patterns: `0`
- boundary-protected patterns: `4`
- drift surface reduction estimate: `medium`
- conservative LoC reduction: `54-116`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `6`
- classified findings: `20`
- high-confidence candidates: `1`
- boundary-protected findings: `8`

## 9. Architectural risk summary

- Current DRY pressure is moderate and warmer than the April 17 run.
- The dangerous cross-layer duplication families are still protective, not signs of owner drift.
- The new pressure comes from prepared fallback semantics staying too thick across lowering and session binding, which raises drift surface without yet crossing into high-risk divergence.

## 10. DRY risk index (1-10, lower is better)

**5.1/10**

Interpretation:

- Risk remains in the moderate band because the runtime still has `0` high-risk divergence patterns and the main cross-layer duplication families remain boundary-protective.
- Risk is higher than the prior run because prepared fallback now forms one cross-layer evolution-drift seam across two very large files.
- The most valuable next contraction is the prepared fallback authority cleanup already outlined for `0.112`.

## 11. Verification Readout

- comparability status: `comparable`
- mandatory sections present: `yes`
- status: `PASS`

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
