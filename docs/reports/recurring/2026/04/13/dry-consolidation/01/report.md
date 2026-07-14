# DRY Consolidation Audit - 2026-04-13

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-28/dry-consolidation.md`
- code snapshot identifier: `562f320cd`
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

- `docs/audits/reports/2026-04/2026-04-13/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-04/2026-04-13/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`
- `/tmp/dry-runtime-metrics-2026-04-13.tsv`

## STEP 0 — Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-03/2026-03-28/dry-consolidation.md` | same | none | yes |
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
| predicate capability classification + consumption | `db/predicate/capability.rs`; `db/predicate/runtime.rs`; `db/index/predicate/compile.rs` | `predicate/capability.rs:124`; `predicate/runtime.rs:215-220`; `index/predicate/compile.rs:51-53,130-132` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs`; `db/executor/planning/continuation/scalar.rs` | `cursor/mod.rs:141-173`; `cursor/spine.rs:164-322`; `query/plan/continuation.rs:313-430`; `session/query.rs:381-467`; `executor/planning/continuation/scalar.rs:209-223` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/load.rs`; `db/executor/planning/route/hints/aggregate.rs` | `route/capability.rs`; `route/contracts/capabilities.rs`; `route/hints/load.rs:37-138`; `route/hints/aggregate.rs:20-75` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope + size guards | `db/commit/store.rs`; `db/commit/marker.rs` | `commit/store.rs:70-106,165-241,265-427`; `commit/marker.rs:29-35,180-317` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| bounded text projection terminal wrapper family | `db/query/fluent/load/terminals.rs` | `terminals.rs:218-238,713,982,1039,1096` | Boilerplate duplication | yes | yes | no | no | high | low-medium | low-medium |
| SQL text-function lowering + literal validation ladder | `db/sql/lowering/select/projection.rs` | `projection.rs:133-196,220-336` | Evolution drift duplication | yes | yes | no | yes | medium-high | medium | medium |

## STEP 2A — Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability classification | 3 | predicate authority, runtime compilation, index predicate compile | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 5 | cursor, planner continuation, session, executor continuation | yes | no | yes | yes | yes (`db/cursor/mod.rs`) | high | medium | cursor boundary | medium-low |
| route capability snapshot propagation | 4 | route capability, route contracts, route hints | yes | no | yes | yes | yes (`db/executor/planning/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store.rs`) | high | low | commit store boundary | low-medium |
| bounded text projection page/value wrapper family | 1 owner family | fluent load terminal projection wrappers | no | yes | yes | no | yes (`db/query/fluent/load/terminals.rs`) | high | low | fluent load boundary | low-medium |
| SQL text-function projection lowering/validation family | 1 owner family | SQL projection lowering, function-to-Expr mapping, literal validation | no | yes | yes | no | yes (`db/sql/lowering/select/projection.rs`) | medium-high | low-medium | SQL lowering boundary | medium |

## STEP 3A — Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/predicate/runtime.rs` | `1859` | `1` | yes | under-splitting | runtime compilation, scalar/generic execution, and slot-marking remain correctly owner-local but are dense enough that future same-owner duplication would spread quickly | medium |
| `crates/icydb-core/src/db/query/fluent/load/terminals.rs` | `1258` | `1` | yes | under-splitting | repeated text-projection payload wrappers are small, but they sit inside an already large terminal owner | medium-low |
| `crates/icydb-core/src/db/sql/lowering/select/projection.rs` | `353` | `1` | yes | under-splitting | function-to-Expr lowering and literal-contract validation now live together with a repeated same-owner mapping ladder | medium |
| `crates/icydb-core/src/db/query/intent/query.rs` | `1270` | `0` | yes | safety-neutral | large API surface, but current density is mostly deliberate typed wrapper surface rather than meaningful semantic duplication | low-medium |
| `crates/icydb-core/src/db/commit/store.rs` | `1079` | `1` | no | safety-neutral | stable envelope validation and store orchestration remain dense but boundary-protected | low-medium |
| `crates/icydb-core/src/db/executor/planning/route/hints/load.rs` | `169` | `1` | yes | safety-neutral | bounded fetch-hint helpers still repeat small same-owner call patterns but remain localized | low-medium |

## STEP 4A — Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| predicate capability meaning | `db/predicate/capability.rs` | yes | `predicate/capability.rs`; `predicate/runtime.rs`; `index/predicate/compile.rs` | defining + runtime admission + index admission | no | yes | 3 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` | yes | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `session/query.rs`; `executor/planning/continuation/scalar.rs` | defining + validating + transport + defensive re-checking | no | yes | 5 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/planning/route/capability.rs` | yes | `route/capability.rs`; `route/contracts/capabilities.rs`; `route/hints/load.rs`; `route/hints/aggregate.rs` | defining + transport + application | no | yes | 4 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store.rs` | yes | `commit/store.rs`; `commit/marker.rs` | defining + defensive re-checking | no | yes | 2 | Safety-enhancing | low-medium | low-medium |
| fluent text projection value/page wrapper contract | `db/query/fluent/load/terminals.rs` | yes | `project_terminal_values`; `project_terminal_optional_value`; `project_terminal_values_with_ids` | typed adaptation + payload shaping | yes | no | 3 | Consolidation candidate | low-medium | low-medium |
| SQL text-function projection literal contract | `db/sql/lowering/select/projection.rs` | yes | `lower_text_function_expr`; `validate_text_function_literal_contract`; `validate_text_function_primary_literal`; `validate_text_function_second_literal`; `validate_text_function_numeric_literals`; `sql_text_function_to_function` | defining + wrapper validation + typed adaptation | yes | no | 6 | Consolidation candidate | medium | medium |

## STEP 5A — Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability fallback to runtime/index policy | `db/predicate/capability.rs`; `db/predicate/runtime.rs`; `db/index/predicate/compile.rs` | low | no | no | no | yes | high | already consolidated | low |
| cursor continuation mismatch mapping | `db/cursor/error.rs`; `db/cursor/mod.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs` | low | no | low | no | yes | high | boundary-sensitive | low-medium |
| commit marker envelope failure mapping | `db/commit/store.rs`; `db/commit/marker.rs` | low | no | no | no | yes | high | boundary-protected | low-medium |
| fluent text projection wrapper failures | `db/query/fluent/load/terminals.rs` | low-medium | no | no | yes | yes | high | safe local unification | low-medium |
| SQL text-function literal validation failures | `db/sql/lowering/select/projection.rs` | medium | no | no | yes | yes | medium-high | safe local unification | medium |

## STEP 6B — Boundary-Protective Redundancy

Evidence mode: `classified`

| Area [M] | Duplication Sites [M] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Protective Rationale [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability classifier vs runtime/index application | `db/predicate/capability.rs`; `db/predicate/runtime.rs`; `db/index/predicate/compile.rs` | no | yes | yes | classifier owns semantic meaning; runtime and index compile own admission/application only | medium |
| cursor contract definition vs planner/runtime/session transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs`; `db/executor/planning/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, executor revalidation, and session transport distinct | high |
| route capability derivation vs route hint consumption | `db/executor/planning/route/capability.rs`; `db/executor/planning/route/contracts/capabilities.rs`; `db/executor/planning/route/hints/*` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and downstream execution contracts | high |
| commit marker stable envelope vs payload codec | `db/commit/store.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary; marker owns the row-op payload shape | high |

## STEP 7B — Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| fluent text projection terminal wrapper compression | `db/query/fluent/load/terminals.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | fluent load boundary | low | low-medium | 8-14 | low-medium |
| SQL text-function lowering/validation ladder compression | `db/sql/lowering/select/projection.rs` | Evolution drift duplication | yes | yes | no | yes | safe local unification | SQL lowering boundary | low-medium | medium | 14-24 | medium |
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
| total duplication patterns found | `6` | `6` | `0` | duplication pressure stayed flat, but the dominant same-owner hotspots moved |
| total high-risk divergence patterns | `0` | `0` | `0` | no high-risk drift-triggering duplication |
| same-layer accidental duplication count | `2` | `2` | `0` | there are still two credible same-owner consolidation candidates, but they are no longer concentrated in one `persisted_row` monolith |
| cross-layer intentional duplication count | `3` | `3` | `0` | remaining cross-layer duplication is still protective transport/application separation |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `4` | `4` | `0` | dominant cross-layer pattern remains protective redundancy rather than accidental divergence |
| invariants with `>3` enforcement sites | `4` | `4` | `0` | cursor, route, commit, and SQL text-function owners still carry the broadest repeated invariants |
| error-construction families with `>3` custom mappings | `1` | `1` | `0` | the SQL text-function lowering ladder is now the clearest owner-local mapping family |
| drift surface reduction estimate | `medium-high` | `medium` | softened | the big `persisted_row` hotspot cooled, but new owner-local SQL/fluent duplication remains worth trimming |
| estimated LoC reduction range (conservative) | `36-60` | `26-46` | decreased | remaining work is narrower and more localized than the March persisted-row hotspot |

High-risk ledger not required (`total high-risk divergence patterns = 0`).

## STEP 9A — Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 6 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 20 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 2 | two same-owner consolidation candidates with clear owner-local authority |
| boundary-protected findings count | 8 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; this run is comparable to the 2026-03-28 baseline.

## 2. Mode A summary: high-impact consolidation opportunities

- The broad March `persisted_row` hotspot is no longer the dominant DRY risk in the runtime tree.
- The clearest same-owner candidate is now `db/sql/lowering/select/projection.rs`, where text-function lowering, function-to-`Expr` mapping, and literal validation still form one medium-risk local ladder.
- The next candidate is smaller and lower-risk: `db/query/fluent/load/terminals.rs` still repeats three near-identical text-projection wrapper shapes for `Vec<Value>`, `Option<Value>`, and `(Id<E>, Value)` payloads.

## 3. Mode A summary: medium opportunities

- Route hint helpers still repeat a small amount of same-owner fetch-hint plumbing across load and aggregate hint modules.
- `predicate/runtime.rs` remains large enough that future cleanup should prefer owner-local compression over adding new helper ladders.

## 4. Mode A summary: low/cosmetic opportunities

- `query/intent/query.rs` is large, but its density is mostly deliberate typed wrapper surface rather than meaningful semantic duplication.
- `commit/store.rs` remains dense but structurally contained behind the persistence boundary.

## 5. Mode B summary: protective redundancies (keep separate)

- Cursor contract definition vs transport/application remains intentionally split.
- Route capability derivation vs route hint consumption remains intentionally split.
- Commit marker store-envelope checks vs marker payload codec remain intentionally split.
- Predicate capability meaning vs runtime/index application remains intentionally split.

## 6. Dangerous consolidations (do not merge)

- Do not merge cursor semantic ownership into planner/runtime/session transport code.
- Do not merge route capability derivation into hint or execution consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not move predicate capability meaning out of `db/predicate/capability.rs`.

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- patterns found: `6`
- high-risk divergence patterns: `0`
- boundary-protected patterns: `4`
- drift surface reduction estimate: `medium`
- conservative LoC reduction: `26-46`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `6`
- classified findings: `20`
- high-confidence candidates: `2`
- boundary-protected findings: `8`

## 9. Architectural risk summary

- Current DRY pressure is moderate and localized rather than broad.
- The dangerous cross-layer duplication families are still protective, not signs of owner drift.
- The remaining credible consolidation work is owner-local and does not require reopening planner/executor/session boundaries.
