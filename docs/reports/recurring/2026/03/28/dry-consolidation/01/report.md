# DRY Consolidation Audit - 2026-03-28

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-26/dry-consolidation.md`
- code snapshot identifier: `d38b29fa`
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

- `docs/audits/reports/2026-03/2026-03-28/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-03/2026-03-28/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`
- `/tmp/dry-runtime-metrics-2026-03-28.tsv`

## STEP 0 — Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-03/2026-03-26/dry-consolidation.md` | same | none | yes |
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
| predicate capability classification + consumption | `db/predicate/capability.rs`; `db/index/predicate/compile.rs`; `db/executor/preparation.rs`; `db/executor/explain/descriptor.rs` | `predicate/capability.rs:97,123,131,143,194`; `index/predicate/compile.rs:54,94`; `executor/preparation.rs:49,88`; `executor/explain/descriptor.rs:63,170,243,914,922` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/cursor/validation.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs`; `db/executor/executable_plan.rs`; `db/executor/continuation/scalar.rs` | `cursor/mod.rs:89,106,122,137,185,217,245,264`; `cursor/spine.rs:83,123,165,180`; `query/plan/continuation.rs:232,256,287,311,336,360,446`; `session/query.rs:139,193`; `executor/executable_plan.rs:115,167,186,202,218,261,506,690`; `executor/continuation/scalar.rs:151,198,273` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/route/capability.rs`; `db/executor/route/contracts/capabilities.rs`; `db/executor/route/planner/entrypoints.rs`; `db/executor/route/hints/load.rs`; `db/executor/route/hints/aggregate.rs`; `db/executor/continuation/capabilities.rs` | `route/capability.rs:28,100,118`; `route/contracts/capabilities.rs:21`; `route/planner/entrypoints.rs:83`; `route/hints/load.rs:23,42,63`; `route/hints/aggregate.rs:23,35,69`; `continuation/capabilities.rs:11,28,47` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope + size guards | `db/commit/store.rs`; `db/commit/marker.rs` | `commit/store.rs:52,57,64,68,102,125,132,179,186,199,211`; `commit/marker.rs:181,232,253,297` | Defensive duplication | no | yes | yes | yes | high | low-medium | low-medium |
| dense row slot image staging | `db/data/persisted_row.rs` | `persisted_row.rs:519,548,572,580,663,677,687,1408,1495` | Boilerplate duplication | yes | yes | no | yes | high | medium | medium |
| persisted slot payload helper ladder | `db/data/persisted_row.rs` | `persisted_row.rs:1194,1206,1230,1244,1256,1274,1288,1310,1322,1358,1376` | Evolution drift duplication | yes | yes | no | yes | medium-high | medium | medium |

## STEP 2A — Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate execution capability classification | 4 | predicate authority, index predicate compile, executor preparation, executor explain | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 7 | cursor, planner continuation, session, executor continuation | yes | no | yes | yes | yes (`db/cursor/mod.rs`) | high | medium | cursor boundary | medium-low |
| route capability snapshot propagation | 6 | route capability, route contracts, route hints, continuation capability projection | yes | no | yes | yes | yes (`db/executor/route/capability.rs`) | high | medium | route boundary | medium |
| commit marker envelope enforcement | 2 logical owners | commit store, commit marker payload codec | yes | no | yes | yes | yes (`db/commit/store.rs`) | high | low | commit store boundary | low-medium |
| canonical slot image emission and replay | 1 owner family | persisted row canonicalization, serialized patch staging, row re-emission | no | yes | yes | no | yes (`db/data/persisted_row.rs`) | high | low-medium | persisted-row boundary | medium |
| persisted leaf/scalar/custom slot helper ladder | 1 owner family | persisted row leaf codec, scalar codec, custom value codec | no | yes | yes | no | yes (`db/data/persisted_row.rs`) | medium-high | medium | persisted-row boundary | medium |

## STEP 3A — Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/data/persisted_row.rs` | `3350` | `2` | yes | under-splitting | canonical row staging, serialized patch staging, structural decode/encode helpers, and dense fixture-local negative cases now co-locate inside one owner file and amplify same-owner DRY pressure | high |
| `crates/icydb-core/src/db/predicate/runtime.rs` | `1681` | `2` | yes | under-splitting | canonical execution plus generic/scalar evaluation still co-locate most predicate runtime branching | medium |
| `crates/icydb-core/src/db/executor/explain/descriptor.rs` | `1178` | `1` | no | safety-neutral | explain still consumes the canonical predicate capability profile directly instead of re-deriving semantics, but it remains large | low-medium |
| `crates/icydb-core/src/db/commit/store.rs` | `796` | `1` | no | safety-neutral | stable envelope validation and store orchestration remain dense but owner-local | low-medium |
| `crates/icydb-core/src/db/predicate/capability.rs` | `493` | `1` | yes | under-splitting | scalar and index capability classification remain correctly centralized but are dense enough to deserve continued owner-local discipline | medium-low |
| `crates/icydb-core/src/db/cursor/error.rs` | `421` | `1` | yes | safety-neutral | constructor-rich cursor error surface is owner-local and bounded | low-medium |
| `crates/icydb-core/src/db/cursor/mod.rs` | `319` | `1` | no | safety-neutral | cursor contract transport is spread, but the defining authority stays centralized | low-medium |
| `crates/icydb-core/src/db/executor/route/planner/entrypoints.rs` + `db/executor/route/hints/{load,aggregate}.rs` | `439` | `1` | yes | safety-neutral | capability snapshot forwarding and bounded fetch hint gating still repeat small same-owner call patterns | low-medium |

## STEP 4A — Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| predicate capability meaning | `db/predicate/capability.rs` | yes | `predicate/capability.rs`; `index/predicate/compile.rs`; `executor/preparation.rs`; `executor/explain/descriptor.rs` | defining + application + transport snapshot + explain rendering | no | yes | 4 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` | yes | `cursor/mod.rs`; `cursor/spine.rs`; `query/plan/continuation.rs`; `session/query.rs`; `executor/executable_plan.rs`; `executor/continuation/scalar.rs` | defining + validating + transport + defensive re-checking | no | yes | 6 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/route/capability.rs` | yes | `route/capability.rs`; `route/contracts/capabilities.rs`; `route/planner/entrypoints.rs`; `route/hints/load.rs`; `route/hints/aggregate.rs`; `continuation/capabilities.rs` | defining + transport + application | no | yes | 6 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store.rs` | yes | `commit/store.rs`; `commit/marker.rs` | defining + defensive re-checking | no | yes | 2 | Safety-enhancing | low-medium | low-medium |
| canonical slot image completeness | `db/data/persisted_row.rs` | yes | `SlotBufferWriter`; `SerializedPatchWriter`; `emit_raw_row_from_slot_payloads`; `dense_canonical_slot_image_from_serialized_patch`; `canonical_row_from_raw_row` | defining + validating + defensive re-checking + replay normalization | yes | partially | 5 | Consolidation candidate | medium | medium |
| persisted slot payload leaf/scalar/custom encode-decode contract | `db/data/persisted_row.rs` | yes | `encode_persisted_slot_payload`; `encode_persisted_scalar_slot_payload`; `encode_persisted_option_scalar_slot_payload`; `decode_persisted_slot_payload`; `decode_persisted_non_null_slot_payload`; `decode_persisted_option_slot_payload`; `decode_persisted_scalar_slot_payload`; `decode_persisted_option_scalar_slot_payload`; `encode/decode_persisted_custom*_slot_payload` | defining + wrapper validation + typed adaptation | yes | no | 10 | Consolidation candidate | medium | medium |

## STEP 5A — Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability fallback to runtime/index/explain policy | `db/predicate/capability.rs`; `db/index/predicate/compile.rs`; `db/executor/preparation.rs`; `db/executor/explain/descriptor.rs` | low | no | no | no | yes | high | already consolidated | low |
| cursor continuation mismatch mapping | `db/cursor/error.rs`; `db/cursor/mod.rs`; `db/query/plan/continuation.rs`; `db/executor/executable_plan.rs` | low | no | low | no | yes | high | boundary-sensitive | low-medium |
| commit marker envelope failure mapping | `db/commit/store.rs`; `db/commit/marker.rs` | low | no | no | no | yes | high | boundary-protected | low-medium |
| row slot image completeness failures | `db/data/persisted_row.rs` | medium | no | no | yes | yes | high | safe local unification | medium |
| persisted slot leaf/scalar/custom wrapper failures | `db/data/persisted_row.rs` | medium | no | no | yes | yes | medium-high | safe local unification | medium |

## STEP 6B — Boundary-Protective Redundancy

Evidence mode: `classified`

| Area [M] | Duplication Sites [M] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Protective Rationale [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability classifier vs runtime/index/explain application | `db/predicate/capability.rs`; `db/index/predicate/compile.rs`; `db/executor/preparation.rs`; `db/executor/explain/descriptor.rs` | no | yes | yes | classifier owns semantic meaning; compile/preparation/explain own application and rendering policy only | medium |
| cursor contract definition vs planner/runtime/session transport | `db/cursor/mod.rs`; `db/cursor/spine.rs`; `db/query/plan/continuation.rs`; `db/session/query.rs`; `db/executor/executable_plan.rs`; `db/executor/continuation/scalar.rs` | no | yes | yes | preserves one continuation meaning while keeping wire decode, planner validation, executor revalidation, and session transport distinct | high |
| route capability derivation vs route hint consumption | `db/executor/route/capability.rs`; `db/executor/route/contracts/capabilities.rs`; `db/executor/route/planner/entrypoints.rs`; `db/executor/route/hints/*`; `db/executor/continuation/capabilities.rs` | no | yes | yes | keeps route-owned capability reasoning separate from hint application and continuation transport | high |
| commit marker stable envelope vs payload codec | `db/commit/store.rs`; `db/commit/marker.rs` | no | yes | yes | store owns the persisted trust boundary; marker owns the row-op payload shape | high |

## STEP 7B — Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| row slot image completion and re-emission compression | `db/data/persisted_row.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | persisted-row boundary | low-medium | medium | 12-20 | medium-low |
| persisted slot payload helper ladder compression | `db/data/persisted_row.rs` | Evolution drift duplication | yes | yes | no | yes | safe local unification | persisted-row boundary | medium | medium | 18-30 | medium |
| route capability snapshot call-site compression | `db/executor/route/planner/entrypoints.rs`; `db/executor/route/hints/{load,aggregate}.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | route boundary | low-medium | low | 6-10 | low-medium |

## STEP 8B — Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| cursor contract definition and planner/runtime/session transports | split preserves one semantic owner while keeping decode, validation, and transport boundaries explicit | yes | do not merge | high |
| route capability derivation and planner/hint/continuation consumption | split preserves route-owned capability reasoning and prevents policy re-derivation in consumers | yes | do not merge | high |
| commit marker store envelope and payload codec | split preserves the persistence trust boundary and failure classification edge | yes | do not merge | high |
| predicate capability meaning and explain/runtime/index application | split preserves one semantic authority while keeping consumer policy application local | yes | do not merge | medium |

## STEP 9 — Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | `5` | `6` | `+1` | one additional same-owner helper family surfaced inside `persisted_row.rs` |
| total high-risk divergence patterns | `0` | `0` | `0` | no high-risk drift-triggering duplication |
| same-layer accidental duplication count | `1` | `2` | `+1` | `persisted_row.rs` now carries two meaningful same-owner consolidation candidates |
| cross-layer intentional duplication count | `3` | `3` | `0` | remaining cross-layer duplication is still mostly protective transport/application separation |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `4` | `4` | `0` | dominant cross-layer pattern remains protective redundancy rather than accidental divergence |
| invariants with `>3` enforcement sites | `2` | `4` | `+2` | `persisted_row` now has broader repeated owner-local invariants alongside continuation and route capability |
| error-construction families with `>3` custom mappings | `0` | `1` | `+1` | persisted slot helper wrappers now form one material owner-local mapping family |
| drift surface reduction estimate | `high` | `medium-high` | softened | predicate/continuation/route remain contained, but persisted-row same-owner pressure materially increased |
| estimated LoC reduction range (conservative) | `10-18` | `36-60` | increased | the highest-value remaining work is now concentrated inside one owner file rather than across layers |

High-risk ledger not required (`total high-risk divergence patterns = 0`).

## STEP 9A — Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 6 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 20 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 2 | two same-owner `persisted_row` consolidation candidates with clear authority boundaries |
| boundary-protected findings count | 8 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; this run is comparable to the 2026-03-26 baseline.

## 2. Mode A summary: high-impact consolidation opportunities

- `persisted_row.rs` is now the clear DRY hotspot in the runtime tree.
- The file has grown from `1512` total lines in the 2026-03-26 report to `3350` total lines in the current tree.
- The previous report recorded one owner-local row-slot staging seam. The current tree now has two:
  - dense slot image emission and replay
  - the leaf/scalar/custom slot helper ladder
- Both seams stay inside one authority owner and can be reduced without crossing planner/executor/persistence boundaries.

## 3. Mode A summary: medium opportunities

- Route capability snapshot forwarding is still mildly repetitive across planner entrypoints and hint modules, but it remains small and same-owner.
- Predicate capability pressure is still dense, but the important semantic consolidation already happened. The remaining duplication there is mostly transport/application, not authority drift.

## 4. Mode A summary: low/cosmetic opportunities

- `cursor/error.rs`, `cursor/mod.rs`, and `commit/store.rs` remain dense but structurally contained.
- These should not drive opportunistic cleanup unless they are already touched for other work.

## 5. Mode B summary: protective redundancies (keep separate)

- Cursor contract definition vs transport/application remains intentionally split.
- Route capability derivation vs route hint consumption remains intentionally split.
- Commit marker store-envelope checks vs marker payload codec remain intentionally split.
- Predicate capability meaning vs explain/runtime/index application remains intentionally split.

## 6. Dangerous consolidations (do not merge)

- Do not merge cursor semantic ownership into planner/runtime/session transport code.
- Do not merge route capability derivation into hint or continuation consumers.
- Do not merge commit store envelope validation into payload codec logic.
- Do not move predicate capability meaning out of `db/predicate/capability.rs`.

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- patterns found: `6`
- high-risk divergence patterns: `0`
- boundary-protected patterns: `4`
- drift surface reduction estimate: `medium-high`
- conservative LoC reduction: `36-60`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `6`
- classified findings: `20`
- high-confidence candidates: `2`
- boundary-protected findings: `8`

## 9. Architectural risk summary

- Cross-layer DRY pressure is still low and well-contained.
- The current DRY risk is not distributed across the architecture; it is concentrated inside one owner module: `db/data/persisted_row.rs`.
- That is a better failure mode than the older cross-layer duplication families, but it is now large enough that further owner-local compression should be treated as a real follow-up and not as optional polish.

## 10. DRY risk index (1-10, lower is better)

- **4.4/10** (`low-to-moderate risk / pressure concentrated in one owner`)

## 11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `scripts/audit/runtime_metrics.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo clippy -p icydb-core --all-targets -- -D warnings` -> PASS
- `cargo test -p icydb-core db::data::persisted_row::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::cursor::tests -- --nocapture` -> PASS
- audit status: **PASS**
