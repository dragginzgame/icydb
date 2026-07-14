# DRY Consolidation Audit - 2026-03-26

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/dry-consolidation.md`
- code snapshot identifier: `a956de44`
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

- `docs/audits/reports/2026-03/2026-03-26/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-03/2026-03-26/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`

## STEP 0 — Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-03/2026-03-24/dry-consolidation.md` | same | none | yes |
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
| predicate capability classification + consumption | `db/predicate/capability.rs`; `db/predicate/runtime.rs`; `db/index/predicate/compile.rs`; `db/executor/preparation.rs`; `db/executor/explain/descriptor.rs`; `db/predicate/mod.rs` | `predicate/capability.rs:99,118`; `predicate/runtime.rs:211`; `index/predicate/compile.rs:49,89,160`; `executor/preparation.rs:52`; `executor/explain/descriptor.rs:70,187,843` | Intentional boundary duplication | no | yes | yes | yes | high | low | low |
| continuation cursor contract transport | `db/cursor/mod.rs`; `db/session/query.rs`; `db/executor/executable_plan.rs`; `db/query/plan/continuation.rs`; `db/executor/continuation/engine.rs`; `db/cursor/validation.rs`; `db/cursor/tests.rs`; `db/query/fingerprint/shape_signature/tests.rs` | `cursor/mod.rs:67,196,231,245,264`; `executor/executable_plan.rs:172,191,207`; `query/plan/continuation.rs:311,348,360` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| route capability snapshot forwarding | `db/executor/route/capability.rs`; `db/executor/route/mod.rs`; `db/executor/route/planner/entrypoints.rs`; `db/executor/route/planner/feasibility/mod.rs`; `db/executor/route/hints/{load,aggregate}.rs`; `db/executor/route/contracts/capabilities.rs`; `db/executor/continuation/capabilities.rs` | `route/capability.rs:29,62,84`; `route/mod.rs:18,19,20`; `route/planner/entrypoints.rs:66` | Intentional boundary duplication | no | yes | yes | yes | high | medium | medium |
| commit marker envelope + size guards | `db/commit/store.rs` | `commit/store.rs:52,57,64,69,91,112,170,189,199,242,271` | Defensive duplication | yes | yes | yes | yes | high | low-medium | low-medium |
| dense row slot image staging | `db/data/persisted_row.rs` | `persisted_row.rs:351,361,377,558,586,1123,1132,1175,1210,1226,2222` | Boilerplate duplication | yes | yes | no | yes | high | low | low-medium |

## STEP 2A — Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate execution capability classification | 6 | predicate runtime, predicate authority, index predicate compile, executor preparation, executor explain | yes | no | yes | yes | yes (`db/predicate/capability.rs`) | high | low | predicate boundary | low |
| continuation meaning transport and revalidation | 8 | cursor, session, planner continuation, executor continuation | yes | no | yes | yes | yes (`db/cursor/mod.rs`) | high | medium | cursor boundary | medium-low |
| route capability snapshot propagation | 7 | route capability, route planning, route hints, continuation capability projection | yes | no | yes | yes | yes (`db/executor/route/capability.rs`) | medium-high | medium | route boundary | medium |
| commit marker envelope enforcement | 1 logical family in 1 owner module | commit store, commit marker codec | no | yes | yes | yes | yes (`db/commit/store.rs`) | high | low | commit store boundary | low-medium |
| dense row / serialized patch slot image staging | 3 | persisted-row row encoding, persisted-row patch staging, persisted-row negative fixture generation | no | yes | yes | no | yes (`db/data/persisted_row.rs`) | high | low | persisted-row boundary | low-medium |

## STEP 3A — Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/data/persisted_row.rs` | 1512 | 1 | yes | under-splitting | canonical row staging, structural read validation, slot caching, and negative-row fixture seams still co-locate inside one owner file | medium |
| `crates/icydb-core/src/db/predicate/runtime.rs` | 983 | 2 | yes | under-splitting | canonical execution plus generic/scalar evaluation still co-locate most predicate runtime branching | medium |
| `crates/icydb-core/src/db/predicate/capability.rs` | 297 | 2 | yes | under-splitting | scalar and index capability classification remain correctly owner-local but dense | medium-low |
| `crates/icydb-core/src/db/executor/explain/descriptor.rs` | 997 | 1 | no | safety-neutral | explain still consumes the canonical capability profile directly instead of re-deriving predicate compatibility | low |
| `crates/icydb-core/src/db/cursor/error.rs` | 311 | 1 | yes | safety-neutral | dense constructor surface remains owner-local | low-medium |
| `crates/icydb-core/src/db/cursor/mod.rs` | 260 | 1 | no | safety-neutral | cursor contract transport is spread, but the defining authority stays centralized | low-medium |
| `crates/icydb-core/src/db/index/predicate/compile.rs` | 216 | 1 | no | over-splitting reduced | compile policy still consumes capability classification instead of re-deriving eligibility | low |
| `crates/icydb-core/src/db/executor/route/planner/entrypoints.rs` | 179 | 1 | no | safety-neutral | route capability snapshots still thread through planner entrypoints and hints | low-medium |

## STEP 4A — Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| predicate capability meaning | `db/predicate/capability.rs` | yes | `predicate/capability.rs`; `predicate/runtime.rs`; `index/predicate/compile.rs`; `executor/preparation.rs`; `executor/explain/descriptor.rs` | defining + validating + application + transport snapshot + explain rendering | no | yes | 5 | Safety-enhancing | low | low |
| continuation contract meaning | `db/cursor/mod.rs` | yes | `cursor/mod.rs`; `session/query.rs`; `executor/executable_plan.rs`; `query/plan/continuation.rs`; `executor/continuation/engine.rs` | defining + transport + defensive re-checking | no | yes | 5 | Safety-enhancing | medium-low | medium-low |
| route capability snapshot interpretation | `db/executor/route/capability.rs` | yes | `route/capability.rs`; `route/planner/*`; `route/hints/*`; `continuation/capabilities.rs` | defining + application | no | yes | 5 | Safety-enhancing | medium | medium |
| commit marker canonical envelope | `db/commit/store.rs` | yes | `commit/store.rs`; `commit/marker.rs` | defining + defensive re-checking | partially | yes | 2 | Safety-enhancing | low-medium | low-medium |
| canonical slot image completeness | `db/data/persisted_row.rs` | yes | `SlotBufferWriter`; `SerializedPatchWriter`; `StructuralSlotReader::from_raw_row` | defining + validating + defensive re-checking | yes | partially | 3 | Consolidation candidate | low-medium | low-medium |

## STEP 5A — Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability fallback to runtime/index/explain policy | `db/predicate/capability.rs`; `db/predicate/runtime.rs`; `db/index/predicate/compile.rs`; `db/executor/preparation.rs`; `db/executor/explain/descriptor.rs` | low | no | no | no | yes | high | already consolidated | low |
| cursor continuation mismatch mapping | `db/cursor/error.rs`; `db/cursor/mod.rs`; `db/query/plan/continuation.rs`; `db/executor/executable_plan.rs` | low | no | low | no | yes | high | boundary-sensitive | low-medium |
| commit marker envelope failure mapping | `db/commit/store.rs`; `db/commit/marker.rs` | low | no | no | yes | yes | high | owner-local | low |
| row slot image completeness failures | `db/data/persisted_row.rs` | medium | no | no | yes | yes | high | safe local unification | low-medium |

## STEP 6B — Boundary-Protective Redundancy

Evidence mode: `classified`

| Area [M] | Duplication Sites [M] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Protective Rationale [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| predicate capability classifier vs runtime/index/explain application | `db/predicate/capability.rs`; `db/predicate/runtime.rs`; `db/index/predicate/compile.rs`; `db/executor/preparation.rs`; `db/executor/explain/descriptor.rs` | no | yes | yes | classifier owns meaning; runtime, index, preparation, and explain own application/rendering policy only | medium |
| cursor contract definition vs transport/application | `db/cursor/mod.rs`; `db/query/plan/continuation.rs`; `db/executor/executable_plan.rs`; `db/session/query.rs` | no | yes | yes | preserves one continuation meaning while keeping planner/runtime/session transports separate | high |
| route capability derivation vs route hint consumption | `db/executor/route/capability.rs`; `db/executor/route/planner/*`; `db/executor/route/hints/*`; `db/executor/continuation/capabilities.rs` | no | yes | yes | keeps route capability ownership distinct from planning/hint application | high |
| commit marker canonical envelope vs payload codec | `db/commit/store.rs`; `db/commit/marker.rs` | no | yes | yes | store owns stable envelope, marker owns payload shape | high |

## STEP 7B — Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| row slot image completion loop compression | `db/data/persisted_row.rs` | Boilerplate duplication | yes | yes | no | yes | safe local unification | persisted-row boundary | low | low-medium | 4-8 | low |
| route capability snapshot call-site compression | `db/executor/route/planner/entrypoints.rs`; `db/executor/route/hints/{load,aggregate}.rs` | Boilerplate duplication | yes | yes | partially | yes | safe local unification | route boundary | low-medium | low | 6-10 | low-medium |

## STEP 8B — Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| cursor contract definition and planner/runtime/session transports | split preserves one meaning owner while keeping transport/application boundaries explicit | yes | do not merge | high |
| route capability derivation and planner/hint application | split preserves route-owned capability reasoning and prevents policy re-derivation in consumers | yes | do not merge | high |
| commit marker stable envelope and payload codec | split preserves persistence trust boundary | yes | do not merge | high |

## STEP 9 — Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | `4` | `5` | `+1` | one same-owner row-staging seam surfaced after the `0.65` canonical row hardening landed |
| total high-risk divergence patterns | `0` | `0` | `0` | no high-risk drift-triggering duplication |
| same-layer accidental duplication count | `0` | `1` | `+1` | the only new material seam is owner-local inside `persisted_row.rs` |
| cross-layer intentional duplication count | `3` | `3` | `0` | remaining cross-layer duplication is still mostly protective transport/application separation |
| defensive duplication count | `1` | `1` | `0` | commit marker envelope checks remain intentionally duplicated at the store/payload boundary |
| boundary-protected duplication count | `4` | `4` | `0` | dominant cross-layer pattern remains protective redundancy rather than accidental divergence |
| invariants with `>3` enforcement sites | `2` | `2` | `0` | continuation and route capability still have the broadest application surfaces |
| error-construction families with `>3` custom mappings | `0` | `0` | `0` | no broad mapping family re-opened in this audit scope |
| drift surface reduction estimate | `high` | `high` | unchanged | the new row seam is same-owner and does not widen authority spread |
| estimated LoC reduction range (conservative) | `6-10` | `10-18` | increased | the remaining row-local completion seam plus the route-local compression are both safe candidates |

High-risk ledger not required (`total high-risk divergence patterns = 0`).

## STEP 9A — Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 5 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 16 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 1 | one same-owner persisted-row consolidation candidate with clear authority and format boundaries |
| boundary-protected findings count | 8 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; run is comparable to the 2026-03-24 baseline.

## 2. Mode A summary: high-impact consolidation opportunities

- No high-risk same-owner drift family emerged.
- The only credible new consolidation target is the remaining row slot image completion seam inside `db/data/persisted_row.rs`.

## 3. Mode A summary: medium opportunities

- `persisted_row.rs` now owns canonical row staging, serialized patch staging, full structural read validation, and negative-row fixture support. The recent helper extraction reduced the row-local DRY seam, and the remaining completion loop duplication is still safe to compress because it does not cross persistence or query owner boundaries.

## 4. Mode A summary: low/cosmetic opportunities

- Route capability snapshot call sites still have minor local forwarding pressure, but that remains a small same-owner cleanup, not a broad cross-layer seam.

## 5. Mode B summary: protective redundancies (keep separate)

- Cursor contract transport, route capability derivation vs consumption, and commit marker envelope vs payload codec remain protective separations and should stay boundary-scoped.

## 6. Dangerous consolidations (do not merge)

- Do not merge cursor meaning with transport/application, route capability derivation with hint/planner consumption, or commit marker store envelope checks with payload codec logic.

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- Patterns found: `5`
- High-risk divergence patterns: `0`
- Boundary-protected patterns: `4`
- Drift surface reduction estimate: `high`
- Conservative LoC reduction: `10-18`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `5`
- classified findings: `16`
- high-confidence candidates: `1`
- boundary-protected findings: `8`

## 9. Architectural risk summary

- DRY pressure remains low. The only new material seam is same-owner row slot image staging inside `persisted_row.rs`, while the historically larger cross-layer duplication families remain protective and contained.

## 10. DRY risk index (1-10, lower is better)

- **3.8/10** (`low risk / contained pressure`)

## 11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `scripts/audit/runtime_metrics.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo clippy -p icydb-core --all-targets -- -D warnings` -> PASS
- `cargo test -p icydb-core db::data::persisted_row::tests -- --nocapture` -> PASS
- audit status: **PASS**
