# DRY Consolidation Audit - 2026-03-15

## Report Preamble

- scope: duplication and consolidation pressure across `crates/icydb-core/src` runtime boundaries (tests excluded)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-12/dry-consolidation.md`
- code snapshot identifier: `39b1d676`
- method tag/version: `DRY-1.2`
- method manifest:
  - `method_version = DRY-1.2`
  - `duplication_taxonomy = DT-1`
  - `owner_layer_taxonomy = OL-1`
  - `invariant_role_model = IR-1`
  - `facade_inclusion_rule = FI-1`
  - `consolidation_safety_model = CS-1`
- comparability status: `non-comparable` (baseline report uses `Method V3`; this run uses the explicit `DRY-1.2` method contract)

## Evidence Artifacts

- `docs/audits/reports/2026-03/2026-03-15/helpers/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-03/2026-03-15/helpers/dry-consolidation-module-pressure.tsv`

## STEP 0 — Run Metadata + Scope Capture

Evidence mode: `semi-mechanical`

| Item [M/C] | Previous [M/C] | Current [M/C] | Delta [D] | Comparable? [C] |
| ---- | ---- | ---- | ---- | ---- |
| baseline report path | `docs/audits/reports/2026-03/2026-03-12/dry-consolidation.md` | same | none | yes |
| method version | `Method V3` | `DRY-1.2` | `N/A (method change)` | no |
| duplication taxonomy | not explicitly tagged | `DT-1` | `N/A (method change)` | no |
| owner-layer taxonomy | not explicitly tagged | `OL-1` | `N/A (method change)` | no |
| invariant role model | not explicitly tagged | `IR-1` | `N/A (method change)` | no |
| facade inclusion rule | implicit | `FI-1` | `N/A (method change)` | no |
| consolidation safety model | implicit | `CS-1` | `N/A (method change)` | no |
| in-scope roots | `icydb-core` runtime policy surfaces | `crates/icydb-core/src` (runtime + facade mappings only where semantic) | stable | yes |
| exclusions | tests/bench/examples/generated | tests/bench/examples/generated | stable | yes |

## STEP 1A — Structural Duplication Scan

Evidence mode: `mechanical`

| Pattern [M] | Files [M] | Lines [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Safety Critical? [C] | Behavioral Equivalence Confidence [C] | Drift Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| continuation envelope guards | `db/index/envelope.rs`; `db/cursor/continuation.rs`; `db/cursor/mod.rs`; `db/index/mod.rs` | `index/envelope.rs:38,71`; `cursor/continuation.rs:59,62` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| semantic bound conversion wrappers | `db/index/range.rs`; `db/access/lowering.rs`; `db/cursor/anchor.rs`; `db/index/mod.rs` | `index/range.rs:100,162`; `access/lowering.rs:274`; `cursor/anchor.rs:185` | Intentional boundary duplication | no | yes | yes | yes | high | medium-low | medium-low |
| cursor token decode + grouped revalidation wrappers | `db/cursor/spine.rs`; `db/cursor/mod.rs`; `db/codec/cursor.rs`; `db/executor/executable_plan.rs`; `db/query/plan/continuation.rs` | `cursor/spine.rs:168,299`; `cursor/mod.rs:72,143`; `query/plan/continuation.rs:340,354` | Evolution drift duplication | no | yes | yes | yes | medium | medium | medium |
| query/plan error mapping fanout | `db/session/mod.rs`; `db/session/sql.rs`; `db/query/intent/errors.rs`; `db/error/planner.rs`; `db/error/executor.rs` | `session/mod.rs:35`; `session/sql.rs:53,69`; `query/intent/errors.rs:93,107,116` | Intentional boundary duplication | no | yes | yes | yes | medium | medium | medium |
| reverse relation index mutation bookkeeping | `db/relation/reverse_index.rs`; `db/relation/validate.rs`; `db/executor/mutation/commit_window.rs`; `db/relation/mod.rs`; `db/commit/replay.rs` | `relation/reverse_index.rs:74,183`; `executor/mutation/commit_window.rs:39,225` | Evolution drift duplication | no | yes | yes | yes | medium | medium-high | medium-high |
| commit replay/prepare/rollback sequencing | `db/commit/recovery.rs`; `db/commit/replay.rs`; `db/commit/prepare.rs`; `db/commit/rollback.rs`; `db/executor/mutation/commit_window.rs` | `commit/recovery.rs:67,77,79`; `commit/replay.rs:25,34`; `commit/prepare.rs:31,50` | Defensive duplication | partially | yes | yes | yes | high | medium-low | medium-low |

## STEP 2A — Semantic Redundancy Scan

Evidence mode: `classified`

| Pattern Family [M] | Occurrences [M] | Layers Involved [M] | Cross-Layer? [D] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Difficulty [C] | Suggested Owner Layer [C] | Risk [C] |
| ---- | ----: | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| continuation envelope interpretation | 4 | index, cursor, executor-facing adapters | yes | no | yes | yes | yes (`db/index/envelope.rs`) | high | medium | index boundary | medium-low |
| semantic bound conversion | 4 | index, access lowering, cursor anchor | yes | no | yes | yes | yes (`db/index/range.rs`) | high | medium | index boundary | medium-low |
| cursor token decode + grouped cursor revalidation | 8 | codec, cursor, planner, executor | yes | no | yes | yes | yes (`db/cursor/*`) | medium | medium | cursor boundary | medium |
| QueryError/PlanError/InternalError mapping families | 5 | error, query intent, session | yes | no | yes | yes | yes (`db/query/intent/errors.rs`) | medium | medium | query intent + session boundary split | medium |
| reverse relation mutation + metrics fanout | 5 | relation, executor mutation, commit replay | yes | no | yes | yes | yes (`db/relation/reverse_index.rs`) | medium | high | relation boundary | medium-high |
| recovery replay + rollback sequencing | 10 | commit recovery, replay, prepare, rollback, mutation window | yes | partially | yes | yes | yes (`db/commit/recovery.rs`) | high | medium | commit boundary | medium-low |

## STEP 3A — Duplication-Driven Split Pressure Only

Evidence mode: `semi-mechanical`

| Module [M] | Size [M] | Duplication Families [M] | Same Owner Layer? [C] | Pressure Type [C] | Duplication Amplification [C] | Risk [C] |
| ---- | ----: | ----: | ---- | ---- | ---- | ---- |
| `crates/icydb-core/src/db/index/envelope.rs` | 470 | 2 | no | under-splitting | continuation and envelope helper families remain co-located with high call fanout | medium-high |
| `crates/icydb-core/src/db/session/sql.rs` | 446 | 3 | yes | under-splitting | SQL route + lowering mapping + projection label helpers increase local duplication pressure | medium |
| `crates/icydb-core/src/db/access/lowering.rs` | 396 | 2 | no | under-splitting | prefix/range lowering wrappers duplicate index-bound error mapping shape | medium |
| `crates/icydb-core/src/db/relation/reverse_index.rs` | 319 | 2 | no | under-splitting | reverse key derivation and mutation-prep logic coupled in one file | medium |
| `crates/icydb-core/src/db/commit/prepare.rs` | 223 | 2 | yes | under-splitting | prepare + decoding + mutation fanout in one owner file | medium |
| `crates/icydb-core/src/db/commit/recovery.rs` | 119 | 2 | yes | over-splitting | orchestration wrappers mirror commit replay/rebuild safety checks | low-medium |

## STEP 4A — Invariant Repetition Classification

Evidence mode: `classified`

| Invariant [M] | Canonical Owner [C] | Canonical Owner Known? [C] | Enforcement Sites [M] | Site Roles [C] | Same Owner Layer? [C] | Boundary-Protected? [C] | Sites Count [D] | Classification [C] | Divergence Risk [C] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- | ---- | ---- |
| continuation envelope containment + strict advancement | `db/index/envelope.rs` | yes | `index/envelope.rs`; `cursor/continuation.rs`; `cursor/mod.rs`; `index/mod.rs` | defining + defensive re-checking | no | yes | 4 | Safety-enhancing | medium | medium-low |
| semantic index-range bound conversion | `db/index/range.rs` | yes | `index/range.rs`; `access/lowering.rs`; `cursor/anchor.rs` | defining + validating + defensive re-checking | no | yes | 3 | Safety-enhancing | medium-low | low-medium |
| recovery marker replay-before-clear sequencing | `db/commit/recovery.rs` | yes | `commit/recovery.rs`; `commit/replay.rs`; `commit/guard.rs`; `commit/store.rs` | defining + recovery re-verification + defensive re-checking | no | yes | 4 | Safety-enhancing | medium | medium |
| reverse relation index symmetry | `db/relation/reverse_index.rs` | yes | `relation/reverse_index.rs`; `relation/validate.rs`; `executor/mutation/commit_window.rs`; `commit/replay.rs` | defining + validating + recovery re-verification | no | yes | 4 | Divergence-prone | medium-high | medium-high |

## STEP 5A — Error Mapping / Construction Drift

Evidence mode: `semi-mechanical`

| Error Family [M] | Files [M] | Mapping Logic Duplication [C] | Classification Drift? [C] | Origin Drift? [C] | Same Owner Layer? [C] | Canonical Owner Known? [C] | Behavioral Equivalence Confidence [C] | Consolidation Safety Class [C] | Drift Risk [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| executor plan error -> query plan error mapping | `db/session/mod.rs`; `db/session/query.rs` | yes (small wrapper fanout) | no | no | yes | yes (`db/session/mod.rs`) | high | safe local unification | low-medium |
| SQL parse/lowering errors -> query errors | `db/session/sql.rs`; `db/query/intent/errors.rs` | yes | low | low | no | partially | medium | boundary-sensitive | medium |
| InternalError class mapping -> QueryExecutionError variants | `db/query/intent/errors.rs`; `db/error/{planner,cursor,executor,query}.rs` | yes | no | low | no | yes (`db/query/intent/errors.rs`) | high | boundary-sensitive | medium-low |

## STEP 6B — Boundary-Protective Redundancy

Evidence mode: `classified`

| Area [M] | Duplication Sites [M] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Protective Rationale [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| planner validation vs executor defensive validation | `db/query/plan/validate/core.rs`; `db/executor/plan_validate.rs`; executor call sites | no | yes | yes | fail-closed runtime enforcement if caller bypasses planner-only contracts | high |
| cursor boundary checks across planning and execution | `db/query/plan/continuation.rs`; `db/cursor/continuation.rs`; `db/index/envelope.rs` | no | yes | yes | preserves cursor token isolation and strict resume semantics | high |
| recovery replay + integrity validation + marker clear | `db/commit/recovery.rs`; `db/commit/replay.rs`; `db/commit/guard.rs` | no | yes | yes | protects durability and replay symmetry before marker clear | critical |
| reverse relation validation and mutation preparation | `db/relation/validate.rs`; `db/relation/reverse_index.rs`; `db/executor/mutation/commit_window.rs` | no | yes | yes | prevents dangling strong-relation references across live and replay paths | high |

## STEP 7B — Consolidation Candidates (Post-Protection Gate)

Evidence mode: `classified`

| Area [M] | Files [M] | Duplication Type [C] | Same Owner Layer? [C] | Shared Authority? [C] | Boundary-Protected? [C] | Canonical Owner Known? [C] | Consolidation Safety Class [C] | Suggested Owner Layer [C] | Difficulty [C] | Drift Surface Reduction [C] | Estimated LoC Reduction [D] | Risk Level [C] |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----: | ---- |
| session-side query error wrapper consolidation | `db/session/mod.rs`; `db/session/query.rs`; `db/session/sql.rs` | Accidental duplication | yes | yes | no | yes | safe local unification | session boundary | low | medium | 20 | low |
| reverse-index delta counter helper extraction | `db/executor/mutation/commit_window.rs`; `db/relation/reverse_index.rs` | Boilerplate duplication | yes | yes | no | yes | safe helper extraction | executor mutation boundary | low | low | 8 | low |
| semantic-bound error mapping helper unification | `db/access/lowering.rs`; `db/cursor/anchor.rs`; `db/index/range.rs` | Evolution drift duplication | no | yes | partially | yes | boundary-sensitive | index boundary | medium | low-medium | 12 | medium-low |

## STEP 8B — Dangerous Consolidations (Do NOT Merge)

Evidence mode: `classified`

| Area [M] | Why Duplication Is Protective [C] | Boundary-Protected? [C] | Consolidation Safety Class [C] | Risk If Merged [C] |
| ---- | ---- | ---- | ---- | ---- |
| planner + executor plan validation surfaces | duplicate checks protect against bypassed planner contracts at runtime | yes | do not merge | high |
| cursor planning checks + runtime envelope checks | split keeps token protocol semantics isolated from execution-time envelope mechanics | yes | do not merge | high |
| commit recovery orchestration + replay apply loops | separation preserves fail-closed recovery sequencing and rollback symmetry | yes | do not merge | critical |
| reverse relation validation + reverse index mutation prep | split preserves authority boundaries between validation and mutation derivation | yes | do not merge | high |

## STEP 9 — Quantitative Summary + High-Risk Ledger

Evidence mode: `semi-mechanical`

| Metric [M/C/D] | Previous [M/C/D] | Current [M/C/D] | Delta [D] | Interpretation [C] |
| ---- | ---- | ---- | ---- | ---- |
| total duplication patterns found | N/A (method change) | 6 | N/A (method change) | concentrated in cursor/envelope, error mapping, relation/recovery flows |
| total high-risk divergence patterns | N/A (method change) | 0 | N/A (method change) | no immediate high-risk drift-triggering duplication |
| same-layer accidental duplication count | N/A (method change) | 1 | N/A (method change) | localized to session query/sql wrapper mapping |
| cross-layer intentional duplication count | N/A (method change) | 4 | N/A (method change) | mostly boundary-protective duplication |
| defensive duplication count | N/A (method change) | 2 | N/A (method change) | replay/recovery and validation fail-closed checks |
| boundary-protected duplication count | N/A (method change) | 5 | N/A (method change) | dominant pattern; consolidation must remain boundary-aware |
| invariants with `>3` enforcement sites | N/A (method change) | 3 | N/A (method change) | continuation envelope, recovery marker flow, reverse relation symmetry |
| error-construction families with `>3` custom mappings | N/A (method change) | 1 | N/A (method change) | error-class to query-execution mapping remains centralized but high-fanout |
| drift surface reduction estimate | N/A (method change) | medium | N/A (method change) | safe candidates reduce local fanout without layer collapse |
| estimated LoC reduction range (conservative) | N/A (method change) | 28-40 | N/A (method change) | mainly helper extraction/local unification candidates |

High-risk ledger not required (`total high-risk divergence patterns = 0`).

## STEP 9A — Analyst Verification Readout

Evidence mode: `semi-mechanical`

| Verification Metric [M/C/D] | Count [D] | Definition [M/C] |
| ---- | ----: | ---- |
| mechanical findings count | 6 | STEP 1A rows backed by direct pattern/file anchors |
| classified findings count | 17 | STEP 2A + STEP 4A + STEP 5A + STEP 6B + STEP 7B + STEP 8B rows requiring owner/safety judgment |
| high-confidence candidate count | 2 | consolidation candidates with high equivalence confidence and `safe helper extraction` / `safe local unification` safety class |
| boundary-protected findings count | 9 | rows where `Boundary-Protected? = yes` across Steps 1A/4A/6B/8B |

## 1. Run metadata + comparability note

- `DRY-1.2` method manifest applied; run is `non-comparable` against the 2026-03-12 baseline due method contract change.

## 2. Mode A summary: high-impact consolidation opportunities

- Reverse relation mutation bookkeeping remains the highest-impact medium-high drift surface; keep canonical ownership in `db/relation/reverse_index.rs` and avoid cross-layer merges.

## 3. Mode A summary: medium opportunities

- Session-level error wrapper fanout and cursor decode/revalidate wrappers show medium consolidation opportunity within existing owner boundaries.

## 4. Mode A summary: low/cosmetic opportunities

- Local helper extraction around mutation delta counters and bound-error mapping wrappers offers low-risk cleanup with limited but real drift-surface reduction.

## 5. Mode B summary: protective redundancies (keep separate)

- Planner vs executor validation duplication, cursor boundary duplication, recovery sequencing duplication, and reverse relation validation/mutation duplication are all protective and should stay separated by boundary.

## 6. Dangerous consolidations (do not merge)

- Do not merge planner and executor validation, cursor planning/runtime checks, recovery orchestration with replay loops, or relation validation with mutation prep.

## 7. Quantitative summary (trend backbone + drift surface estimate + LoC estimate)

- Patterns found: `6`
- High-risk divergence patterns: `0`
- Boundary-protected patterns: `5`
- Drift surface reduction estimate: `medium`
- Conservative LoC reduction: `28-40`

## 8. Analyst verification readout (mechanical/classified/high-confidence/boundary-protected counts)

- mechanical findings: `6`
- classified findings: `17`
- high-confidence candidates: `2`
- boundary-protected findings: `9`

## 9. Architectural risk summary

- DRY pressure is moderate and boundary-constrained: most duplication is intentional/defensive redundancy preserving fail-closed semantics, with a smaller set of safe local unification opportunities.

## 10. DRY risk index (1-10, lower is better)

- **5.0/10** (`moderate risk / manageable pressure`)

## 11. Verification readout (`PASS` / `FAIL` / `BLOCKED`)

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
- audit status: **PASS**
