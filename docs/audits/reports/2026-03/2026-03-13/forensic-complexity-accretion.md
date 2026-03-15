This audit must not reuse structural counts from other audits in the same run.
All metrics must originate from STEP -1 enumeration or the metrics dataset.

# Forensic Complexity Accretion Audit - 2026-03-13

## Scope and Method

- Scope root: `crates/icydb-core/src/`
- Excluded from scan: `**/tests/**`, `**/tests.rs`, `**/testing/**`, `**/benches/**`, `**/examples/**`
- Runtime modules scanned: `458`
- Full per-module dataset: `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-runtime-metrics.tsv`
- Full layer verification table: `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-layer-verification.tsv`
- Full concept spread table: `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-concept-map.tsv`
- Full invariant match artifacts: `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-invariant-*.txt`

---

## 1. Runtime Topology Map

### 1.1 Full Module Enumeration Table

The complete table requested in this phase is provided in:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-runtime-metrics.tsv`

Columns:

- `module_path`
- `file`
- `loc`
- `pub_types`
- `pub_fns`
- `match_count`
- `if_count`
- `branch_pressure`
- `max_match_depth`
- `fanout`
- `module_layer`
- `import_layers_touched`
- `upward_imports`
- `cross_layer_imports`

### 1.2 Pressure Node Detection

Global pressure counts:

- `LOC > 800`: `10` modules
- `LOC > 600`: `20` modules
- `branch_pressure > 40`: `6` modules

Pressure node slice:

| Module Path | File | LOC | pub types | pub fns | match | if | branch_pressure |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `db::session` | `crates/icydb-core/src/db/session.rs` | 3367 | 1 | 30 | 16 | 9 | 25 |
| `db::sql::parser` | `crates/icydb-core/src/db/sql/parser.rs` | 1683 | 13 | 1 | 11 | 74 | 85 |
| `db::sql::lowering` | `crates/icydb-core/src/db/sql/lowering.rs` | 1457 | 4 | 2 | 16 | 34 | 50 |
| `types::decimal` | `crates/icydb-core/src/types/decimal.rs` | 1392 | 3 | 9 | 5 | 72 | 77 |
| `db::executor::explain::descriptor` | `crates/icydb-core/src/db/executor/explain/descriptor.rs` | 1030 | 0 | 3 | 20 | 34 | 54 |
| `value` | `crates/icydb-core/src/value/mod.rs` | 958 | 5 | 35 | 20 | 31 | 51 |
| `db::query::plan::access_choice` | `crates/icydb-core/src/db/query/plan/access_choice.rs` | 830 | 4 | 2 | 12 | 45 | 57 |
| `db::query::explain::plan` | `crates/icydb-core/src/db/query/explain/plan.rs` | 821 | 15 | 6 | 14 | 2 | 16 |

---

## 2. Architecture Layer Verification

Expected order used for verification:

- `intent -> query/plan -> access -> executor -> index/storage -> codec`

### 2.1 Full Layer Table

The complete module-level layer table is provided in:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-layer-verification.tsv`

### 2.2 Violation Slice (Top Upward/Cross-Layer Import Pressure)

| Module | Layer | Imports From | Upward Imports | Cross-Layer Imports | Violations |
| --- | --- | --- | ---: | ---: | --- |
| `db::executor::explain::descriptor` | `executor` | `query/plan,access,executor` | 18 | 18 | upward + cross-layer mix |
| `db::executor::executable_plan` | `executor` | `query/plan,access,executor,codec` | 14 | 15 | upward + cross-layer mix |
| `db::executor::aggregate::projection` | `executor` | `query/plan,executor,index/storage` | 11 | 12 | upward + cross-layer mix |
| `db::executor::aggregate::terminals` | `executor` | `query/plan,access,executor,index/storage` | 11 | 12 | upward + cross-layer mix |
| `db::executor` | `executor` | `access,executor,index/storage` | 10 | 11 | upward + cross-layer mix |
| `db::cursor::continuation` | `executor` | `query/plan,access,executor,index/storage` | 5 | 7 | upward + cross-layer mix |
| `db::index::scan` | `index/storage` | `executor,index/storage` | 5 | 5 | upward + cross-layer mix |

Evidence:

- Cross-layer import concentration in executor explain descriptor: `crates/icydb-core/src/db/executor/explain/descriptor.rs:6`
- Executor plan consuming planner contracts + explain + lowering: `crates/icydb-core/src/db/executor/executable_plan.rs:8`
- Cursor continuation touching access + query plan + index: `crates/icydb-core/src/db/cursor/continuation.rs:6`

---

## 3. Concept Authority Map

### 3.1 Concept Spread Table

| Concept | Implementing Files | Candidate Authority | Drift Risk |
| --- | ---: | --- | --- |
| continuation | 110 | `db/cursor/envelope.rs` + `db/cursor/continuation.rs` | High |
| cursor | 120 | `db/cursor/mod.rs` subtree | High |
| anchor | 25 | `db/cursor/anchor.rs` | High |
| envelope | 28 | `db/cursor/envelope.rs` | High |
| access path | 60 | `db/access/path.rs` + `db/access/canonical.rs` | High |
| access plan | 93 | `db/access/plan.rs` | High |
| route shape | 17 | `db/executor/route/contracts/shape.rs` | High |
| executor kernel | 25 | `db/executor/kernel/mod.rs` | High |
| predicate evaluation | 81 | `db/predicate/runtime.rs` + executor projection/eval | High |
| predicate encoding | 3 | `db/predicate/encoding.rs` | Medium |
| canonicalization | 35 | split authority (`db/access/canonical.rs`, `db/predicate/normalize.rs`) | High |
| fingerprint | 39 | split authority (`db/query/fingerprint/*`, `db/schema/fingerprint.rs`) | High |
| schema resolution | 37 | `db/schema/info.rs` + `db/schema/validate.rs` | High |
| index validation | 48 | split authority (`db/index/plan/*`, `db/access/lowering.rs`) | High |
| replay/recovery | 23 | `db/commit/recovery.rs` + `db/commit/replay.rs` | High |
| mutation guard | 27 | split authority (`db/commit/guard.rs`, executor route/mutation guards) | High |
| commit markers | 36 | `db/commit/marker.rs` | High |

Full source for counts and matched files:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-concept-map.tsv`

---

## 4. Semantic Re-Derivation Findings

| Invariant | Implementations (files) | Owner Layer Candidate | Re-Derivation Severity |
| --- | ---: | --- | --- |
| index range validation | 51 | `query/plan` + `access` | High |
| continuation advancement | 11 | `cursor` | High |
| canonical ordering | 82 | split (`access` + `index` + `query/plan`) | High |
| predicate canonicalization | 21 | `predicate` | High |
| schema identity checks | 40 | `schema` | High |
| anchor containment | 5 | `cursor` | Medium |
| unique constraint detection | 5 | `index` | Medium |

Evidence (line-level):

- Lowering-side index range derivation: `crates/icydb-core/src/db/access/lowering.rs:256`
- Executable plan carries `index_range_spec_invalid`: `crates/icydb-core/src/db/executor/executable_plan.rs:70`
- Runtime alignment check in traversal: `crates/icydb-core/src/db/executor/stream/access/traversal.rs:210`
- Cursor envelope authority functions: `crates/icydb-core/src/db/cursor/envelope.rs:13`, `:26`, `:60`, `:80`, `:97`
- Cursor continuation revalidates envelope and advancement: `crates/icydb-core/src/db/cursor/continuation.rs:57`, `:72`
- Unique constraint enforcement entrypoint: `crates/icydb-core/src/db/index/plan/unique.rs:35`
- Unique check call at index plan composition: `crates/icydb-core/src/db/index/plan/mod.rs:176`

### 4.1 Audit-Guard Blind Spot (Important)

The layer-authority script regex is strict to `fn name(` and misses generic signatures:

- Pattern expecting non-generic signature: `scripts/ci/check-layer-authority-invariants.sh:123`
- Second pattern with same limitation: `scripts/ci/check-layer-authority-invariants.sh:145`
- Actual generic functions: `crates/icydb-core/src/db/cursor/envelope.rs:13`, `:26`, `:60`

Risk:

- Semantic-owner drift can evade this guard when moved to generic signatures.

---

## 5. Control-Plane Complexity

Control-plane candidate table is in:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-control-plane.tsv`

Hot slice:

| Module | Branch Count | Match | If | Max Match Depth | Risk Characterization |
| --- | ---: | ---: | ---: | ---: | --- |
| `db::query::plan::planner::range` | 28 | 12 | 16 | 2 | routing + range policy branching |
| `db::session` | 25 | 16 | 9 | 2 | facade dispatch/entrypoint multiplexing |
| `db::executor::aggregate::terminals` | 17 | 7 | 10 | 1 | terminal route/aggregation mode branching |
| `db::executor::pipeline::operators::reducer` | 17 | 11 | 6 | 2 | stage/operator state transitions |
| `db::executor::pipeline::orchestrator` | 9 | 7 | 2 | 1 | pipeline stage orchestration |

Required threshold finding:

- `branch_pressure > 60` modules exist (`db::sql::parser`, `types::decimal`) but they are parser/types, not control-plane dispatch modules.

---

## 6. Fan-Out Dependency Mapping

Full fan-out table:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-fanout.tsv`

Top fan-out and multi-layer touch modules:

| Module | Fan-Out | Layers Touched | Risk |
| --- | ---: | --- | --- |
| `db::session` | 56 | intent, query/plan, access, executor, index/storage | High |
| `db::executor::runtime_context` | 27 | executor, index/storage | High |
| `db::executor::mutation::commit_window` | 27 | index/storage | High |
| `db::executor::executable_plan` | 26 | query/plan, access, executor, codec | High |
| `db::relation::reverse_index` | 25 | index/storage | High |
| `db::access::lowering` | 23 | access, index/storage | High |
| `db::executor::aggregate::numeric` | 23 | query/plan, access, executor | High |
| `db::executor::aggregate::terminals` | 22 | query/plan, access, executor, index/storage | High |

Flag counts:

- modules with `fanout > 12`: `63`
- modules touching `> 3` architecture layers: `6`

---

## 7. State Machine Integrity Scan

Full state-machine extraction:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-state-machines.tsv`

State machine table:

| State Machine | File | States | Transition Refs |
| --- | --- | ---: | ---: |
| `SaveMode` | `crates/icydb-core/src/db/executor/mutation/save.rs` | 3 | 15 |
| `KeyOrderState` | `crates/icydb-core/src/db/executor/stream/access/physical.rs` | 3 | 14 |
| `LoadMode` | `crates/icydb-core/src/db/executor/pipeline/orchestrator/mod.rs` | 3 | 10 |
| `LoadPipelineState` | `crates/icydb-core/src/db/executor/pipeline/orchestrator/mod.rs` | 1 | 7 |
| `LoadPipelineStage` | `crates/icydb-core/src/db/executor/pipeline/stages/stage.rs` | 6 | 6 |
| `SqlExplainMode` | `crates/icydb-core/src/db/sql/parser.rs` | 3 | 5 |
| `AggregateReducerState` | `crates/icydb-core/src/db/executor/aggregate/contracts/state.rs` | 7 | 1 |

Implicit state-machine pressure:

- `db::executor::pipeline::operators::reducer` has branch-heavy staged logic (`branch_pressure=17`) without a single state enum owner for all paths.

---

## 8. Hidden Coupling Detection

Full coupling table:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-hidden-coupling.tsv`

High-coupling modules:

| Module | Domains Mixed | Coupling Risk |
| --- | --- | --- |
| `db::session` | intent, query/plan, access, executor, index/storage | High |
| `db::executor::executable_plan` | query/plan, access, executor, codec | High |
| `db::executor::aggregate::terminals` | query/plan, access, executor, index/storage | High |
| `db::cursor::continuation` | query/plan, access, executor, index/storage | High |
| `db::executor::terminal::bytes` | query/plan, access, executor, index/storage | High |
| `db::query::fingerprint::fingerprint` | intent, query/plan, access, codec | High |

Evidence:

- Cross-domain imports in session facade: `crates/icydb-core/src/db/session.rs:8`
- Cross-domain imports in executable plan: `crates/icydb-core/src/db/executor/executable_plan.rs:8`
- Cross-domain imports in cursor continuation: `crates/icydb-core/src/db/cursor/continuation.rs:6`

---

## 9. DRY Semantic Duplication

Duplication matrix:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-duplication.tsv`

| Concept | Files | Severity | Evidence |
| --- | ---: | --- | --- |
| index range validation | 51 | High | lowering + executable plan + traversal (`access/lowering.rs:256`, `executable_plan.rs:96`, `traversal.rs:210`) |
| continuation advancement | 11 | High | cursor envelope + continuation + executor continuation (`cursor/envelope.rs:13`, `cursor/continuation.rs:72`) |
| canonical ordering | 82 | High | access canonical + index ordered + planner stability (`access/canonical.rs:38`, `index/key/ordered/mod.rs`, `query/plan/stability.rs:16`) |
| predicate canonicalization | 21 | High | predicate normalize/encoding and query lowering (`predicate/normalize.rs`, `predicate/encoding.rs`, `sql/lowering.rs`) |
| schema identity checks | 40 | High | spread across access/query/schema/commit (`access/validate.rs`, `schema/describe.rs`, `query/plan/access_choice.rs`) |
| anchor containment | 5 | Medium | cursor envelope + index envelope + cursor anchor |
| unique constraint detection | 5 | Medium | index plan unique + index plan mod |

---

## 10. Pass-Through Layer Detection

Full pass-through scan:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-pass-through.tsv`

Top pass-through concentration:

| Module | Pass-Through Count | Max Chain Depth | Consolidation Signal |
| --- | ---: | ---: | --- |
| `db::query::fluent::load::terminals` | 45 | 6 | facade wrapper saturation |
| `value` | 31 | 5 | API forwarding density |
| `db::session` | 18 | 6 | orchestration + forwarding blend |
| `db::response` | 16 | 6 | DTO adapter layering |
| `db::query::fluent::load::builder` | 14 | 4 | fluent wrapper layering |
| `db::query::builder::field` | 10 | 6 | chained DSL forwarding |

Evidence example (wrapper-heavy fluent terminal API):

- `crates/icydb-core/src/db/query/fluent/load/terminals.rs:95`
- `crates/icydb-core/src/db/query/fluent/load/terminals.rs:121`
- `crates/icydb-core/src/db/query/fluent/load/terminals.rs:153`

Notes:

- Heuristic scanner flags method-chain wrappers; `db::sql::parser` appears in raw output but is not a practical pass-through authority hotspot.

---

## 11. Phantom Structures

Scan artifact:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-phantom-candidates.tsv`

Result:

- No strong runtime phantom structures were detected.
- One candidate was found in a test-only block and is excluded as runtime phantom:

| Structure | File | Evidence |
| --- | --- | --- |
| `CommitMarkerWithExtra` | `crates/icydb-core/src/db/commit/store.rs` | Defined under `#[cfg(test)]` at line 220 |

---

## 12. Invariant Registry Reconstruction

| Invariant | Owner Module Candidate | Enforcement Points | Flag (>2 locations) |
| --- | --- | --- | --- |
| Cursor anchors stay within envelope | `db/cursor/envelope.rs` | `cursor/envelope.rs:60`, `cursor/envelope.rs:80`, `cursor/continuation.rs:57` | Yes |
| Continuation must advance strictly | `db/cursor/envelope.rs` | `cursor/envelope.rs:13`, `cursor/envelope.rs:97`, `cursor/continuation.rs:72` | Yes |
| Index ranges align with lowered plan contracts | `db/access/lowering.rs` + `db/query/plan/planner/range.rs` | `access/lowering.rs:256`, `executor/executable_plan.rs:96`, `executor/stream/access/traversal.rs:210` | Yes |
| Replay/recovery is fail-closed and idempotent boundary | `db/commit/recovery.rs` + `db/commit/replay.rs` | `commit/recovery.rs:62`, `commit/recovery.rs:67`, `commit/replay.rs:25` | Yes |
| Unique constraints fail closed | `db/index/plan/unique.rs` | `index/plan/unique.rs:35`, `index/plan/mod.rs:176`, `index/plan/mod.rs:147` | Yes |
| Canonicalization deterministic for planning/fingerprints | split (`db/access/canonical.rs`, `db/predicate/normalize.rs`) | `access/canonical.rs:38`, `query/plan/stability.rs:16`, `predicate/normalize.rs` | Yes |

---

## 13. Complexity Hotspot Ranking (Top 15)

Formula used:

- `score = LOC/100 + branch_pressure/20 + fanout/5 + duplication_hits*2 + invariant_rederivations*3`

Ranking source:

- `docs/audits/reports/2026-03/2026-03-13/artifacts/forensic-complexity-accretion/forensic-hotspots.tsv`

| Rank | Module | Score | Reason |
| ---: | --- | ---: | --- |
| 1 | `db::session` | 51.12 | extreme LOC + fanout centralization |
| 2 | `db::sql::lowering` | 30.07 | high LOC + high branching + cross-layer touch |
| 3 | `db::sql::parser` | 27.48 | high branch surface in one parser module |
| 4 | `db::cursor::anchor` | 23.87 | fanout + continuation/anchor re-derivations |
| 5 | `value` | 23.33 | high branch density + broad utility centralization |
| 6 | `db::executor::continuation::scalar` | 22.68 | continuation semantics spread + fanout |
| 7 | `db::executor::mutation::save_validation` | 22.25 | multi-invariant validation concentration |
| 8 | `db::executor::executable_plan` | 22.13 | cross-layer authority concentration |
| 9 | `db::query::intent::query` | 21.70 | large intent API surface + fanout |
| 10 | `db::executor::explain::descriptor` | 21.40 | high branching + cross-layer import matrix |
| 11 | `db::predicate::normalize` | 21.23 | predicate canonicalization authority + spread |
| 12 | `db::executor::aggregate::projection` | 20.61 | branch + fanout + cross-layer references |
| 13 | `types::decimal` | 20.37 | numeric parsing/validation branch surface |
| 14 | `db::cursor::envelope` | 20.08 | continuation and anchor enforcement hub |
| 15 | `db::executor::route::planner::feasibility` | 19.05 | route feasibility authority mix |

---

## 14. Architecture Risk Summary

| Category | Risk | Concrete Reason |
| --- | --- | --- |
| Authority fragmentation | High | Core concepts (`continuation`, `access plan`, `predicate evaluation`) appear in dozens of files. |
| Control-plane expansion | Medium | No control-plane module exceeds branch>60, but fanout-heavy coordination modules remain (`session`, `executable_plan`, route/planner layers). |
| Semantic duplication | High | Index range, continuation, canonicalization, and schema checks are re-derived across many modules. |
| Layer violations | Medium | 95 modules carry upward imports (path-based classification); strongest concentration in executor descriptor/plan hubs. |
| Fan-out centralization | High | 63 modules exceed fanout>12; `db::session` at fanout 56 acts as a control/facade super-node. |

---

## 15. Refactor Recommendations (Concrete Targets)

1. Centralize continuation-envelope authority into one contract module.
   Action:
   - Keep `continuation_advanced`, `anchor_within_envelope`, `resume_bounds_from_refs` under one explicit authority (`db/cursor/envelope.rs`) and remove re-derivation helpers outside this module.
   - Replace direct logic copies with contract calls in `db/cursor/continuation.rs` and executor continuation modules.

2. Split `db::session` into bounded entrypoint adapters.
   Action:
   - Extract `sql facade`, `fluent load/delete facade`, and `mutation facade` into separate modules with a thin coordinator.
   - Target fanout reduction from `56` to `<25`.

3. Split `db::executor::executable_plan` into contracts vs execution-shaping modules.
   Action:
   - Move explain-descriptor assembly references to dedicated explain adapter.
   - Keep `ExecutablePlan` focused on immutable executor contracts and invariant state only.

4. Remove cross-layer planner imports from executor descriptor assembly.
   Action:
   - Replace direct `db::query::plan::*` imports in `db/executor/explain/descriptor.rs` with route/execution contract DTOs.
   - Preserve explain output shape through contract interfaces.

5. Collapse pass-through fluent terminal wrappers by macro or trait-generated adapters.
   Action:
   - In `db/query/fluent/load/terminals.rs`, consolidate repetitive wrappers into a generated terminal adapter map.
   - Preserve public terminal API while reducing forwarding duplication.

6. Hard-split canonicalization ownership.
   Action:
   - Define explicit canonicalization authority boundaries: access-shape canonicalization (`db/access/canonical.rs`) vs predicate canonicalization (`db/predicate/normalize.rs`).
   - Forbid canonicalization helpers in planner/executor layers except through owner APIs.

7. Harden layer-authority invariant script patterns.
   Action:
   - Update `scripts/ci/check-layer-authority-invariants.sh` function regex to match generic signatures (`fn name<...>(...)`) so owner drift is not missed.
   - Align expected owner path message with current cursor envelope owner.

8. Consolidate index-range validation checkpoints.
   Action:
   - Retain canonical lowering in `db/access/lowering.rs` and one runtime alignment gate in `db/executor/stream/access/traversal.rs`.
   - Remove duplicate range validity flags and checks where equivalent invariants are already guaranteed.

9. Reduce route/planner mixed responsibility seams.
   Action:
   - Keep route-shape derivation in `db/executor/route/planner/*` and move planner-policy reads behind typed capability providers.
   - Prevent executor route modules from importing planner internals directly.

10. Establish an invariant registry module for auditability.
    Action:
    - Introduce a small invariant registry doc/module that lists owner + enforcement points for continuation, index-range alignment, replay, and unique constraints.
    - CI can diff this registry against implementation references.
