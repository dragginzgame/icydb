> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

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

## 2. Architecture Layer Verification

Expected order used for verification:

- `intent -> query/plan -> access -> executor -> index/storage -> codec`

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/13/forensic-complexity-accretion/01/report.md).
