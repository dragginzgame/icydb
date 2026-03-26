# Layer Violation Audit - 2026-03-26

## Report Preamble

- scope: authority layering and semantic ownership boundaries across db runtime modules
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/layer-violation.md`
- code snapshot identifier: `a956de44`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports and cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` upward imports, `0` policy re-derivations, `0` cross-layer predicate duplication`) | Low |
| Access and route authority fan-out | `check-layer-authority-invariants.sh` snapshot (`AccessPath decision owners: 3`, `RouteShape decision owners: 2`) | PASS | Low-Medium |
| Ordering / comparator leakage outside index | `check-layer-authority-invariants.sh` snapshot (`Comparator definitions outside index: 0`) | PASS | Low |
| Capability and explain policy ownership | `db/predicate/capability.rs`; `db/executor/preparation.rs`; `db/executor/explain/descriptor.rs`; targeted predicate/explain tests | PASS | Low |
| Continuation contract ownership | `db/cursor/mod.rs`; `db/query/plan/continuation.rs`; targeted cursor tests | PASS | Low-Medium |
| Canonical persisted-row invariant ownership | `db/data/persisted_row.rs`; `db/executor/projection/*`; `db/relation/reverse_index.rs`; current tree readback | PASS (`0.65` read-side hardening remains concentrated at the structural row boundary, with downstream consumers delegating instead of re-deriving row validity`) | Low |
| Runtime compiles with current boundary wiring | `cargo check -p icydb-core` | PASS | Low-Medium |

- High-Risk Cross-Cutting Violations: none found in this run.
- Medium-Risk Drift Surfaces: route capability ownership remains the main multi-module semantic surface, but it is still concentrated inside the expected executor/route owner layer.
- Low-Risk / Intentional Redundancy: cursor definition vs transport/application, predicate capability classification vs consumer application, and commit marker envelope vs payload codec remain protective separations.
- Quantitative Snapshot:
  - Policy duplications found: `0` cross-layer re-derivations in the invariant checker
  - Comparator leaks: `0`
  - Capability fan-out >2 layers: `1` (`AggregateKind::=4` in the checker snapshot)
  - Invariants enforced in >3 sites: `2` notable bounded families (`predicate capability meaning`, `continuation contract meaning`)
  - Protective redundancies: `3`

- Cross-Cutting Risk Index: **3/10**

## Follow-Up Actions

- No mandatory follow-up actions for this run.
- Monitoring-only: keep route capability ownership concentrated in `db/executor/route/*`; that remains the most visible multi-module authority surface, but it is still within the intended owner layer.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core db::cursor::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::predicate::capability::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::query::explain::tests -- --nocapture` -> PASS
