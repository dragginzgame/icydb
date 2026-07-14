# Layer Violation Audit - 2026-03-24

## Report Preamble

- scope: authority layering and semantic ownership boundaries across db runtime modules
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-12/layer-violation.md`
- code snapshot identifier: `3f453012`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports and cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` upward imports, `0` policy re-derivations, `0` cross-layer predicate duplication`) | Low |
| Access and route authority fan-out | `check-layer-authority-invariants.sh` snapshot (`AccessPath decision owners: 3`, `RouteShape decision owners: 2`) | PASS | Low-Medium |
| Ordering / comparator leakage outside index | `check-layer-authority-invariants.sh` snapshot (`Comparator definitions outside index: 0`) | PASS | Low |
| Canonical predicate capability ownership | `db/predicate/capability.rs`; `db/executor/preparation.rs`; `db/executor/explain/descriptor.rs`; targeted predicate/explain tests | PASS | Low |
| Continuation contract ownership | `db/cursor/mod.rs`; targeted cursor tests | PASS | Low-Medium |
| Runtime compiles with current boundary wiring | `cargo check -p icydb-core` | PASS | Low-Medium |

- Cross-Cutting Risk Index: **3/10**

## Follow-Up Actions

- No mandatory follow-up actions for this run.
- Monitoring-only: keep route capability ownership concentrated in `db/executor/route/*`; that is the remaining notable multi-module authority surface, but it is still within the expected owner layer.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core db::cursor::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::predicate::capability::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::query::explain::tests -- --nocapture` -> PASS
