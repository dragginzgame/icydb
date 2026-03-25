# Recurring Audit Summary - 2026-03-24

## Report Preamble

- scope: crosscutting recurring subset run (`complexity-accretion`, `canonical-semantic-authority`, `dry-consolidation`, `error-taxonomy`, `layer-violation`, `module-structure`, `velocity-preservation`, `wasm-footprint`)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-15/summary.md`
- code snapshot identifier: `3f453012`
- method tag/version: `Method V3`
- comparability status: `non-comparable` (mixed per-audit methods; complexity remains non-comparable to baseline while canonical-semantic-authority and dry-consolidation are comparable)

## Audit Run Order and Results

1. `crosscutting/crosscutting-complexity-accretion` -> `complexity-accretion.md` (Risk: 5.1/10)
2. `crosscutting/crosscutting-canonical-semantic-authority` -> `canonical-semantic-authority.md` (Risk: 3.8/10)
3. `crosscutting/crosscutting-dry-consolidation` -> `dry-consolidation.md` (Risk: 3.6/10)
4. `contracts/error-taxonomy` -> `error-taxonomy.md` (Risk: 4/10)
5. `crosscutting/crosscutting-layer-violation` -> `layer-violation.md` (Risk: 3/10)
6. `crosscutting/crosscutting-module-structure` -> `module-structure.md` (Risk: 4/10)
7. `crosscutting/crosscutting-velocity-preservation` -> `velocity-preservation.md` (Risk: 4/10)
8. `crosscutting/crosscutting-wasm-footprint` -> `wasm-footprint.md` + `artifacts/wasm-footprint/*.md` (PASS=4, PARTIAL=1, FAIL=0)

## Global Findings

- Runtime complexity pressure remains moderate (`5.1/10`) and is still concentrated in the same dense planner/predicate/explain/runtime clusters rather than a new broad subsystem explosion.
- Canonical semantic authority remains structurally stable (`3.8/10`) with no high-risk drift triggers.
- DRY consolidation pressure dropped further to low (`3.6/10`) after the predicate normal-form, capability-owner, and planner/explain capability-profile follow-through; no high-risk divergence-prone duplication families remain.
- Error taxonomy boundaries remain stable (`4/10`): constructor ownership is still explicit, current error-class/origin regression coverage stays green, and no immediate downgrade or escalation drift surfaced in this run.
- Layer authority remains clean (`3/10`): no upward imports, no cross-layer policy re-derivations, no cross-layer predicate duplication, and no comparator leakage outside `db/index/*`.
- Module structure pressure improved slightly (`4/10`): the old load-side hub pressure has collapsed, and `executor/route/planner/mod.rs` is now the main remaining structural hotspot.
- Velocity pressure improved slightly (`4/10`): the old load-side gravity well is gone, and route planning is now the main remaining extension-cost hotspot rather than a second broad hub.
- The route feature budget guard is now live in the route owner boundary, so the soft ceiling on route-shape growth is executable rather than just present in source.
- Wasm footprint capture completed for the current canister set, but delta comparability is still partial because baseline size artifacts are missing at the scoped report paths.
- All `8` tracked semantic concept families still converge on one canonical typed model, and missing-canonical-model blockers remain `0`.
- The latest `0.63` work reduced both semantic drift and duplication pressure: Pocket-IC/wasm harness cleanup shrank support-surface duplication, and predicate execution now converges on one canonical tree plus one capability classifier.

## Follow-Up Actions

- No mandatory follow-up actions for this subset run.
- Complexity follow-through: rerun the complexity-accretion report on top of the restored canonical runtime metrics generator before the next comparable baseline.
- Monitoring-only: keep schema/runtime `IndexKeyItem` parity and route-shape lowering ownership in the next canonical semantic authority cycle.
- DRY follow-through: no corrective consolidation is required now; only minor route-local call-site compression remains if future cleanup is desired.
- Error-taxonomy follow-through: no corrective action required; continue treating constructor ownership and class/origin mapping tests as the main guardrail against drift.
- Layer follow-through: no corrective action required; continue treating route capability as a monitored owner-layer surface rather than a refactor target.
- Module-structure follow-through: no corrective action required; monitor `executor/route/planner/mod.rs` and avoid adding more non-route imports there.
- Velocity follow-through: no corrective action required; monitor `executor/route/planner/mod.rs` for future feature-axis growth.
- Wasm-footprint follow-through: preserve scoped baseline size artifacts so future delta tables are comparable without falling back to `N/A`.

## Verification Readout

- `scripts/audit/runtime_metrics.sh` -> PASS (generator restored after the earlier complexity pass)
- fallback mechanical runtime extraction -> PASS (`artifacts/complexity-accretion/runtime-metrics.tsv`; complexity narrative still reflects the pre-restoration run)
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
- `cargo test -p icydb-core db::cursor::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core canonicalization_ownership_stays_in_access_and_predicate_layers -- --nocapture` -> PASS
- `cargo test -p icydb-core db::predicate::capability::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::predicate::runtime::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::index::predicate::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::query::explain::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core error::tests -- --nocapture` -> PASS
- `cargo clippy -p icydb-core --all-targets -- -D warnings` -> PASS
- `cargo test -p icydb-core --lib -- --list | rg "db::executor::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta"` -> PASS
- `cargo test -p icydb-core db::executor::route::tests::route_feature_budget_shape_kinds_stay_within_soft_delta -- --nocapture` -> PASS
- `WASM_AUDIT_DATE=2026-03-24 bash scripts/ci/wasm-audit-report.sh` -> PASS (generated one top-level `wasm-footprint.md` summary plus per-canister detail files under `artifacts/wasm-footprint/`)
