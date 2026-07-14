# Layer Violation Audit - 2026-04-21

## Report Preamble

- scope: authority layering and semantic ownership boundaries across db runtime modules
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-13/layer-violation.md`
- code snapshot identifier: `b43bba078` (`dirty` working tree)
- method tag/version: `Method V3`
- comparability status: `comparable`

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports and cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` upward imports, `0` policy re-derivations, `0` cross-layer predicate duplication`) | Low |
| Access and route authority fan-out | `check-layer-authority-invariants.sh` snapshot (`AccessPath decision owners: 2`, `RouteShape decision owners: 3`) | PASS | Low-Medium |
| Predicate coercion ownership concentration | `check-layer-authority-invariants.sh` snapshot (`Predicate coercion owners: 4`) | PASS | Medium |
| Enum fan-out beyond two layers | `check-layer-authority-invariants.sh` snapshot (`Enum fan-out > 2 layers: 1`; `AggregateKind::=3`) | PASS | Medium |
| Ordering / comparator leakage outside index | `check-layer-authority-invariants.sh` snapshot (`Comparator definitions outside index: 0`) | PASS | Low |
| Canonicalization entrypoint sprawl | `check-layer-authority-invariants.sh` snapshot (`Canonicalization entrypoints: 1`) | PASS | Low |
| Continuation rewrite / advancement leakage outside index-cursor boundaries | targeted scan of `continuation_signature`, `resume_bounds_for_continuation`, `continuation_advanced`, and strict-advance references under `crates/icydb-core/src/db` | PASS: continuation rewrite remains index-owned and cursor/runtime consumers stay on transport, validation, and execution wiring | Low |
| Prepared fallback ownership seam vs true layer violation | targeted scan of `db/sql/lowering/prepare.rs` and `db/session/sql/parameter.rs`, cross-checked against the canonical semantic authority, complexity, and DRY audits | Monitoring only: prepared fallback is still the active structural contraction seam, but this run found no new cross-layer semantic owner split that violates the layer model | Medium |

- High-Risk Cross-Cutting Violations: `0`
- Medium-Risk Drift Surfaces: `3`
- Low-Risk / Intentional Redundancy Areas: `4`
- Cross-Cutting Risk Index: **3.5/10**

## Interpretation

- This run remains clean from a layer-violation perspective.
- The tracked authority metrics are effectively unchanged from the April 13 baseline:
  - `AccessPath` decision owners remain `2`
  - `RouteShape` decision owners remain `3`
  - predicate coercion owners remain `4`
  - enum fan-out beyond two layers remains `1`
  - comparator definitions outside index remain `0`
  - canonicalization entrypoints remain `1`
- The prepared fallback seam that now dominates the completeness, canonical semantic authority, complexity, and DRY audits does **not** currently present as a true layer violation.
- That seam is still better described as intra-cluster structural drift inside the prepared / expression pipeline boundary than as cross-layer authority breakage.

## Legitimate Cross-Cutting (Do Not Merge)

| Area | Why Redundant | Risk If Merged |
| --- | --- | --- |
| Planner continuation contracts and cursor/runtime transport | keeps semantic contract ownership in planning while runtime layers validate and transport cursor state locally | High |
| Route capability derivation and route-hint consumers | preserves route-owned capability truth while allowing downstream hint and execution code to consume snapshots without re-deriving policy | High |
| Commit marker store envelope and payload codec | preserves the stable-storage trust boundary while marker payload code remains responsible only for payload shape | High |
| Predicate capability classification and runtime/index consumers | preserves predicate-owned capability truth while runtime and index compilation apply that truth in their own layers | Medium |

## Follow-Up Actions

- No mandatory follow-up actions for this run.
- Monitoring-only: keep predicate coercion ownership from diffusing past the current four-owner set without a deliberate audit.
- Monitoring-only: keep `AggregateKind` fan-out from spreading beyond the current three-layer footprint.
- Monitoring-only: treat prepared fallback contraction as a structural cleanup target in `0.114`, but do not classify it as a layer violation unless it starts re-deriving planner-owned truth across the layer boundary rather than inside the current cluster.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
