# Layer Violation Audit - 2026-04-13

## Report Preamble

- scope: authority layering and semantic ownership boundaries across db runtime modules
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-24/layer-violation.md`
- code snapshot identifier: `562f320cd`
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

- Cross-Cutting Risk Index: **3.4/10**

## Follow-Up Actions

- No mandatory follow-up actions for this run.
- Monitoring-only: keep `AggregateKind` fan-out from spreading beyond the current three-layer footprint.
- Monitoring-only: keep predicate coercion ownership from diffusing past the current four-owner set without a deliberate audit.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
