# Layer Violation Audit - 2026-03-10 (Rerun 2)

## Report Preamble

- scope: authority layering and semantic ownership boundaries across db runtime modules
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-10/layer-violation.md`
- code snapshot identifier: `b456bbc4`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports and cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` upward imports, `0` policy re-derivations) | Low |
| Runtime compiles with current boundary wiring | `cargo check -p icydb-core` | PASS | Low-Medium |

- Cross-Cutting Risk Index: **4/10**

## Follow-Up Actions

- None required for this rerun.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
