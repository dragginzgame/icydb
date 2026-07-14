# Complexity Accretion Audit - 2026-03-10 (Rerun 2)

## Report Preamble

- scope: conceptual growth and branch-surface pressure in `crates/icydb-core/src/db` runtime (non-test)
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-10/complexity-accretion.md`
- code snapshot identifier: `b456bbc4`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Layer Health Snapshot

| Metric | Baseline (2026-03-10 first run) | Current (2026-03-10 rerun 2) | Delta |
| ---- | ----: | ----: | ----: |
| Upward imports (tracked edges) | 0 | 0 | 0 |
| Cross-layer policy re-derivations | 0 | 0 | 0 |
| Cross-layer predicate duplication count | 0 | 0 | 0 |
| AccessPath decision owners | 3 | 3 | 0 |
| RouteShape decision owners | 3 | 3 | 0 |
| Predicate coercion owners | 4 | 4 | 0 |
| Enum fan-out > 2 layers | 1 | 1 | 0 |

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Layer-authority and decision-owner caps remain contained | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Medium |
| Architecture text-scan invariant remains clean | `bash scripts/ci/check-architecture-text-scan-invariants.sh` | PASS | Low |
| Runtime compiles under current complexity surface | `cargo check -p icydb-core` | PASS | Medium |

## Complexity Risk Index

**5/10**

## Follow-Up Actions

- None required for this rerun.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-architecture-text-scan-invariants.sh` -> PASS
- `cargo check -p icydb-core` -> PASS
