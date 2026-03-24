# Recurring Audit - Wasm Footprint (2026-03-24)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, one_simple, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-15/wasm-footprint.md`
- code snapshot identifier: `d85286d4`
- method tag/version: `WASM-1.0`
- comparability status: `non-comparable (one or more baseline size artifacts are missing)`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | per-canister size reports + summaries written under `artifacts/wasm-footprint/` |
| Twiggy top breakdown generated | PASS | per-canister top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | per-canister dominator text artifacts written |
| Twiggy monomorphization breakdown generated | PASS | per-canister monos artifacts written |
| Baseline delta availability | PARTIAL | one or more prior scoped size artifacts are missing |

PASS=4, PARTIAL=1, FAIL=0

## Per-Canister Size Snapshot

| Canister | Baseline Status | Previous shrunk `.wasm` | Current shrunk `.wasm` | Previous shrunk `.wasm.gz` | Current shrunk `.wasm.gz` | Detail Report |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `minimal` | PASS | 1,403,593 | 1,293,185 | 488,906 | 475,641 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.md` |
| `one_simple` | PARTIAL | N/A | 1,383,715 | N/A | 507,627 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.md` |
| `one_complex` | PARTIAL | N/A | 1,384,746 | N/A | 507,891 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.md` |
| `ten_simple` | PARTIAL | N/A | 1,396,718 | N/A | 508,878 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.md` |
| `ten_complex` | PARTIAL | N/A | 1,397,717 | N/A | 509,102 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.md` |

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve scoped baseline size artifacts so future consolidated summary runs stay comparable.

## Verification Readout

- `WASM_AUDIT_DATE=2026-03-24 bash scripts/ci/wasm-audit-report.sh` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
