# Recurring Audit - Wasm Footprint (2026-03-24)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, one_simple, one_complex, ten_simple, ten_complex` with profile `debug` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-15/wasm-footprint.md`
- code snapshot identifier: `5a1d34bd`
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
| `minimal` | PARTIAL | N/A | 2,719,250 | N/A | 821,706 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.minimal.debug.sql-on.md` |
| `one_simple` | PARTIAL | N/A | 2,879,061 | N/A | 876,449 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_simple.debug.sql-on.md` |
| `one_complex` | PARTIAL | N/A | 2,882,418 | N/A | 877,471 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.one_complex.debug.sql-on.md` |
| `ten_simple` | PARTIAL | N/A | 2,929,083 | N/A | 880,652 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_simple.debug.sql-on.md` |
| `ten_complex` | PARTIAL | N/A | 2,935,278 | N/A | 881,459 | `docs/audits/reports/2026-03/2026-03-24/artifacts/wasm-footprint/wasm-footprint.ten_complex.debug.sql-on.md` |

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve scoped baseline size artifacts so future consolidated summary runs stay comparable.

## Verification Readout

- `WASM_AUDIT_DATE=2026-03-24 bash scripts/ci/wasm-audit-report.sh` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
