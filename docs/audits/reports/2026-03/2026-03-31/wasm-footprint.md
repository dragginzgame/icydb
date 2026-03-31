# Recurring Audit - Wasm Footprint (2026-03-31)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, one_simple, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-15/wasm-footprint.md`
- code snapshot identifier: `1356b3bc`
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
| `minimal` | PASS | 1,403,593 | 1,125,935 | 488,906 | 425,708 | `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.md` |
| `one_simple` | PARTIAL | N/A | 1,254,475 | N/A | 469,274 | `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.md` |
| `one_complex` | PARTIAL | N/A | 1,256,836 | N/A | 469,842 | `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.md` |
| `ten_simple` | PARTIAL | N/A | 1,271,399 | N/A | 470,988 | `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.md` |
| `ten_complex` | PARTIAL | N/A | 1,272,395 | N/A | 471,219 | `docs/audits/reports/2026-03/2026-03-31/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.md` |

## Follow-Up Actions

- owner boundary: `wasm-audit history`; action: preserve scoped baseline size artifacts so future consolidated summary runs stay comparable.

## Verification Readout

- `WASM_AUDIT_DATE=2026-03-31 bash scripts/ci/wasm-audit-report.sh` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
