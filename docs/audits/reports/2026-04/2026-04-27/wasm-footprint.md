# Recurring Audit - Wasm Footprint (2026-04-27)

## Report Preamble

- scope: recurring wasm footprint audit for `minimal, one_simple, one_complex, ten_simple, ten_complex` with profile `wasm-release` and SQL variant `sql-on`
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-22/wasm-footprint.md`
- code snapshot identifier: `9890a4d7a`
- method tag/version: `WASM-1.0`
- comparability status: `comparable`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Wasm size artifacts captured | PASS | per-canister size reports + summaries written under `artifacts/wasm-footprint/` |
| Twiggy top breakdown generated | PASS | per-canister top text/csv artifacts written |
| Twiggy dominator breakdown generated | PASS | per-canister dominator text artifacts written |
| Twiggy monomorphization breakdown generated | PASS | per-canister monos artifacts written |
| Baseline delta availability | PASS | baseline size artifacts loaded for all canisters |

PASS=5, PARTIAL=0, FAIL=0

## Per-Canister Size Snapshot

| Canister | Baseline Status | Previous shrunk `.wasm` | Current shrunk `.wasm` | Previous shrunk `.wasm.gz` | Current shrunk `.wasm.gz` | Detail Report |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| `minimal` | PASS | 633,590 | 636,717 | 226,298 | 227,645 | `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.md` |
| `one_simple` | PASS | 832,381 | 841,039 | 300,012 | 303,771 | `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_simple.wasm-release.sql-on.md` |
| `one_complex` | PASS | 833,407 | 842,069 | 300,251 | 304,000 | `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.one_complex.wasm-release.sql-on.md` |
| `ten_simple` | PASS | 846,925 | 854,542 | 302,117 | 305,418 | `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_simple.wasm-release.sql-on.md` |
| `ten_complex` | PASS | 848,027 | 855,516 | 302,446 | 305,813 | `docs/audits/reports/2026-04/2026-04-27/artifacts/wasm-footprint/wasm-footprint.ten_complex.wasm-release.sql-on.md` |

## Follow-Up Actions

- No follow-up actions required for this run.

## Verification Readout

- `WASM_AUDIT_DATE=2026-04-27 bash scripts/ci/wasm-audit-report.sh` -> PASS
- per-canister size-report JSON + Twiggy artifacts -> PASS
