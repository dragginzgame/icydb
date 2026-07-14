# Recurring Audit - Wasm Footprint (2026-04-03)

## Report Preamble

- scope: partial recurring wasm footprint refresh after the generated query
  route collapse
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-02/wasm-footprint.md`
- code snapshot identifier: `625d1d31`
- method tag/version: `WASM-1.0`
- comparability status: `partial`
  - current-tree `minimal` `sql-on` and `sql-off` summaries are preserved below
  - the full recurring multi-canister rerun did not complete because fresh
    audited builds now hit a crates.io resolution mismatch on
    `canic-cdk = ^0.22.3`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| Current `minimal` SQL-on size artifact preserved | PASS | current summary + JSON copied under `artifacts/wasm-footprint/` |
| Current `minimal` SQL-off size artifact preserved | PASS | current summary + JSON copied under `artifacts/wasm-footprint/` |
| Full recurring multi-canister rerun | FAIL | `bash scripts/ci/wasm-audit-report.sh` stopped on crates.io resolution mismatch |
| Broad baseline delta availability | PARTIAL | only `minimal` current-tree artifacts were available for this blocked refresh |

PASS=2, PARTIAL=1, FAIL=1

## Current Minimal Snapshot

| Variant | Built `.wasm` | Shrunk `.wasm` | Detail Artifact |
| --- | ---: | ---: | --- |
| `sql-on` | `779,081` | `722,820` | `docs/audits/reports/2026-04/2026-04-03/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md` |
| `sql-off` | `684,317` | `637,210` | `docs/audits/reports/2026-04/2026-04-03/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-off.size-summary.md` |

Current `minimal` SQL premium on the accepted tree:

- built `.wasm`: `+94,764`
- shrunk `.wasm`: `+85,610`

These numbers match the post-route-collapse current tree and are materially
better than the earlier pre-collapse `minimal` SQL-on baseline.

## Blocker

The full recurring rerun did not complete. The attempted command was:

- `WASM_AUDIT_DATE=2026-04-03 bash scripts/ci/wasm-audit-report.sh`

The build stopped immediately on fresh dependency resolution with:

- `failed to select a version for the requirement canic-cdk = "^0.22.3"`
- crates.io candidates visible during the run: `0.22.2`, `0.22.1`, `0.22.0`

That means the current tree can still report the already-built `minimal`
current-tree wasm artifacts, but it cannot yet refresh the full
`minimal/one_simple/one_complex/ten_simple/ten_complex` recurring matrix until
the dependency resolution mismatch is fixed.

## Artifacts

- current `minimal` SQL-on summary:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-summary.md`
- current `minimal` SQL-on JSON:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-on.size-report.json`
- current `minimal` SQL-off summary:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-off.size-summary.md`
- current `minimal` SQL-off JSON:
  - `docs/audits/reports/2026-04/2026-04-03/artifacts/wasm-footprint/wasm-footprint.minimal.wasm-release.sql-off.size-report.json`

## Verification Readout

- `WASM_AUDIT_DATE=2026-04-03 bash scripts/ci/wasm-audit-report.sh` -> FAIL
- blocker: crates.io resolution mismatch on `canic-cdk = "^0.22.3"`
- current-tree `minimal` SQL-on and SQL-off summaries copied into the dated
  audit directory -> PASS
