# RECURRING AUDIT — Wasm Footprint

## Purpose

Track wasm footprint drift over time and identify size drivers with Twiggy.

This is a build-artifact audit.
It is not a correctness audit.
It is not a feature-design audit.

---

## Scope

Measure and report:

- `dfx-built` wasm size (`.wasm` and deterministic `.wasm.gz`)
- canonical `dfx-shrunk` wasm size (`.wasm` and deterministic `.wasm.gz`)
- shrink deltas between built and shrunk artifacts
- `ic-wasm info` structure snapshots (function/data/export counts)
- Twiggy breakdowns (`top`, `dominators`, `monos`) for size attribution

Default targets:

- canisters: `minimal` and `twenty`
- profile: `wasm-release`

---

## Required Checklist

For each run, explicitly mark `PASS` / `PARTIAL` / `FAIL` with concrete evidence.

1. Wasm artifacts were built and captured for each target canister/profile in scope.
2. Artifact sizes were recorded in a machine-readable artifact.
3. Twiggy `top` output was captured for offender ranking.
4. Twiggy `dominators` output was captured for retained-size ownership.
5. Twiggy `monos` output was captured for generic bloat signal.
6. Baseline path was selected according to daily baseline discipline.
7. Size deltas versus baseline were recorded when comparable baseline artifacts exist.
8. Verification readout includes command outcomes with `PASS`/`FAIL`/`BLOCKED`.

---

## Execution Contract

Preferred command:

- `bash scripts/ci/wasm-audit-report.sh`

Optional controls:

- `WASM_AUDIT_DATE=YYYY-MM-DD` (pins report day path)
- `WASM_AUDIT_SKIP_BUILD=1` (reuse existing artifacts in `artifacts/wasm-size`)
- `WASM_CANISTER_NAME=<name>` and `WASM_PROFILE=<profile>` (single-canister override)

---

## Output Contract

Write one dated result file for each canister run:

- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/wasm-footprint*.md`

Write artifacts under:

- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/artifacts/wasm-footprint/`

Required artifacts for each run:

- copied size report JSON (`*.size-report.json`)
- copied size summary markdown (`*.size-summary.md`)
- Twiggy top (`*.twiggy-top.txt`, `*.twiggy-top.csv`)
- Twiggy retained hotspots (`*.twiggy-retained.csv`)
- Twiggy dominators (`*.twiggy-dominators.txt`)
- Twiggy monos (`*.twiggy-monos.txt`)

Result must include:

- report preamble fields required by `docs/audits/AUDIT-HOWTO.md`
- checklist table with status and evidence
- size snapshot table (previous/current/delta)
- Twiggy-derived culprit tables
- explicit follow-up actions for each `PARTIAL`/`FAIL`
- verification readout section

Do not overwrite prior dated results.
