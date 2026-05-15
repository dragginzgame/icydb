# RECURRING AUDIT — Wasm Footprint

## Purpose

Track wasm footprint drift over time and identify size drivers with Twiggy.

This is a build-artifact audit.
It is not a correctness audit.
It is not a feature-design audit.

---

## Scope

Measure and report:

- `icp-built` wasm size (`.wasm` primary, deterministic `.wasm.gz` secondary)
- canonical `icp-shrunk` wasm size (`.wasm` primary, deterministic `.wasm.gz` secondary)
- shrink deltas between built and shrunk artifacts
- `ic-wasm info` structure snapshots (function/data/export counts)
- Twiggy breakdowns (`top`, `dominators`, `monos`) for size attribution

Default targets:

- canisters: `minimal`, `one_simple`, `one_complex`, `ten_simple`, and `ten_complex`
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

Decision rule:

- Raw non-gzipped wasm is the optimization authority.
- Use built `.wasm` and shrunk `.wasm` as the primary pass/fail and trend metrics.
- Record deterministic gzip artifacts for transport continuity, but treat them as secondary context rather than the deciding metric for optimization work.

---

## Execution Contract

Preferred command:

- `bash scripts/ci/wasm-audit-report.sh`

Optional controls:

- `--date YYYY-MM-DD` pins the report day path.
- `--skip-build` reuses existing artifacts in `artifacts/wasm-size`.
- `--canister <name>` narrows or repeats the canister scope.
- `--profile <profile>` selects `debug`, `release`, or `wasm-release`.
- `--sql-variant sql-on|sql-off` selects the SQL feature mode.

---

## Output Contract

Write exactly one dated top-level summary file for a batch run:

- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/wasm-footprint.md`

If per-canister markdown detail files are emitted, they MUST live only under:

- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/artifacts/wasm-footprint/`

Top-level numbered variants such as `wasm-footprint-2.md` are prohibited for
batch runs.

Write artifacts under:

- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/artifacts/wasm-footprint/`

Required artifacts for each run:

- copied size report JSON (`*.size-report.json`)
- copied size summary markdown (`*.size-summary.md`)
- per-canister detailed markdown report (`*.md`) when multiple canisters are in scope
- Twiggy top (`*.twiggy-top.txt`, `*.twiggy-top.csv`)
- Twiggy retained hotspots (`*.twiggy-retained.csv`)
- Twiggy dominators (`*.twiggy-dominators.txt`)
- Twiggy monos (`*.twiggy-monos.txt`)

Result must include:

- report preamble fields required by `docs/audits/AUDIT-HOWTO.md`
- one top-level summary report with checklist status and per-canister snapshot links
- either:
  - one combined top-level file that contains all per-canister detail, or
  - per-canister detail markdown files stored under `artifacts/wasm-footprint/`
- explicit follow-up actions for each `PARTIAL`/`FAIL`
- verification readout section

Do not overwrite prior dated results.
