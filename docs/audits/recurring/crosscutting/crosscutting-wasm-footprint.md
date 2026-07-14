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

- canisters: `default_empty`, `default_empty_metrics`,
  `one_entity_fluent_rows`, `one_entity_fluent_execute`,
  `one_entity_sql_query`, and `ten_entity_fluent_rows`
- profile: `wasm-release`

Default target roles:

- `default_empty` is the zero-export generated-runtime floor. Keep generated
  metrics disabled so it measures baseline runtime retention without IC method
  glue.
- `default_empty_metrics` isolates the compact generated metrics endpoint cost.
  It intentionally starts from the empty schema so metrics/Candid/IC method
  retention is not mixed into query runtime growth.
- `one_entity_fluent_rows` measures the fluent rows-only query endpoint.
- `one_entity_fluent_execute` measures the broader fluent `execute()` response
  path.
- `one_entity_sql_query` measures the SQL query frontend/runtime path.
- `ten_entity_fluent_rows` measures entity-count scale against the one-entity
  fluent rows baseline.

The `sql_perf` audit canister is deliberately excluded from the default
footprint matrix. It is a broad instruction-sampling and access-shape fixture,
not a small attribution fixture, and should be measured explicitly when a perf
scenario needs it.

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

Write exactly one summary file for each batch run:

- `docs/reports/recurring/YYYY/MM/DD/wasm-footprint/<run>/report.md`

Suffixed report names such as `wasm-footprint-2.md` are prohibited. Same-day
reruns use the next run directory.

Write artifacts under:

- `docs/reports/recurring/YYYY/MM/DD/wasm-footprint/<run>/artifacts/`

Required artifacts for each run:

- copied size report JSON (`*.size-report.json`)
- copied size summary markdown (`*.size-summary.md`)
- Twiggy top (`*.twiggy-top.txt`)
- Twiggy retained hotspots (`*.twiggy-retained.csv`)
- Twiggy dominators (`*.twiggy-dominators.txt`)
- Twiggy monos (`*.twiggy-monos.txt`)

Result must include:

- report preamble fields required by `docs/audits/README.md`
- one top-level summary report with checklist status and per-canister size
  summary links
- size attribution detail in copied size summaries and raw Twiggy artifacts
- explicit follow-up actions for each `PARTIAL`/`FAIL`
- verification readout section

Do not overwrite prior dated results.
