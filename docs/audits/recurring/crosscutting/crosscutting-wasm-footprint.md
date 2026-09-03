# RECURRING AUDIT — Wasm Footprint

## Purpose

Track wasm footprint drift over time and identify size drivers with Twiggy.

This is a build-artifact audit.
It is not a correctness audit.
It is not a feature-design audit.

---

## Scope

Measure and report:

- compiler-emitted Wasm and canonical Binaryen-transformed final deployable
  size (`.wasm` primary, deterministic `.wasm.gz` secondary)
- exact post-link raw-byte and basis-point reduction from compiler output to
  final deployed bytes
- `ic-wasm info` structure snapshots (function/data/export counts)
- Twiggy breakdowns (`top`, `dominators`, `monos`) for size attribution

The exact default targets are:

- canisters: `default_empty`, `default_empty_metrics`, `one_entity_dynamic_query`,
  `one_entity_reachable_operations`, `one_entity_typed_query`,
  `one_entity_sql_query`, `request_future_scale`, `ten_entity_typed_query`,
  `ten_entity_reachable_operations`, `sql_perf`, and `sql`
- profile: `wasm-release`
- build profile: production, `--no-default-features`, exact maintained
  production features, and Candid metadata enabled

Default target roles:

- `default_empty` is the zero-export generated-runtime floor. Keep generated
  metrics disabled so it measures baseline runtime retention without IC method
  glue.
- `default_empty_metrics` isolates the generated entity-cost metrics endpoint.
  It intentionally starts from the empty schema so metrics/Candid/IC method
  retention is not mixed into query runtime growth.
- `one_entity_typed_query` measures the generated typed projection over the
  accepted dynamic-query lane.
- `one_entity_sql_query` measures the SQL query frontend/runtime path.
- `ten_entity_typed_query` measures entity-count scale against the one-entity
  typed-query baseline.

`sql_perf` and the mutation-capable `sql` actor are deliberately included so
the footprint matrix covers the exact actors used by instruction and mutation
evidence rather than extrapolating from small query fixtures.

Every report carries source revision/tree/dirty state, lockfile identity,
build and target roots, exact features, Rust identity, `ic-wasm` version/hash,
the pinned Binaryen version/hash and flags, Candid identity, exports, accepted
Wasm features, and final raw artifact identity. Dirty reports remain useful
locally but cannot become a baseline or satisfy a regression verdict.

The checked-in comparison ledger classifies the controlled metrics,
entity-scale and request-future pairs as attributable. Typed and SQL ingress
remain directional because their maintained actors differ in more than one
owner.

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

- Raw non-gzipped final deployable wasm is the optimization authority.
- The staged Binaryen `-Oz` output is the final artifact. Compiler output and
  any separately experimented shrink output cannot supply a baseline, delta
  verdict, runtime proof, or installed-byte identity.
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
