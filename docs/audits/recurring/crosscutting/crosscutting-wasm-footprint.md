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

The canonical default canister list is `wasm_report_default_canisters` in
[wasm-report-common.sh](../../../../scripts/ci/wasm-report-common.sh).
It includes the empty/metrics controls, four nested-relation controls, query
and entity-scale actors, request-future scaling, `sql_perf`, and `sql`.
Use that helper's current list rather than maintaining a second exact matrix here.

Default build settings:

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
- The `nested_relation_*` actors isolate no-relation, direct, shallow, and
  repeated relation shapes under a shared operation harness.
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

`WASM-4.0` binds capture to the existing version-1 report provenance. Both
fresh-build and `--skip-build` runs verify the final Wasm and gzip byte counts
and SHA-256 hashes against the copied size report before attribution. Twiggy
reads a private verified Wasm copy, so later changes to shared build outputs
cannot change the attributed bytes. The requested canister, profile, and SQL
variant must match the report.

Every actor in a batch must have identical recorded provenance, tools, and
pipeline identity. Mixed revisions, trees, lockfiles, dirty flags, or build
environments reject the batch before a summary can claim success. The summary
uses the recorded source revision/tree/dirty state and lockfile hash, never
the checkout's current `HEAD`. A dirty flag does not identify uncommitted file
contents; dirty captures remain non-comparable even when their metadata agrees.

Raw size metrics are unchanged. Older reports may supply those stable comparison
anchors when their artifact metadata passes the existing comparison contract;
the new capture checks do not retroactively verify historical attribution.
Record this distinction when comparing methods.

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

Apply [Authorization And Read-Only Work](../../README.md#authorization-and-read-only-work).
Both normal execution and `--skip-build` write reports and attribution artifacts.
For inspection-only work, read existing reports and artifacts without invoking
this writer. Authorized capture reserves a new run directory; an existing
directory, including one supplied with `--report-dir`, must not be reused.

Preferred command:

- `bash scripts/ci/wasm-audit-report.sh`

Optional controls:

- `--date YYYY-MM-DD` pins the report day path.
- A canonical `--report-dir` carries its own date/run identity; that path's day
  also supplies the report date. For output outside the canonical hierarchy,
  `--date` supplies the comparison day.
- `--skip-build` reuses existing artifacts in `artifacts/wasm-size`, subject to
  the same hash, byte-count, subject, and batch-provenance checks as fresh builds.
- `--canister <name>` narrows or repeats the canister scope.
- `--profile <profile>` selects `debug`, `release`, or `wasm-release`.
- `--sql-variant sql-on|sql-off` selects the SQL feature mode.

Same-day reruns compare to run `01` even if it is non-comparable; if its report
is absent, record `N/A` rather than substituting another day's report. A first run
selects the latest earlier report whose artifacts are comparable for every
requested canister, or records `N/A`. Explicit paths and backdated captures
follow the same rule; future reports never supply a baseline.

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
