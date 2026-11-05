> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Boundary / Envelope Semantics Audit — Storage Proof Follow-Up

## Metadata And Scope

- Authorization: implement BND-01's focused storage proof and rerun the audit.

- Definition: `docs/audits/recurring/range/boundary-envelope-semantics.md`.

- Method: **BOUNDARY-5 + DOMAIN-1**, unchanged from the daily baseline.

- Compared baseline: [2026-09-27 run 01](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/09/27/boundary-semantics/01/report.md), the canonical daily
  baseline, preserved unchanged.

- Code snapshot: `b6111f5f7185136b56860ee35287b206d9debafa`, workspace
  `0.261.11`, plus the pre-existing dirty Cargo inputs and this run's addition
  to `crates/icydb-core/src/db/index/scan/tests.rs`.

- Comparability: **comparable**. Scope, invariants, method, features and
  verification families remain the same. The existing store-scan family gains
  one executable test for an already required obligation.

- Definition SHA-256:
  `2bb0a5c398ecd02b10f8fcd62c503d059eaac52d6c981079f8b20dd6a7ef10ca`.

- Changed test source SHA-256:
  `635996f907893ddb6d3ab4b2dd34930e2d1bed95707b2d5639940d1b0dd6f938`.

- Unchanged `Cargo.toml` SHA-256:
  `b34b059d3b555740d4d2ccdfb2a5ad39f102381294ed87313beee8d5cc78242c`.

## Findings And Verdict

**BND-01 — RESOLVED in this run.** The formerly missing populated-store proof
now covers endpoint collapse, inverted intervals and equal-exclusive intervals
in the live and canonical scanners. The original finding remains immutable
in run 01. No new finding was identified.

**Verdict: PASS for the declared scope.** Required source and behavioral
obligations have sufficient evidence. The baseline's drift triggers remain:
new encodings need independent ordering evidence; merged traversal callers must
preserve exact-prefix envelope preconditions; chunk-progress and public-token
changes need corresponding directional/binding proof.

## Verification Readout

The exact command wrapper bodies and selector sets are retained in the
[baseline verification readout](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/09/27/boundary-semantics/01/report.md#verification-readout).
They are unchanged and were inspected before execution. Selection A includes
the complete `db::index::scan::tests::` family, which now selects the new test
as well. Selection B retains its eight named cases.

All commands ran at `/home/adam/projects/icydb`, with `rustc 1.98.1
(48a229cea 2026-09-01)`, repository Cargo paths from Makefile,
`RUST_TEST_THREADS=8`, `--locked -p icydb-core --lib --features sql`, no
ignored-test override and no property-case reduction. Source inputs are the
snapshot and hashes recorded above. No behavioral result from run 01 is
counted as a new execution or substituted for one.

| Exact command / check | Outcome | Selected / passed / failed / ignored |
| --- | --- | --- |
| `cargo fmt --all` | PASS | N/A |
| `bash /tmp/icydb-boundary-audit-selection.sh --list` | PASS | 62 / N/A / N/A / 0 |
| `bash /tmp/icydb-boundary-audit-selection.sh` | PASS | 62 / 62 / 0 / 0 |
| `bash /tmp/icydb-boundary-audit-admission.sh --list` | PASS | 8 / N/A / N/A / 0 |
| `bash /tmp/icydb-boundary-audit-admission.sh` | PASS | 8 / 8 / 0 / 0 |
| `git diff --check`, local-link and snapshot/hash verification | PASS | N/A |

## Delivery And Follow-Up

This follow-up changes four files: one owner-local test file, this report,
and root/detailed changelogs. The test adds approximately 83 net lines;
overall the follow-up adds approximately 264 lines including audit evidence
and release notes. Runtime implementation shape is unchanged, with no new
behavior axes, execution flows, formats or configuration. Verification debt
BND-01 is resolved.

Version `0.261.11` already has its release tag, so changelog-only notes open
`0.261.12` within the current minor line. Cargo versions and the pre-existing
Cargo dependency changes are untouched. The audit definition and daily
baseline report are also unchanged from run 01.

Raw Wasm size, IC cycles and instruction deltas are **unmeasured**; this change
adds test coverage and makes no runtime performance claim. No implementation
follow-up remains for BND-01. Future evidence belongs in a new audit run.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/09/27/boundary-semantics/02/report.md).
