> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# 0.206 SQL Performance Remediation Investigation

Status: complete; the corrected Slice 2 candidate and hard-cut subject identity
passed exact three-run cohort review, the ordinary baseline gate reproduced all
maintained evidence, and reviewed run `29751989919` is selected.

## Decision

The accepted post-0.205 cohort freezes a 14-family leading set. Exact selection
run `29696076149` freezes 206-003 as the sole `OptimizeNow` finding. The selected
family is
`scale.user.unsupported_order.all.window10`: at 2,048 rows it costs 104,312,717
instructions, including 35,243,583 in the kernel order-window phase, and the
current source retains the complete qualifying row set before applying the
bounded expression order.

The Slice 1 measurement patch added only
`kernel_row_peak_retained_candidates` at the canonical kernel scan collector.
It changed neither semantics nor dispatch. The exact current-source run records
16, 256, and 2,048 retained candidates at the corresponding cardinalities, so
the predeclared 2,048-to-at-most-11 structural budget is now measured and
frozen. The direct-field incompatible-order guard already records 11 retained
candidates at 2,048 rows.

No directly coupled family is proposed. The leading incompatible-filter/order
family already uses direct-field bounded kernel collection and does not share
the missing expression-order key materialization invariant.

## Closeout Comparison Decision

Selection run `29696076149` and the corrected candidate cohort differ at
`Cargo.lock`: workspace versions changed from 0.206.0 to 0.206.2, and the
candidate lock resolves `serde_json` 1.0.151 and `syn` 3.0.2. Treating that
lockfile as external environment was the ownership error that made ordinary
release versioning invalidate the baseline. 0.206.3 hard-cuts the artifact
shape so the lock hash is recorded with source revision and raw Wasm as measured
subject provenance. Dependency changes remain visible, and their performance
effects participate in the candidate verdict instead of making it inadmissible.

There is no decoder for the superseded environment shape and no synthetic
historical-source or source-override workflow lane. Existing 0.206 artifacts
remain immutable historical evidence; they are not rewritten to manufacture a
new exact comparison. Ordinary run `29748512253` was cancelled during its
shared-Wasm build, before shard execution, once the ownership hard cut made its
superseded-shape result non-authoritative.

The maintained performance claims are therefore narrower and exact:

- peak retained candidates fall from an exactly observed 2,048 in the selected
  old implementation to exactly 11 in every clean candidate ordinal;

- the candidate's absolute instruction, store-call, result-signature, and raw
  Wasm observations are exact and stable across the accepted three-run cohort;

- all 1,787 candidate P1 scenarios pass and all three ordinals produce the same
  424-scenario P2 selection and attribution overhead; and

## Source Ownership And Disposition

Source inspection was performed only after the table above was frozen.

`SchemaDependent` does not recommend changing the benchmark fixture. It states
that the required engine work is removable only through an application-declared
compatible index or a different query. Adding an index to claim an engine win
would violate 0.206.

The closeout concerns outside the sole selected family also have explicit
owners and dispositions. Materialized expression order requires the complete
scan but no longer requires complete candidate retention; 206-003 removes that
avoidable engine work. Incompatible direct-field filter/order remains
`SchemaDependent` under 206-009. The maintained residual-primary scenario reads
1,546 rows at 2,048-row scale before producing the ordered ten-row window,
retains only ten candidates, and spends no materialized-order phase work; the
residual ordered-scan owner is therefore behaving according to the accepted
schema, while removing the row reads requires a compatible application index.
Scalar aggregate scan cost is `SchemaDependent` under 206-007. Large-membership
compile/planner work is owned by SQL lowering and the prefix access planner
under 206-010 through 206-014, and remains `NeedsTypedMeasurement` rather than
an unproved optimization claim.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/07/19/sql-performance-remediation/0.206/report.md).
