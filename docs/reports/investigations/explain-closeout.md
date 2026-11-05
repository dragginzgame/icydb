> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Published 0.257.22 — whole-call and multi-entity qualification

Closeout disposition, 2026-09-17: the user accepted the diagnostic trade-off and
finite coverage below and authorized closing 0.257. The four cost-gate misses
remain unchanged; the measurements and original recommendation are preserved.

2026-09-17 · published source `f3bd969be9dd7087a0c00e2655d703c397756616`.
Measurement-only follow-up authorized by the user. Production, actor, fixture,
dependency and maintained test sources are unchanged after temporary probe removal.

## Verdict

The requested measurement pass is complete. All result/report comparisons pass.
Whole-endpoint typed diagnostics remain 18.70–20.96% more expensive than matched
SQL in instructions, down from the previously recorded 27.34–30.02%. Ordinary
queries in the maintained multi-entity actor improve in all six cases, and all
18 paired update samples consume fewer charged cycles. These finite fixtures
do not establish a general application or schema-size guarantee.

The unchanged local diagnostic gate still fails four warm equality/sort samples.
Recommend accepting the measured diagnostic trade-off and bounded coverage at
closeout; this is a recommendation, not user acceptance or a passing gate.
No further optimization is authorized by these results.

## Validation and remaining decision

Three focused probes pass; the separately refreshed diagnostic cost gate fails
the same four samples. Thirty-six disposable PocketIC fixtures were created and
released; shared local networks were untouched. No native timing metric, full
repository suite or downstream application test was run. Temporary wiring was
removed; formatting, whitespace, invariants and all 41 checked local links pass.
The handoff changes ten documentation/evidence files, approximately 300 net
lines; zero retained code changes and neutral runtime complexity.

Remaining unmeasured scope is explicit: fully cold multi-entity startup,
diagnostic query charged cycles, arbitrary schema/data scale, non-explain and
SQL-only control rebuilds, and composed application attribution. Existing C1-N,
C1-M and other accepted deferrals remain; this does not prove complete resource
bounds. Recommend accepting this finite qualification scope alongside the
diagnostic cost trade-off. Both require the user's closeout decision; 0.258 is
not started and the checksum-table experiment remains unimplemented.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/17/explain-closeout/06/report.md).
