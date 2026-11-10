> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# IcyDB Technical Debt Audit

Date: 2026-07-04

## Executive summary

IcyDB is not accumulating uncontrolled technical debt. The repository shows a
strong pattern of paying down planner, cursor, admission, schema-authority, and
durability debt as slices land. The current risk is different: the system is
moving fast enough that documentation, diagnostic schemas, perf attribution, and
test tripwires are lagging the architecture.

Top 10 technical debt findings:

Top 5 cleanup PRs to do immediately:

1. Update query/cursor contracts to describe semantic guarantees separately from
   route-dependent pushdown facts.
2. Add the three missing fast-path guard tests named by
   `docs/governance/fast-path-inventory.md`.
3. Split SQL perf matrix route classification into a structured helper with
   scenario metadata and unit tests, preserving the current dirty grouped
   aggregate classification.
4. Add 0.197 cache/proof tripwire tests before implementing primary-key
   canonicalization.
5. Refresh `docs/roadmap/1.0-TODO.md` so closed items, intentional non-goals, and real
   blockers are not mixed.

## Scope

Repository commit observed at final audit point:
`1f4fb54ac062bbdb8538b4888251b444e6f0ebcd`.

The audit started with `ec8c119a2359921232ea36b6ca4a252314d19192`; the worktree
changed during the audit. I treated later changes as user work and did not
revert them.

Dirty worktree:

- Initial dirty files:
  - `CHANGELOG.md`
  - `docs/changelog/0.196.md`
  - `docs/design/0.196-sqlite-comparison-audit/implementation-results.json`
  - `docs/design/0.196-sqlite-comparison-audit/implementation-results.md`
  - `testing/integration/tests/sql_perf_matrix_audit.rs`

- Final pre-report dirty file before adding this audit:
  - `testing/integration/tests/sql_perf_matrix_audit.rs`

- Final validation dirty files after writing this audit:
  - `CHANGELOG.md`
  - `docs/changelog/0.196.md`
  - `docs/design/0.196-sqlite-comparison-audit/implementation-results.json`
  - `docs/design/0.196-sqlite-comparison-audit/implementation-results.md`
  - `testing/integration/tests/sql_perf_matrix_audit.rs`
  - `docs/audits/reports/2026-07/2026-07-04/` (new audit artifacts from this task)

## Final recommendation

Is IcyDB accumulating serious technical debt?

- Mostly no. The architecture is being actively simplified and documented. The
  serious debt is hardening debt that will become expensive at 1.0 if not closed.

Where is the debt most dangerous?

- Persisted byte formats, migration/recovery lifecycle, public DTO schemas,
  cursor token/contract closure, and 0.197 proof/cache/admission ownership.

What should be cleaned immediately?

- Query/cursor contract drift, fast-path guard gaps, 0.197 cache/proof tests,
  SQL perf classifier structure, and stale 1.0/0.196 documentation.

What should wait until after 0.196?

- Primary-key canonicalization implementation. 0.196 should remain diagnostic
  and closeout-clean.

What should wait until after 0.197?

- Deeper row-codec/visitor optimization and broad perf cleanup should wait for
  0.197 correctness and perf evidence unless they are measurement-only.

What should be fixed before 1.0?

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/07/04/technical-debt/01/report.md).
