> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# 0.246 Query Execution Convergence Baseline

## 0. Run Metadata + Comparability Note

- scope: clean complete SQL P1, scale and P2 measurement plus prepared
  aggregate and scalar `DISTINCT` materialization inspection for 0.246 Patch 1

- definition path:
  `docs/audits/recurring/crosscutting/crosscutting-perf-audit.md`

- compared baseline report path:
  [report.md](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/08/26/perf-audit/01/report.md)

- code snapshot identifier: tag `v0.245.1`, revision
  `630cc9c974294631ff77a77d69a2704581ccb2dd`, tree
  `4b1c4464679e836bfb22da61b72080db1133b7bd`

- method tag/version: `PERF-0.246-clean-matrix/v1`

- comparability status: exact clean current-source baseline; no historical
  instruction-delta verdict is made because the preceding recurring report was
  a dirty, focused debug run rather than the complete current profile

- auditor: Codex

- run timestamp (UTC): `2026-08-27T16:02:28Z`

- branch: `main`

- worktree: clean detached measurement checkout at the exact released tag

## 6. Patch 2 Decision

Patch 2 is eligible for one bounded implementation attempt.

The prerequisite is met structurally and across two semantic consumers:

- prepared aggregate: 109,234,618 instructions, 2,048 row gets, 2,048 retained
  rows delivered only after the vector is complete;

- scalar projection `DISTINCT`: 109,375,905–118,738,995 instructions, 2,048
  row gets and no more than five canonical distinct outputs; and

- both enter existing consumer-owned semantics after the common scalar
  retained-row boundary.

This does not pre-approve retention. Patch 2 must measure at least 25M and 25%
savings for each consumer family. Its implementation must share only projected
value delivery; aggregate folding and `DISTINCT` tuple identity, ordering,
pagination and budget charging remain separate. A result that helps only the
aggregate case or only one expression/type is a measured no-build.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/08/27/perf-audit/01/report.md).
