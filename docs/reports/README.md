# IcyDB Reports

This directory contains immutable outputs from executed audits. Audit
definitions and reusable playbooks live under `docs/audits/`.

## Ownership Hierarchy

```text
docs/reports/
├── recurring/YYYY/MM/DD/<scope>/<run>/
├── releases/<version>/closeout/YYYY-MM-DD/<run>/
└── investigations/YYYY/MM/DD/<scope>/<run>/
```

Every run directory owns its complete result:

```text
<run>/
├── report.md
├── findings.json        # when structured findings exist
└── artifacts/           # when supporting output exists
```

The first run for a recurring scope on a date is `01`; same-day reruns use
`02`, `03`, and so on. A rerun never creates a suffixed scope or report name.

## Classification

- `recurring/` contains executions of definitions from
  `docs/audits/recurring/` and their run summaries.
- `releases/` contains version closeout evidence.
- `investigations/` contains one-time, incident, comparison, and bounded
  follow-up reports.

Test fixtures under `canisters/audit/` and `schema/audit/` are executable audit
inputs, not documentation reports. Temporary benchmark output under
`artifacts/` is not committed report history until it is deliberately captured
beneath an owning report run.

## Retention

Reports and structured findings are append-only. Do not overwrite, delete,
alias, or copy them to preserve an old path. New evidence receives a new run.

Artifacts are committed only when at least one of these is true:

- tooling consumes the file as the current comparison baseline;
- the file is explicitly linked as unique evidence;
- the owning report cannot preserve the material result without it.

Delete raw search output, duplicate CSV/text renderings, derived TSV tables
already summarized by the report, and superseded baselines. Reproducible
working output belongs under the repository-level `artifacts/` directory and
must not be promoted into report history by default.

Historical reports may quote former storage paths or artifacts that were later
pruned under this policy. Those strings are execution-time evidence; this
hierarchy and retention policy are the only current contracts.
