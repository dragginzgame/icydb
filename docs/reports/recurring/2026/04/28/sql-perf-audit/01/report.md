# SQL Perf Audit

Date: 2026-04-28

## Scope

This report stores the recovered local SQL perf audit artifacts from `/tmp` so
later comparisons do not depend on ephemeral files.

## Artifacts

- `artifacts/perf-audit/sql-perf-before.json`
- `artifacts/perf-audit/sql-perf-after.json`
- `artifacts/perf-audit/sql-perf-delta.tsv`

## Summary

Compared rows: 92

| Metric | Value |
|---|---:|
| Before total instructions | 73,702,838 |
| After total instructions | 62,790,190 |
| Delta | -10,912,648 |
| Delta percent | -14.81% |
| Improved rows | 88 |
| Regressed rows | 4 |

## Largest Improvements

| Scenario | Delta | Delta % |
|---|---:|---:|
| `account.tier.in.limit3` | -467,422 | -19.92% |
| `account.tier_gold.lower.handle_prefix.limit3` | -341,011 | -18.15% |
| `account.tier_gold.handle_prefix.limit3` | -316,885 | -19.68% |
| `user.name.lower.range.limit3` | -303,315 | -15.84% |
| `user.name.range.limit3` | -283,702 | -14.81% |

## Regressions

| Scenario | Delta | Delta % |
|---|---:|---:|
| `user.explain_json.lower.order.limit1` | +1,369 | +0.22% |
| `account.show_indexes` | +241 | +0.60% |
| `user.explain.lower.order.limit1` | +97 | +0.02% |
| `user.show_tables` | +12 | +0.05% |

