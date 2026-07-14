# SQL Perf Audit

Date: 2026-04-29

## Scope

This report stores the SQL perf audit run from the current worktree and compares
it against the recovered 2026-04-28 `after` artifact.

## Command

```bash
POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic CARGO_TARGET_DIR=/tmp/icydb-perf-audit-20260429-target cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_audit_harness_reports_instruction_samples -- --nocapture
```

## Artifacts

- `artifacts/perf-audit/sql-perf-current.json`
- `artifacts/perf-audit/sql-perf-delta-vs-2026-04-28-after.tsv`

## Summary

Compared rows: 92

| Metric | Value |
|---|---:|
| 2026-04-28 after total instructions | 62,790,190 |
| 2026-04-29 current total instructions | 62,818,514 |
| Delta | +28,324 |
| Delta percent | +0.05% |
| Improved rows | 27 |
| Regressed rows | 65 |

## Largest Regressions

| Scenario | Delta | Delta % |
|---|---:|---:|
| `user.grouped.case_sum.having_alias.order.limit5.cold_query` | +59,886 | +3.92% |
| `repeat.user.distinct.age.order_only.asc.limit3.runs10` | +16,121 | +2.92% |
| `repeat.user.grouped.case_sum.having_alias.order.limit5.runs10` | +10,562 | +1.30% |
| `user.age_plus_rank.alias_order.asc.limit3` | +5,569 | +0.71% |
| `user.age_plus_rank.direct_order.asc.limit3` | +5,546 | +0.71% |

## Largest Improvements

| Scenario | Delta | Delta % |
|---|---:|---:|
| `user.explain_json.lower.order.limit1` | -26,215 | -4.26% |
| `user.explain.lower.order.limit1` | -25,668 | -4.54% |
| `user.explain_execution.lower.order.limit1` | -23,226 | -2.93% |
| `user.age_div3_round.direct_order.desc.limit3` | -12,823 | -1.51% |
| `repeat.user.case_where.order_id.limit3.runs10` | -9,446 | -1.99% |

