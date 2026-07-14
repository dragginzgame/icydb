# SQL Perf Audit

Date: 2026-04-30

## Scope

This report stores the full SQL perf harness run from the current worktree and
compares it against the stored 2026-04-29 current artifact.

## Command

```bash
POCKET_IC_BIN=/tmp/pocket-ic-server-13.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_audit_harness_reports_instruction_samples -- --nocapture
```

## Artifacts

- `artifacts/perf-audit/sql-perf-current.json`
- `artifacts/perf-audit/sql-perf-current-raw.tsv`
- `artifacts/perf-audit/sql-perf-delta-vs-2026-04-29.tsv`

## Summary

Compared rows: 92

| Metric | Value |
|---|---:|
| 2026-04-29 current total instructions | 62,818,514 |
| 2026-04-30 current total instructions | 67,169,438 |
| Delta | +4,350,924 |
| Delta percent | +6.92% |
| Improved rows | 5 |
| Regressed rows | 87 |

The harness's embedded baseline comparison still shows most comparable rows
faster than the checked-in baseline: 72 improved and 7 regressed across 79
baseline-backed rows, for a total delta of `-33,026,351` instructions
(`-35.93%`).

## Largest Regressions vs 2026-04-29

| Scenario | Delta | Delta % |
|---|---:|---:|
| `account.tier.in.limit3` | +548,324 | +29.14% |
| `user.name.lower.range.limit3` | +223,022 | +13.80% |
| `account.tier_gold.lower.handle_prefix.limit3` | +207,409 | +13.47% |
| `user.age.in.limit3` | +185,383 | +17.14% |
| `user.name.range.limit3` | +185,327 | +11.34% |

## Improvements vs 2026-04-29

| Scenario | Delta | Delta % |
|---|---:|---:|
| `repeat.user.grouped.case_sum.having_alias.order.limit5.runs10` | -11,704 | -1.42% |
| `user.grouped.case_sum.having_alias.order.limit5.warm_after_update` | -10,274 | -1.40% |
| `repeat.user.grouped.age_count.limit10.runs100` | -6,270 | -1.31% |
| `repeat.user.grouped.age_count.limit10.runs10` | -5,283 | -1.05% |
| `repeat.user.grouped.age_count.no_order.runs10` | -3,298 | -0.70% |

## Highest Current-Cost Rows

| Scenario | Compile | Execute | Total |
|---|---:|---:|---:|
| `account.tier.in.limit3` | 1,181,534 | 1,248,176 | 2,429,710 |
| `user.name.lower.range.limit3` | 617,551 | 1,220,624 | 1,838,175 |
| `user.name.range.limit3` | 422,932 | 1,396,514 | 1,819,446 |
| `account.tier_gold.lower.handle_prefix.limit3` | 764,161 | 982,935 | 1,747,096 |
| `user.grouped.case_sum.having_alias.order.limit5.cold_query` | 466,833 | 1,137,882 | 1,604,715 |

## Validation

The full sampler passed:

```text
test sql_perf_audit_harness_reports_instruction_samples ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out
```
