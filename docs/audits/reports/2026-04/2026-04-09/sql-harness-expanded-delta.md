# Expanded SQL Harness Delta

This report captures a current-worktree vs committed-`HEAD` SQL instruction delta using the expanded PocketIC SQL perf harness cohort.

## Scope

- date: `2026-04-09`
- current tree: local worktree with uncommitted changes on top of `31e27185fb4b746c7023a2b28186cf6bfd9aef95`
- baseline tree: detached worktree at `31e27185fb4b746c7023a2b28186cf6bfd9aef95`
- harness: `testing/pocket-ic/tests/sql_canister.rs`
- benchmark entrypoint: `sql_canister_perf_harness_reports_positive_instruction_samples`
- compared scenario rows: `139`

## Method

- The expanded fixture/harness cohort only exists in the current worktree, so the detached `HEAD` worktree was patched with the current benchmark-only files before the baseline run:
  - `testing/pocket-ic/tests/sql_canister.rs`
  - `canisters/test/sql_parity/src/lib.rs`
  - `schema/test/sql_parity/src/fixtures.rs`
- This keeps the benchmark shape fixed while comparing current engine/runtime behavior against committed `HEAD`.

## Summary

- regressions above `1.0%`: `19`
- regressions above `3.0%`: `4`
- improvements below `-1.0%`: `0`
- largest percentage regression: `generated.dispatch.show_entities` `+1,353` instructions, `+5.68%`
- largest percentage improvement: `typed.execute_sql_grouped.user_age_count.having_empty` `-53,149` instructions, `-0.81%`

The expanded cohort does not show a broad executor regression. The main grouped execution cohort stayed relatively tight, with the largest grouped regression at `+2.64%` on `SELECT name, SUM(age) FROM Customer GROUP BY name ORDER BY name ASC LIMIT 10`. The sharpest percentage moves are on low-cost metadata and explain surfaces, where a roughly `+1.3k` instruction change produces a larger percentage swing.

## Representative Deltas

| Scenario | Baseline | Current | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| `typed.execute_sql_grouped.user_name_sum_age` | 7,219,355 | 7,410,110 | +190,755 | +2.64% |
| `typed.execute_sql_grouped.user_name_count` | 7,011,088 | 7,048,177 | +37,089 | +0.53% |
| `typed.execute_sql_grouped.user_age_count` | 6,543,638 | 6,572,364 | +28,726 | +0.44% |
| `typed.execute_sql_grouped.user_age_count.filtered` | 6,386,826 | 6,408,291 | +21,465 | +0.34% |
| `typed.execute_sql_grouped.user_age_count.having_empty` | 6,594,341 | 6,541,192 | -53,149 | -0.81% |
| `generated.dispatch.show_entities` | 23,818 | 25,171 | +1,353 | +5.68% |
| `generated.dispatch.describe.user` | 36,770 | 38,123 | +1,353 | +3.68% |
| `generated.dispatch.show_indexes.user` | 41,419 | 42,774 | +1,355 | +3.27% |
| `generated.dispatch.show_columns.user` | 44,906 | 46,259 | +1,353 | +3.01% |
| `generated.dispatch.explain.aggregate.user_count` | 113,820 | 115,634 | +1,814 | +1.59% |
| `generated.dispatch.predicate.starts_with_name_limit2` | 997,785 | 1,012,574 | +14,789 | +1.48% |
| `typed.dispatch.predicate.starts_with_name_limit2` | 996,821 | 1,011,554 | +14,733 | +1.48% |

## Grouped Cohort

The new grouped scenarios added by the expanded fixture set stayed in a fairly narrow band:

- `typed.execute_sql_grouped.user_name_sum_age`: `+2.64%`
- `typed.execute_sql_grouped.user_name_count`: `+0.53%`
- `typed.execute_sql_grouped.user_age_count`: `+0.44%`
- `typed.execute_sql_grouped.user_age_count.filtered`: `+0.34%`
- `typed.execute_sql_grouped.user_name_count.limit3.second_page`: `+0.03%`
- `typed.execute_sql_grouped.user_age_count.limit2.first_page`: `-0.01%`
- `typed.execute_sql_grouped.user_name_count.limit3.first_page`: `-0.10%`
- `typed.execute_sql_grouped.user_age_count.limit2.second_page`: `-0.15%`
- `typed.execute_sql_grouped.user_age_count.having_empty`: `-0.81%`

## Artifacts

- current rows: [sql-harness-expanded-current.json](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-09/artifacts/perf-audit/sql-harness-expanded-current.json)
- baseline rows: [sql-harness-expanded-head.json](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-09/artifacts/perf-audit/sql-harness-expanded-head.json)
- full delta table: [sql-harness-expanded-delta.tsv](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-09/artifacts/perf-audit/sql-harness-expanded-delta.tsv)
- run log: [sql-harness-expanded-verification-readout.md](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-09/artifacts/perf-audit/sql-harness-expanded-verification-readout.md)
