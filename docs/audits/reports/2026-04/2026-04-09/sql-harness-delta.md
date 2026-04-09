# SQL Harness Delta

This report captures a current-worktree vs committed-`HEAD` SQL instruction delta using the checked-in PocketIC SQL perf harness.

## Scope

- date: `2026-04-09`
- current tree: local worktree with uncommitted changes on top of `31e27185fb4b746c7023a2b28186cf6bfd9aef95`
- baseline tree: detached worktree at `31e27185fb4b746c7023a2b28186cf6bfd9aef95`
- harness: `testing/pocket-ic/tests/sql_canister.rs`
- benchmark entrypoint: `sql_canister_perf_harness_reports_positive_instruction_samples`

## Summary

- compared scenarios: `134`
- regressions above `0.5%`: `0`
- regressions above `1.0%`: `0`
- improvements below `-1.0%`: `0`
- largest percentage regression: `typed.update.user_single` `+4,665` instructions, `+0.40%`
- largest percentage improvement: `generated.dispatch.computed_projection.lower_name_limit2` `-1,300` instructions, `-0.27%`

The broad SQL perf surface is effectively flat in this pass. Read, grouped, aggregate, paging, and delete paths stayed within normal PocketIC noise, and even the largest write-heavy movements remained below `0.5%`.

## Representative Deltas

| Scenario | Baseline | Current | Delta | Delta % |
| --- | ---: | ---: | ---: | ---: |
| `generated.dispatch.projection.user_name_eq_limit` | 571,269 | 571,027 | -242 | -0.04% |
| `typed.dispatch.projection.user_name_eq_limit` | 560,667 | 560,441 | -226 | -0.04% |
| `generated.dispatch.primary_key_covering.user_id_limit1` | 336,364 | 335,445 | -919 | -0.27% |
| `typed.dispatch.primary_key_covering.user_id_limit1` | 328,853 | 327,950 | -903 | -0.27% |
| `typed.execute_sql_grouped.user_age_count` | 615,505 | 616,542 | +1,037 | +0.17% |
| `typed.execute_sql_grouped.user_age_count.limit2.second_page` | 654,284 | 655,321 | +1,037 | +0.16% |
| `typed.execute_sql_aggregate.user_sum_age` | 431,437 | 431,381 | -56 | -0.01% |
| `typed.execute_sql_aggregate.user_avg_age` | 433,879 | 433,823 | -56 | -0.01% |
| `fluent.paged.user_order_id_limit2.first_page` | 627,945 | 627,228 | -717 | -0.11% |
| `fluent.paged.user_order_id_limit2.second_page` | 691,177 | 690,160 | -1,017 | -0.15% |
| `typed.insert.user_single` | 664,437 | 666,607 | +2,170 | +0.33% |
| `typed.update.user_single` | 1,172,550 | 1,177,215 | +4,665 | +0.40% |
| `typed.insert_many_atomic.user_1000` | 868,988,660 | 871,310,625 | +2,321,965 | +0.27% |
| `typed.insert_many_non_atomic.user_1000` | 1,085,258,032 | 1,088,089,046 | +2,831,014 | +0.26% |
| `generated.dispatch.delete` | 1,213,597 | 1,215,102 | +1,505 | +0.12% |
| `typed.dispatch.delete` | 1,235,585 | 1,237,144 | +1,559 | +0.13% |

## Artifacts

- current rows: [sql-harness-current.json](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-09/artifacts/perf-audit/sql-harness-current.json)
- baseline rows: [sql-harness-head.json](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-09/artifacts/perf-audit/sql-harness-head.json)
- full delta table: [sql-harness-delta.tsv](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-09/artifacts/perf-audit/sql-harness-delta.tsv)
- run log: [verification-readout.md](/home/adam/projects/icydb/docs/audits/reports/2026-04/2026-04-09/artifacts/perf-audit/verification-readout.md)
