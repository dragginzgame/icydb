# Recurring Audit - Query Perf Matrix (2026-06-13)

## Report Preamble

- scope: 0.182 baseline for query performance across the existing SQL and fluent audit harnesses
- code snapshot identifier: `f76650cdd`
- PocketIC binary: `/home/adam/projects/icydb/.cache/pocket-ic-server-14.0.0/pocket-ic`
- SQL artifact: `artifacts/perf-audit/sql_perf_audit_harness_reports_instruction_samples.stdout.txt`
- fluent artifact: `artifacts/perf-audit/fluent_perf_audit_harness_reports_instruction_samples.stdout.txt`

## Checklist Results

| Requirement | Status | Evidence |
| --- | --- | --- |
| SQL query matrix captured | PASS | 94 SQL scenarios emitted by `sql_perf_audit_harness_reports_instruction_samples` |
| Fluent query matrix captured | PASS | 17 fluent scenarios emitted by `fluent_perf_audit_harness_reports_instruction_samples` |
| Cache warm/cold coverage captured | PASS | SQL and fluent artifacts include cold, warm-after-update, and repeat scenarios |
| Blob payload coverage captured | PASS | SQL artifact includes thumbnail, chunk, and full blob payload scenarios |
| Grouped-query attribution captured | PASS | SQL and fluent artifacts include grouped stream/fold/finalize counters |

PASS=5, PARTIAL=0, FAIL=0

## SQL Hotspots

| Scenario | Avg Instructions | Delta | Delta % | Notes |
| --- | ---: | ---: | ---: | --- |
| `user.show_entities` | 18321354 | +18301652 | +92892.35% | Metadata formatting dominates current SQL audit cost. |
| `blob.bucket.full_payload.asc.limit2` | 7181125 | N/A | N/A | Full blob projection is the highest query-shaped raw instruction total. |
| `blob.bucket.chunk_payload.asc.limit2` | 6800621 | N/A | N/A | Chunk projection remains close to full payload cost. |
| `user.explain_execution.lower.order.limit1` | 6633214 | +6066669 | +1070.81% | EXPLAIN output construction is expensive. |
| `user.explain_json.lower.order.limit1` | 6529607 | +6164433 | +1688.08% | JSON explain formatting is a clear optimisation target. |
| `user.explain.lower.order.limit1` | 6407968 | +6088807 | +1907.75% | Text explain formatting is a clear optimisation target. |
| `account.describe` | 5682178 | +5645105 | +15226.99% | Metadata describe output is expensive. |
| `user.describe` | 5543430 | +5507802 | +15459.19% | Metadata describe output is expensive. |
| `user.show_columns` | 5534014 | +5483090 | +10767.20% | Metadata column output is expensive. |

## Fluent Hotspots

| Scenario | Total Instructions | Compile | Runtime | Execute | Cache |
| --- | ---: | ---: | ---: | ---: | --- |
| `user.age.order_only.asc.limit3` | 12107483 | 11290857 | 615512 | 816626 | 0 hits / 1 miss |
| `user.age.order_only.asc.limit2.cold_query` | 12079715 | 11291120 | 611094 | 788595 | 0 hits / 1 miss |
| `user.grouped.age_count.limit10` | 12013625 | 11285020 | 536542 | 728605 | 0 hits / 1 miss |
| `user.id.order_only.asc.limit2` | 11607706 | 11179051 | 246085 | 428655 | 0 hits / 1 miss |
| `repeat.user.age.order_only.asc.limit3.runs10` | 1972269 | 1176232 | 615453 | 796037 | 9 hits / 1 miss |

## Verification Readout

- `bash scripts/ci/wasm-audit-report.sh --date 2026-06-13` -> PASS
- `POCKET_IC_BIN=/home/adam/projects/icydb/.cache/pocket-ic-server-14.0.0/pocket-ic cargo test -p icydb-testing-integration --test sql_perf_audit sql_perf_audit_harness_reports_instruction_samples -- --nocapture` -> PASS
- `POCKET_IC_BIN=/home/adam/projects/icydb/.cache/pocket-ic-server-14.0.0/pocket-ic cargo test -p icydb-testing-integration --test fluent_perf_audit fluent_perf_audit_harness_reports_instruction_samples -- --nocapture` -> PASS

## Follow-Up Actions

- Treat SQL metadata output construction (`SHOW ENTITIES`, `DESCRIBE`, `SHOW COLUMNS`, `EXPLAIN`) as the first runtime optimisation target.
- Treat the SQL-only wasm delta in `one_sql_query` as the first footprint optimisation target.
- Re-run this report after each optimisation slice before blessing updated baselines.
