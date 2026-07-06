# 0.197 Focused Primary-Key Canonicalization Before Baseline

- Scenario rows: 33
- Counter-measured rows: 8
- Admitted counter-measured rows: 7
- Contract/not-measured rows: 5
- Non-admitted/fail-closed rows: 26

| Scenario | Canonicalization | Access | Admission | Error | Instructions | data_store.get | Rows Decoded | Rows Returned | Result |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `pk.scalar.generated.filter.existing.try_one` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.scalar.generated.filter.missing.try_one` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.scalar.generated.by_id.existing.try_one` | `ByKey` | `ByKey` | `admitted` | `null` | 276970 | 1 | 1 | 1 | `rows\|PerfAuditUser\|1\|Id(1)` |
| `pk.scalar.external.filter.existing.try_one` | `ByKey` | `ByKey` | `contract_backed_not_measured` | `null` | 0 | 0 | 0 | 1 | `contract_backed_not_measured` |
| `pk.scalar.external.by_id.existing.try_one` | `ByKey` | `ByKey` | `contract_backed_not_measured` | `null` | 0 | 0 | 0 | 1 | `contract_backed_not_measured` |
| `pk.sql.literal.generated.existing` | `ByKey` | `ByKey` | `admitted` | `null` | 835701 | 1 | 1 | 1 | `projection\|PerfAuditUser\|1\|1,Alice` |
| `pk.sql.literal.generated.commuted` | `ByKey` | `ByKey` | `admitted` | `null` | 658563 | 1 | 1 | 1 | `projection\|PerfAuditUser\|1\|1,Alice` |
| `pk.sql.parameter.unsupported` | `UnsupportedByContract` | `UnsupportedByContract` | `rejected` | `E181` | 0 | 0 | 0 | 0 | `error` |
| `pk.sql.literal.generated.wrong_type` | `ValidationFailure` | `ValidationFailure` | `rejected` | `E3` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.empty` | `Empty` | `ByKeys` | `admitted` | `null` | 449185 | 0 | 0 | 0 | `rows\|PerfAuditUser\|0\|` |
| `pk.in.fluent.one` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.duplicates` | `ByKeys` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.multiple_mixed` | `ByKeys` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.raw_terms_over_budget` | `ByKeys` | `ByKeys` | `public_policy_rejected_not_measured` | `E204` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.deduped_over_budget` | `ByKeys` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.by_ids.raw_terms_over_budget` | `ByKey` | `ByKey` | `public_policy_rejected_not_measured` | `E204` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.sql.duplicates.order_asc` | `ByKeys` | `ByKeys` | `admitted` | `null` | 3753647 | 2 | 2 | 2 | `projection\|PerfAuditUser\|2\|1;2` |
| `pk.in.sql.payload_over_budget` | `ByKeys` | `ByKeys` | `public_policy_rejected_not_measured` | `E204` | 0 | 0 | 0 | 0 | `error` |
| `pk.residual.eq.true` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.residual.eq.false` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.residual.eq.invalid_existing` | `ValidationFailure` | `ExplainError(E3)` | `rejected` | `E3` | 0 | 0 | 0 | 0 | `error` |
| `pk.residual.eq.invalid_missing` | `ValidationFailure` | `ExplainError(E3)` | `rejected` | `E3` | 0 | 0 | 0 | 0 | `error` |
| `pk.empty.contradictory_eq` | `Empty` | `ByKeys` | `admitted` | `null` | 461252 | 0 | 0 | 0 | `rows\|PerfAuditUser\|0\|` |
| `pk.empty.eq_and_excluding_in` | `Empty` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.empty.count` | `Empty` | `Empty` | `admitted` | `null` | 316159 | 0 | 0 | 0 | `count\|PerfAuditUser\|0` |
| `pk.empty.require_one` | `Empty` | `Empty` | `not_found` | `E7` | 447991 | 0 | 0 | 0 | `not_found\|PerfAuditUser` |
| `pk.store.heap.existing` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.store.journaled.existing` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.store.heap.deleted` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.store.journaled.deleted` | `ByKey` | `FullScan` | `rejected` | `E190` | 0 | 0 | 0 | 0 | `error` |
| `pk.noncanonical.unique_secondary` | `NotApplied` | `IndexPrefix` | `rejected` | `E188` | 0 | 0 | 0 | 0 | `error` |
| `pk.noncanonical.partial_composite` | `NotApplied` | `Unsupported` | `unsupported_by_fixture` | `null` | 0 | 0 | 0 | 0 | `error` |
| `pk.noncanonical.expression_wrapped` | `NotApplied` | `NotApplied` | `rejected` | `E3` | 0 | 0 | 0 | 0 | `error` |
