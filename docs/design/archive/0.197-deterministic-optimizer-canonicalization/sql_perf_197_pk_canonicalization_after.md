# 0.197 Focused Primary-Key Canonicalization Current Capture

- Scenario rows: 33
- Counter-measured rows: 21
- Admitted counter-measured rows: 20
- Contract/not-measured rows: 5
- Non-admitted/fail-closed rows: 13

| Scenario | Canonicalization | Access | Admission | Error | Instructions | data_store.get | Rows Decoded | Rows Returned | Result |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `pk.scalar.generated.filter.existing.try_one` | `ByKey` | `ByKey` | `admitted` | `null` | 503356 | 1 | 1 | 1 | `rows\|PerfAuditUser\|1\|Id(1)` |
| `pk.scalar.generated.filter.missing.try_one` | `ByKey` | `ByKey` | `admitted` | `null` | 317712 | 1 | 1 | 0 | `rows\|PerfAuditHeapUser\|0\|` |
| `pk.scalar.generated.by_id.existing.try_one` | `ByKey` | `ByKey` | `admitted` | `null` | 275918 | 1 | 1 | 1 | `rows\|PerfAuditUser\|1\|Id(1)` |
| `pk.scalar.external.filter.existing.try_one` | `ByKey` | `ByKey` | `contract_backed_not_measured` | `null` | 0 | 0 | 0 | 1 | `contract_backed_not_measured` |
| `pk.scalar.external.by_id.existing.try_one` | `ByKey` | `ByKey` | `contract_backed_not_measured` | `null` | 0 | 0 | 0 | 1 | `contract_backed_not_measured` |
| `pk.sql.literal.generated.existing` | `ByKey` | `ByKey` | `admitted` | `null` | 835755 | 1 | 1 | 1 | `projection\|PerfAuditUser\|1\|1,Alice` |
| `pk.sql.literal.generated.commuted` | `ByKey` | `ByKey` | `admitted` | `null` | 657721 | 1 | 1 | 1 | `projection\|PerfAuditUser\|1\|1,Alice` |
| `pk.sql.parameter.unsupported` | `UnsupportedByContract` | `UnsupportedByContract` | `rejected` | `E181` | 0 | 0 | 0 | 0 | `error` |
| `pk.sql.literal.generated.wrong_type` | `ValidationFailure` | `ValidationFailure` | `rejected` | `E3` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.empty` | `Empty` | `ByKeys` | `admitted` | `null` | 448971 | 0 | 0 | 0 | `rows\|PerfAuditUser\|0\|` |
| `pk.in.fluent.one` | `ByKey` | `ByKey` | `admitted` | `null` | 506550 | 1 | 1 | 1 | `rows\|PerfAuditUser\|1\|Id(1)` |
| `pk.in.fluent.duplicates` | `ByKeys` | `ByKeys` | `admitted` | `null` | 561476 | 2 | 2 | 2 | `rows\|PerfAuditUser\|2\|Id(1),Id(2)` |
| `pk.in.fluent.multiple_mixed` | `ByKeys` | `ByKeys` | `admitted` | `null` | 535572 | 2 | 2 | 1 | `rows\|PerfAuditUser\|1\|Id(1)` |
| `pk.in.fluent.raw_terms_over_budget` | `ByKeys` | `ByKeys` | `public_policy_rejected_not_measured` | `E204` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.deduped_over_budget` | `ByKeys` | `ByKeys` | `rejected` | `E203` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.fluent.by_ids.raw_terms_over_budget` | `ByKey` | `ByKey` | `public_policy_rejected_not_measured` | `E204` | 0 | 0 | 0 | 0 | `error` |
| `pk.in.sql.duplicates.order_asc` | `ByKeys` | `ByKeys` | `admitted` | `null` | 3759881 | 2 | 2 | 2 | `projection\|PerfAuditUser\|2\|1;2` |
| `pk.in.sql.payload_over_budget` | `ByKeys` | `ByKeys` | `public_policy_rejected_not_measured` | `E204` | 0 | 0 | 0 | 0 | `error` |
| `pk.residual.eq.true` | `ByKey` | `ByKey` | `admitted` | `null` | 567727 | 1 | 1 | 1 | `rows\|PerfAuditUser\|1\|Id(1)` |
| `pk.residual.eq.false` | `ByKey` | `ByKey` | `admitted` | `null` | 535759 | 1 | 1 | 0 | `rows\|PerfAuditUser\|0\|` |
| `pk.residual.eq.invalid_existing` | `ValidationFailure` | `ExplainError(E3)` | `rejected` | `E3` | 0 | 0 | 0 | 0 | `error` |
| `pk.residual.eq.invalid_missing` | `ValidationFailure` | `ExplainError(E3)` | `rejected` | `E3` | 0 | 0 | 0 | 0 | `error` |
| `pk.empty.contradictory_eq` | `Empty` | `ByKeys` | `admitted` | `null` | 462006 | 0 | 0 | 0 | `rows\|PerfAuditUser\|0\|` |
| `pk.empty.eq_and_excluding_in` | `Empty` | `ByKeys` | `admitted` | `null` | 519784 | 0 | 0 | 0 | `rows\|PerfAuditUser\|0\|` |
| `pk.empty.count` | `Empty` | `Empty` | `admitted` | `null` | 317364 | 0 | 0 | 0 | `count\|PerfAuditUser\|0` |
| `pk.empty.require_one` | `Empty` | `Empty` | `not_found` | `E7` | 449001 | 0 | 0 | 0 | `not_found\|PerfAuditUser` |
| `pk.store.heap.existing` | `ByKey` | `ByKey` | `admitted` | `null` | 328824 | 1 | 1 | 1 | `rows\|PerfAuditHeapUser\|1\|Id(1)` |
| `pk.store.journaled.existing` | `ByKey` | `ByKey` | `admitted` | `null` | 345669 | 1 | 1 | 1 | `rows\|PerfAuditJournaledUser\|1\|Id(1)` |
| `pk.store.heap.deleted` | `ByKey` | `ByKey` | `admitted` | `null` | 308451 | 1 | 1 | 0 | `rows\|PerfAuditHeapUser\|0\|` |
| `pk.store.journaled.deleted` | `ByKey` | `ByKey` | `admitted` | `null` | 324471 | 1 | 1 | 0 | `rows\|PerfAuditJournaledUser\|0\|` |
| `pk.noncanonical.unique_secondary` | `NotApplied` | `IndexPrefix` | `rejected` | `E188` | 0 | 0 | 0 | 0 | `error` |
| `pk.noncanonical.partial_composite` | `NotApplied` | `Unsupported` | `unsupported_by_fixture` | `null` | 0 | 0 | 0 | 0 | `error` |
| `pk.noncanonical.expression_wrapped` | `NotApplied` | `NotApplied` | `admitted` | `null` | 1060060 | 6 | 6 | 1 | `projection\|PerfAuditUser\|1\|1` |
