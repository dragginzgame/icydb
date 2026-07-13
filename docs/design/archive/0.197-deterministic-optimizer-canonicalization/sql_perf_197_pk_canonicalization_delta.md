# 0.197 Focused Primary-Key Canonicalization Delta

- Scenario rows: 33
- Result-signature changes: 13
- Expected behavior-change scenarios: 24

| Scenario | Canonicalization | Access Before | Access After | Data Gets Before | Data Gets After | Rows Decoded Before | Rows Decoded After | Result Changed |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `pk.scalar.generated.filter.existing.try_one` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.scalar.generated.filter.missing.try_one` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.scalar.generated.by_id.existing.try_one` | `ByKey` | `ByKey` | `ByKey` | 1 | 1 | 1 | 1 | false |
| `pk.scalar.external.filter.existing.try_one` | `ByKey` | `ByKey` | `ByKey` | 0 | 0 | 0 | 0 | false |
| `pk.scalar.external.by_id.existing.try_one` | `ByKey` | `ByKey` | `ByKey` | 0 | 0 | 0 | 0 | false |
| `pk.sql.literal.generated.existing` | `ByKey` | `ByKey` | `ByKey` | 1 | 1 | 1 | 1 | false |
| `pk.sql.literal.generated.commuted` | `ByKey` | `ByKey` | `ByKey` | 1 | 1 | 1 | 1 | false |
| `pk.sql.parameter.unsupported` | `UnsupportedByContract` | `UnsupportedByContract` | `UnsupportedByContract` | 0 | 0 | 0 | 0 | false |
| `pk.sql.literal.generated.wrong_type` | `ValidationFailure` | `ValidationFailure` | `ValidationFailure` | 0 | 0 | 0 | 0 | false |
| `pk.in.fluent.empty` | `Empty` | `ByKeys` | `ByKeys` | 0 | 0 | 0 | 0 | false |
| `pk.in.fluent.one` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.in.fluent.duplicates` | `ByKeys` | `FullScan` | `ByKeys` | 0 | 2 | 0 | 2 | true |
| `pk.in.fluent.multiple_mixed` | `ByKeys` | `FullScan` | `ByKeys` | 0 | 2 | 0 | 2 | true |
| `pk.in.fluent.raw_terms_over_budget` | `ByKeys` | `ByKeys` | `ByKeys` | 0 | 0 | 0 | 0 | false |
| `pk.in.fluent.deduped_over_budget` | `ByKeys` | `FullScan` | `ByKeys` | 0 | 0 | 0 | 0 | false |
| `pk.in.fluent.by_ids.raw_terms_over_budget` | `ByKey` | `ByKey` | `ByKey` | 0 | 0 | 0 | 0 | false |
| `pk.in.sql.duplicates.order_asc` | `ByKeys` | `ByKeys` | `ByKeys` | 2 | 2 | 2 | 2 | false |
| `pk.in.sql.payload_over_budget` | `ByKeys` | `ByKeys` | `ByKeys` | 0 | 0 | 0 | 0 | false |
| `pk.residual.eq.true` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.residual.eq.false` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.residual.eq.invalid_existing` | `ValidationFailure` | `ExplainError(E3)` | `ExplainError(E3)` | 0 | 0 | 0 | 0 | false |
| `pk.residual.eq.invalid_missing` | `ValidationFailure` | `ExplainError(E3)` | `ExplainError(E3)` | 0 | 0 | 0 | 0 | false |
| `pk.empty.contradictory_eq` | `Empty` | `ByKeys` | `ByKeys` | 0 | 0 | 0 | 0 | false |
| `pk.empty.eq_and_excluding_in` | `Empty` | `FullScan` | `ByKeys` | 0 | 0 | 0 | 0 | true |
| `pk.empty.count` | `Empty` | `Empty` | `Empty` | 0 | 0 | 0 | 0 | false |
| `pk.empty.require_one` | `Empty` | `Empty` | `Empty` | 0 | 0 | 0 | 0 | false |
| `pk.store.heap.existing` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.store.journaled.existing` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.store.heap.deleted` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.store.journaled.deleted` | `ByKey` | `FullScan` | `ByKey` | 0 | 1 | 0 | 1 | true |
| `pk.noncanonical.unique_secondary` | `NotApplied` | `IndexPrefix` | `IndexPrefix` | 0 | 0 | 0 | 0 | false |
| `pk.noncanonical.partial_composite` | `NotApplied` | `Unsupported` | `Unsupported` | 0 | 0 | 0 | 0 | false |
| `pk.noncanonical.expression_wrapped` | `NotApplied` | `NotApplied` | `NotApplied` | 0 | 6 | 0 | 6 | true |
