# 0.165 Identity Versus Semantics Naming

## Status

In progress.

## Accepted Renames

### `AggregateTerminalSemantics` -> `AggregateTerminalSemanticKey`

Role proof:

- Owning module: `db::sql::lowering::aggregate::semantics`
- Payload: wrapper around the shared filter-aware `AggregateSemanticKey` used
  while deduplicating SQL global aggregate terminals
- Main consumers: global aggregate projection remapping and prepared scalar
  aggregate strategy preparation
- Chosen family: `*Key`
- Rejected alternatives:
  - `*Semantics`: too broad because this value is equality/dedup authority, not
    the prepared aggregate behavior itself
  - `*Identity`: too narrow because the wrapped key includes the optional
    aggregate filter as part of terminal equivalence
  - `*Facts`: too vague and does not communicate equality-key use
- Public-surface impact: none; visibility remains inside SQL aggregate lowering
- Hard-cut rule: remove the old type, imports, local variables, and active-doc
  vocabulary from live code

## Kept Names

### `AggregateIdentity`

Kept because it is the canonical identity of one aggregate terminal excluding
the filter expression. The name matches the `*Identity` policy.

### `AggregateSemanticKey`

Kept because it is explicitly a key, and its `Semantic` qualifier distinguishes
runtime aggregate equivalence from purely syntactic SQL terminal spelling.

### `PreparedAggregateSemantics`

Kept because this enum owns model-bound aggregate behavior after normalization:
aggregate kind, target family, and DISTINCT behavior where DISTINCT changes the
runtime result. That matches the `*Semantics` policy.

### `QueryPlanCacheKey`

Kept because it is the session-level cache identity for shared prepared query
plans. It already uses `*Key` rather than broad semantics wording.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "AggregateTerminalSemantics|AggregateTerminalSemanticKey" crates/icydb-core/src/db/sql/lowering
rg -n "AggregateIdentity|AggregateSemanticKey|PreparedAggregateSemantics|QueryPlanCacheKey" crates/icydb-core/src/db/query crates/icydb-core/src/db/session crates/icydb-core/src/db/sql/lowering
rg -n "semantic|semantics|identity|fingerprint|hash|cache|dedup|canonical" crates/icydb-core/src/db/query crates/icydb-core/src/db/session crates/icydb-core/src/db/executor crates/icydb-core/src/db/sql/lowering
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
