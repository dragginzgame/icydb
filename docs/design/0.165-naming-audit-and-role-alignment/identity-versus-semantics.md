# 0.165 Identity Versus Semantics Naming

## Status

Complete.

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

Companion helper rename:

```text
AggregateIdentity::from_parts(...) -> from_kind_input_and_distinct(...)
```

The constructor now names the three inputs that define aggregate identity
instead of using broad parts vocabulary.

### `AggregateSemanticKey`

Kept because it is explicitly a key, and its `Semantic` qualifier distinguishes
runtime aggregate equivalence from purely syntactic SQL terminal spelling.

Companion helper rename:

```text
AggregateSemanticKey::into_parts() -> into_identity_and_filter()
```

The unpacker now names the identity and optional filter fields that define the
filter-aware aggregate key.

### `PreparedAggregateSemantics`

Kept because this enum owns model-bound aggregate behavior after normalization:
aggregate kind, target family, and DISTINCT behavior where DISTINCT changes the
runtime result. That matches the `*Semantics` policy.

Companion helper renames:

```text
PreparedAggregateSemantics::from_parts(...) -> from_kind_target_and_distinct(...)
PreparedAggregateSemantics::into_executor_parts() -> into_terminal_inputs()
```

The helpers now name the semantic inputs and executor terminal inputs they
consume or produce.

### `QueryPlanCacheKey`

Kept because it is the session-level cache identity for shared prepared query
plans. It already uses `*Key` rather than broad semantics wording.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "AggregateTerminalSemantics|AggregateTerminalSemanticKey" crates/icydb-core/src/db/sql/lowering
rg -n "AggregateIdentity::from_parts|AggregateIdentity::from_kind_input_and_distinct|AggregateSemanticKey::into_parts|AggregateSemanticKey::into_identity_and_filter|PreparedAggregateSemantics::from_parts|PreparedAggregateSemantics::from_kind_target_and_distinct|PreparedAggregateSemantics::into_executor_parts|PreparedAggregateSemantics::into_terminal_inputs" crates/icydb-core/src/db/query crates/icydb-core/src/db/sql/lowering docs/design/0.165-naming-audit-and-role-alignment
rg -n "AggregateIdentity|AggregateSemanticKey|PreparedAggregateSemantics|QueryPlanCacheKey" crates/icydb-core/src/db/query crates/icydb-core/src/db/session crates/icydb-core/src/db/sql/lowering
rg -n "semantic|semantics|identity|fingerprint|hash|cache|dedup|canonical" crates/icydb-core/src/db/query crates/icydb-core/src/db/session crates/icydb-core/src/db/executor crates/icydb-core/src/db/sql/lowering
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
