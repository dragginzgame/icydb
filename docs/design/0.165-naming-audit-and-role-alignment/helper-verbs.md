# 0.165 Helper Verb Naming

## Status

Complete.

## Accepted Renames

### `resolve_or_insert_global_aggregate_terminal` -> `intern_global_aggregate_terminal`

Role proof:

- Owning module: `db::sql::lowering::aggregate::projection::remap`
- Payload: mutates the global aggregate terminal table by returning the
  existing canonical terminal index or inserting the new terminal
- Main consumers: global aggregate projection remapping and HAVING aggregate
  terminal lowering
- Chosen verb family: `intern_*`
- Rejected alternatives:
  - `resolve_*`: misleading because the helper can insert new table entries
  - `derive_*`: wrong because it mutates a table rather than producing a pure
    fact or output
  - `prepare_*`: wrong because it does not freeze reusable state for a later
    phase
- Public-surface impact: none; helper is SQL-lowering internal
- Hard-cut rule: remove old helper and callsite vocabulary from live code

### `resolve_having_global_aggregate_terminal_index` -> `intern_having_global_aggregate_terminal_index`

Role proof:

- Owning module: `db::sql::lowering::aggregate::projection::remap`
- Payload: HAVING-specific wrapper over terminal interning
- Main consumers: global aggregate command lowering
- Chosen verb family: `intern_*`
- Rejected alternatives:
  - `resolve_*`: still misleading because HAVING aggregate references can
    extend the terminal table
  - `collect_*`: too broad because this returns one interned terminal index
- Public-surface impact: none; visibility remains inside SQL aggregate lowering
- Hard-cut rule: remove old export and import vocabulary from live code

### `QueryPlanCacheKey::from_authority_parts` -> `from_authority_cache_inputs`

Role proof:

- Owning module: `db::session::query::cache`
- Payload: private constructor for the canonical query-plan cache-key shell from
  entity authority, schema fingerprint, visibility, structural query key, and
  cache method version
- Main consumers: test constructors and normalized-predicate cache-key
  construction
- Chosen family: input vocabulary for a cache-key constructor
- Rejected alternatives:
  - `from_authority_parts`: too weak because the helper assembles cache
    identity inputs, not arbitrary authority parts
  - `from_authority_context`: wrong because it does not accept an owner-local
    context object
  - `from_authority_payload`: wrong because no single payload crosses a
    boundary
- Public-surface impact: none
- Hard-cut rule: remove the old private helper name from live code

## Kept Names

### `resolve_projection_order_alias` / `resolve_projection_having_alias`

Kept because these helpers map an alias reference to an existing projected
expression. They do not mutate the projection list, so `resolve_*` is accurate.

### `derive_normalized_bool_expr_predicate_subset`

Kept because it computes a new predicate subset from one normalized boolean
expression and may return `None`. `derive_*` is accurate for this fact/output
construction.

### `prepare_sql_statement`

Kept because the helper freezes one parsed SQL statement into the prepared
statement form used by later lowering phases.

### `canonicalize_sql_filter_expr_for_schema`

Kept because the helper normalizes SQL filter literals into accepted-schema
canonical value forms while preserving equivalent predicate meaning.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "resolve_or_insert_global_aggregate_terminal|resolve_having_global_aggregate_terminal_index" crates/icydb-core/src docs/design/0.165-naming-audit-and-role-alignment
rg -n "intern_global_aggregate_terminal|intern_having_global_aggregate_terminal_index" crates/icydb-core/src/db/sql/lowering
rg -n "from_authority_parts|from_authority_cache_inputs" crates/icydb-core/src/db/session/query/cache.rs docs/design/0.165-naming-audit-and-role-alignment
rg -n "\\b(classify|analyze|derive|resolve|prepare|canonicalize)_[a-zA-Z0-9_]+" crates/icydb-core/src/db/query crates/icydb-core/src/db/session crates/icydb-core/src/db/executor crates/icydb-core/src/db/sql/lowering
```

Remaining old-name hits are allowed only inside this family note as accepted
rename history and scan terms.
