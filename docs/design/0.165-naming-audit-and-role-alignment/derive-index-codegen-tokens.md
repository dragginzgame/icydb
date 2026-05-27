# Derive Index Codegen Tokens

## Status

Accepted for 0.165.

## Finding

Index derive codegen used `runtime_part(...)`, `predicate_runtime_part(...)`,
and `index_parts` vocabulary for generated token fragments. These helpers do
not expose stable architectural parts; they build schema/model token streams
and predicate support items.

Under the 0.165 naming policy, `Parts` is acceptable only for temporary
construction or handoff decomposition. Derive codegen helpers should say when
they produce schema/runtime tokens.

## Accepted Renames

```text
Index::runtime_part(...) -> Index::runtime_model_tokens(...)
Index::predicate_runtime_part(...) -> Index::predicate_runtime_tokens(...)
IndexKeyItemSpec::schema_part(...) -> schema_tokens(...)
IndexKeyItemSpec::runtime_part(...) -> runtime_tokens(...)
IndexExpressionSpec::schema_part(...) -> schema_tokens(...)
IndexExpressionSpec::runtime_part(...) -> runtime_tokens(...)
index_parts -> index_runtime_outputs
```

## Kept Names

- `HasSchemaPart::schema_part(...)` remains the existing derive-wide trait
  method. This slice only renames index-local helpers whose role is token
  generation.
- `schema_key_items_tokens(...)` and `runtime_key_items_tokens(...)` already
  carry precise token-generation names.

## Residual Scan

```text
Index::runtime_part|predicate_runtime_part|IndexKeyItemSpec::schema_part|IndexKeyItemSpec::runtime_part|IndexExpressionSpec::schema_part|IndexExpressionSpec::runtime_part|index_parts
```
