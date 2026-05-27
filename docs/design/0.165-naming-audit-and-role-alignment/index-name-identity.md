# Index Name Identity Naming

## Status

Accepted.

## Family

Index identity construction.

## Problem

`IndexName::try_from_parts` and `IndexName::try_unique_from_parts` used generic
`Parts` vocabulary for a durable identity constructor. The inputs are not a
temporary decomposition; they are the entity identity plus ordered index-field
identity segments used to produce canonical persisted index names.

## Accepted Renames

```text
IndexName::try_from_parts(...) -> IndexName::try_from_entity_fields(...)
IndexName::try_unique_from_parts(...) -> IndexName::try_unique_from_entity_fields(...)
try_from_parts_with_prefix(...) -> try_from_entity_fields_with_prefix(...)
```

## Public Surface Note

`IndexName` is exported from `icydb_core::db`, so this is a pre-1.0 public API
hard cut. No compatibility alias is retained.

## Old-Vocabulary Scan Terms

```text
IndexName::try_from_parts|IndexName::try_unique_from_parts|try_from_parts_with_prefix
```
