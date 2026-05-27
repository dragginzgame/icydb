# Fingerprint Hash Sections Naming

## Status

Accepted.

## Family

Query fingerprint and continuation-signature hashing.

## Problem

The fingerprint module used `hash_parts` for the owner of canonical plan hash
field/tag encoding. The module does not own arbitrary decomposition parts; it
owns ordered hash sections for plan fingerprints and continuation signatures.

## Accepted Rename

```text
query::fingerprint::hash_parts -> query::fingerprint::hash_sections
hash_parts::ExplainHashProfile -> hash_sections::ExplainHashProfile
hash_parts::hash_explain_plan_profile(...) -> hash_sections::hash_explain_plan_profile(...)
```

## Kept Names

- `ExplainHashProfile`, `ExplainHashField`, and `ExplainHashStep` remain because
  they describe the selected hash profile and its ordered section walk.
- `ProjectedOrderShape` remains because it is a compact normalized ordering
  family shared by explain and planner hashing.

## Old-Vocabulary Scan Terms

```text
query::fingerprint::hash_parts|mod hash_parts|hash_parts::
```
