# Ordered Index-Key Segment Naming

## Status

Accepted.

## Family

Ordered index-key component encoders.

## Problem

The ordered index-key encoder used a private `parts` module for helpers that
emit lexicographic byte segments inside one encoded index component. These
helpers do not expose a temporary decomposition object or stable architectural
`Parts` payload. They append concrete encoded byte segments.

The name also sat next to `encode_segment_len`, which already used the more
precise vocabulary for the bounded byte fragments this module owns.

## Accepted Renames

```text
index::key::ordered::parts -> index::key::ordered::segments
parts::push_terminated_bytes(...) -> segments::push_terminated_bytes(...)
parts::encode_segment_len(...) -> segments::encode_segment_len(...)
```

## Kept Names

- `Decimal::parts()` remains domain-native numeric API. Local users should name
  the binding by domain, such as `decimal_parts`, instead of treating the
  ordered-key encoder module as a `parts` owner.
- `Account::from_parts` and `Ulid::from_parts` remain domain-native primitive
  constructors, as recorded in the payload/segment helper note.

## Old-Vocabulary Scan Terms

```text
index::key::ordered::parts|mod parts;|parts::push_|parts::encode_segment_len
```
