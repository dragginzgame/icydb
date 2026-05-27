# Primitive Constructor Input Naming

## Status

Accepted.

## Family

Primitive constructor inputs and narrow value decomposition helpers.

## Problem

Two public primitive constructors still used generic `from_parts` vocabulary even
though their inputs are stable domain fields:

- account owner plus optional subaccount
- ULID timestamp plus randomness payload

Because 0.165 is a pre-1.0 hard cut, these public helpers should not keep
ambiguous aliases. The constructor names should say which primitive inputs they
consume.

## Accepted Renames

```text
Account::from_parts(...) -> Account::from_owner_and_subaccount(...)
Ulid::from_parts(...) -> Ulid::from_timestamp_and_randomness(...)
render_table_row parts -> padded_cells
```

Internal generic local bindings were also renamed where the value had a clearer
domain role, such as SQL path segments, decimal payload fields, and MIME slash
segments.

## Kept Names

- `WrappedUlid::from_parts(...)` remains the upstream `ulid` crate constructor
  invoked inside the IcyDB `Ulid` wrapper boundary.
- `Decimal::parts()` remains domain-native numeric decomposition API. Local
  bindings should name the resulting value by domain, such as `decimal_parts`.
- Historical changelog examples may still map old constructor names to the
  accepted hard-cut names.

## Public Surface Impact

This is a public API hard cut. There are no compatibility aliases for
`Account::from_parts(...)` or `Ulid::from_parts(...)`.

## Old-Vocabulary Scan Terms

```text
Account::from_parts|Ulid::from_parts|WrappedUlid::from_timestamp_and_randomness|encode_decimal_payload_parts
```
