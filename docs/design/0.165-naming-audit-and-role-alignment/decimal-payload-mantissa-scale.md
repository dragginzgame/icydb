# Decimal Payload Mantissa/Scale Naming

## Status

Accepted.

## Family

Structural decimal payload helpers.

## Problem

The structural field decoder used `decode_decimal_payload_parts(...)` even
though the helper takes the exact decimal payload fields: mantissa and scale.
That name was not the public domain-native `Decimal::parts()` API; it was an
internal structural payload validation and normalization helper.

Under the 0.165 naming policy, generic `parts` vocabulary should not hide
payload roles when the inputs are already known and stable.

## Accepted Renames

```text
decode_decimal_payload_parts(...) -> decode_decimal_payload_mantissa_and_scale(...)
decimal payload parts call sites -> mantissa/scale call sites
structural value-storage decimal decode -> mantissa/scale helper
```

## Kept Names

- `DecimalParts` and `Decimal::parts()` remain domain-native public decimal
  decomposition vocabulary.
- Local bindings named `decimal_parts` remain acceptable when they hold a
  `DecimalParts` value.
- `WrappedUlid::from_parts(...)` remains the upstream `ulid` crate constructor,
  not an IcyDB-owned API.

## Old-Vocabulary Scan Terms

```text
decode_decimal_payload_parts|decimal payload parts
```
