# RFC3339 Timestamp Components

## Status

Accepted for 0.165.

## Finding

The private timestamp parser used `parse_rfc3339_parts(...)` and
`ParsedRfc3339Timestamp` even though the helper returns a concrete set of
calendar, wall-clock, fractional-second, and UTC-offset components.

Under the 0.165 naming policy, `Parts` is acceptable only for temporary
construction or handoff decomposition. The timestamp parser payload is better
named by the parsed RFC3339 components it carries.

## Accepted Renames

```text
parse_rfc3339_parts(...) -> parse_rfc3339_components(...)
ParsedRfc3339Timestamp -> Rfc3339TimestampComponents
```

## Kept Names

- `parse_fractional_nanoseconds(...)` remains role-specific: it parses one
  RFC3339 fractional-second suffix into nanoseconds.
- `parse_rfc3339_offset(...)` remains role-specific: it parses the UTC offset
  suffix.

## Residual Scan

```text
parse_rfc3339_parts|ParsedRfc3339Timestamp|rfc3339 parts
```
