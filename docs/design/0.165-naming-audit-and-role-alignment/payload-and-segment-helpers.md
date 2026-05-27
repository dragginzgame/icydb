# Payload And Segment Helper Naming

## Status

Accepted.

## Family

Storage payload and parser segment helpers.

## Problem

Several narrow helpers still used generic `parts` wording even though their
inputs are concrete storage or parser structures:

- persisted-row slot table plus concatenated payload bytes
- commit-marker test row fields
- SQL field-path identifier segments

These are not stable architectural `Parts` payloads or temporary decomposition
objects; the helper names should say which domain inputs they consume.

## Accepted Renames

```text
encode_slot_payload_from_parts(...) -> encode_slot_payload_from_table_and_bytes(...)
encode_test_single_row_payload_from_parts(...) -> encode_test_single_row_payload_from_fields(...)
sql_field_expr_from_parts(...) -> sql_field_expr_from_segments(...)
```

## Kept Names

- `Account::from_parts` and `Ulid::from_parts` remain domain-native primitive
  constructors.
- Historical application fixture fields named `selected_parts` remain test
  schema vocabulary, not IcyDB helper architecture.

## Old-Vocabulary Scan Terms

```text
encode_slot_payload_from_parts|encode_test_single_row_payload_from_parts|sql_field_expr_from_parts
```
