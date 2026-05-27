# Persisted-Row Structural Slot Reader Naming

## Status

Accepted.

## Family

Persisted-row slot-reader boundary.

## Problem

The persisted-row reader used a private `core` module for the concrete
`StructuralSlotReader` implementation. The module does not own a generic row
reader core or invariant payload shared by wrappers. It adapts persisted row
bytes to the canonical slot-reader seam, validates the row envelope, and lazily
decodes slots.

Under the 0.165 naming policy, `Core` is kept only for genuine invariant
payloads. This owner is better named after the slot-reader role it performs.

## Accepted Renames

```text
persisted_row::reader::core -> persisted_row::reader::structural_slot_reader
reader::core::StructuralSlotReader -> reader::structural_slot_reader::StructuralSlotReader
mod core; -> mod structural_slot_reader;
```

## Kept Names

- `StructuralSlotReader` remains accurate because the type is the concrete
  structural row slot-reader adapter.
- `CanonicalSlotReader` remains the trait vocabulary for callers that need the
  canonical slot-reading contract rather than the concrete persisted-row
  adapter.
- Direct reader helpers remain in `reader::direct` because they intentionally
  avoid constructing the lazy slot-reader adapter.

## Old-Vocabulary Scan Terms

```text
persisted_row::reader::core|reader::core::StructuralSlotReader|core::StructuralSlotReader
```
