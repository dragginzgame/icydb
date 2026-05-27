# Aggregate Value Reducer Naming

## Status

Accepted.

## Family

Aggregate value reducer state and finalization helpers.

## Problem

The aggregate reducer module used `reducer_core` even though it does not own a
generic aggregate execution core. It owns value aggregate reducer state and
helpers for COUNT(value), SUM, AVG, MIN, and MAX transitions shared by scalar,
grouped, and global aggregate paths.

Under the 0.165 naming policy, `Core` is kept only for a genuine invariant
payload shared by wrappers. This module is narrower: it is a reducer boundary
for value aggregate semantics.

## Accepted Renames

```text
executor::aggregate::reducer_core -> executor::aggregate::value_reducer
reducer_core::ValueReducerState -> value_reducer::ValueReducerState
reducer_core::finalize_count(...) -> value_reducer::finalize_count(...)
```

## Kept Names

- `ValueReducerState` remains accurate because the type owns mutable reducer
  payloads for one aggregate terminal.
- `finalize_count(...)` remains accurate because the helper converts one
  already-counted value into the canonical aggregate output value.
- Executor aggregate test modules may keep broader test-suite labels when they
  cover full execution behavior rather than only reducer state.

## Old-Vocabulary Scan Terms

```text
reducer_core|executor::aggregate::reducer_core|aggregate-core|reducer-core
```
