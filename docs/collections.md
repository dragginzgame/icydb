# Collections Contract

## OrderedList vs IdSet
- `OrderedList<T>`: ordered list, duplicates allowed, preserves insertion order, serializes identically to `Vec<T>`.
- `IdSet<E>`: ordered set of `Id<E>`; uniqueness by raw storage key (`E::Key`), order by ascending key; no cascades or ownership semantics.

## Transport vs Domain Semantics
- Cardinality is explicit by container choice:
  - relation `many` fields use set semantics (`IdSet`)
  - non-relation `many` fields use list semantics (`OrderedList`)
- Views are transport: many fields view as `Vec<T::ViewType>`, update views are patch sequences.
- Domain semantics live in collection types and record methods; there is no implicit deduplication, indexing, or cascade behavior.

## Patch Identity Rules
- `ListPatch` identifies elements by index at patch time; patches are applied sequentially.
- `SetPatch` identifies elements by value equality; operations are applied sequentially.
- `MapPatch` identifies entries by key; `Overwrite` replaces the full map.

## Normalization Behavior on Ingest
- `OrderedList` preserves incoming order and duplicates.
- `IdSet` removes duplicate keys and orders by ascending key.
- `from_view` and serde deserialization for `IdSet` perform the same normalization as its constructor.

## Predicate Behavior on Value::List
- `In`/`NotIn` and `Contains` treat lists as collections; order does not affect match results.
- `IsEmpty` on lists checks length only.
- Normalization and fingerprint logic preserve list order for deterministic output.
