# Collections Contract

## OrderedList vs UniqueList vs RefSet
- `OrderedList<T>`: ordered list, duplicates allowed, preserves insertion order, serializes identically to `Vec<T>`.
- `UniqueList<T>`: enforces uniqueness by `Eq + Hash`; deterministic order is first-seen insertion; serializes identically to `Vec<T>`.
- `RefSet<E>`: ordered set of `Ref<E>`; uniqueness by raw storage key (`E::Key`), order by ascending key; no cascades or ownership semantics.

## Transport vs Domain Semantics
- Cardinality (`many`) is shape-only; schema and codegen do not imply uniqueness or ordering semantics beyond container choice.
- Views are transport: many fields view as `Vec<T::ViewType>`, update views are patch sequences.
- Domain semantics live in collection types and record methods; there is no implicit deduplication, indexing, or cascade behavior.

## Patch Identity Rules
- `ListPatch` identifies elements by index at patch time; patches are applied sequentially.
- `SetPatch` identifies elements by value equality; operations are applied sequentially.
- `MapPatch` identifies entries by key; `Overwrite` replaces the full map.

## Normalization Behavior on Ingest
- `OrderedList` preserves incoming order and duplicates.
- `UniqueList` removes later duplicates; order is first-seen.
- `RefSet` removes duplicate keys and orders by ascending key.
- `from_view` and serde deserialization for `UniqueList` and `RefSet` perform the same normalization as their constructors.

## Predicate Behavior on Value::List
- `In`/`NotIn` and `Contains` treat lists as collections; order does not affect match results.
- `IsEmpty` on lists checks length only.
- Map-like values are encoded as lists of `[key, value]` pairs; malformed encodings are treated as non-matches.
- Normalization and fingerprint logic preserve list order for deterministic output.

## Renames I Recommend (High Confidence)
- `ListPatch` -> `IndexListPatch` (or `PositionalListPatch`)
- Rationale: list patches are index-addressed, not identity-addressed; the current name is easily misread as semantic.
