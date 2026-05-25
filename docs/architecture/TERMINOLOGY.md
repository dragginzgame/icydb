# Terminology

IcyDB uses separate vocabularies for schema contracts, runtime values, planner
classification, and storage encodings. Do not collapse these names into one
universal type system.

## Schema Vocabulary

User-facing persisted field contracts use explicit schema labels:

- `int8`, `int16`, `int32`, `int64`, `int128`
- `nat8`, `nat16`, `nat32`, `nat64`, `nat128`
- `int_big(max_bytes=N)`
- `nat_big(max_bytes=N)`

These labels describe persisted schema and storage contracts. `int_big` and
`nat_big` are bounded big-integer contracts; `max_bytes` is part of the schema
contract.

In Rust schema declarations and derive internals, use the matching enum variant
names such as `Primitive::IntBig`, `Primitive::NatBig`, `FieldKind::IntBig`,
and `FieldKind::NatBig`.

## Runtime Value Vocabulary

Runtime algebraic values carry execution payloads:

- `Value::Int`
- `Value::Nat`
- `Value::Int128`
- `Value::Nat128`
- `Value::IntBig`
- `Value::NatBig`

These are runtime carriers, not user-facing schema labels. Comments, docs, and
diagnostics that mention them near schema language should qualify them with
`Value::`.

`types::Int` and `types::Nat` are arbitrary-precision Rust wrappers used by
runtime values. When those wrappers cross a schema or storage boundary for
`int_big` or `nat_big`, the field's `max_bytes` bound applies.

## Planner Vocabulary

Planner and type-inference terms classify expression behavior and coercion
families:

- `ScalarType`
- `SignedNumeric`
- `UnsignedNumeric`
- numeric subtype

These are planner classifications, not persisted schema contracts. They may
intentionally group multiple explicit schema labels when width does not affect
the planner decision being made.

## Storage Vocabulary

Stable-memory and index encodings use storage-specific names:

- `EncodedPrimaryKey`
- `RawDataStoreKey`
- `RawIndexStoreKey`
- `DecodedDataStoreKey`

Use these names when discussing persisted bytes or decoded storage-key
components. `StorageKey` names the older decoded scalar key frame and should not
be used for new schema concepts.

## Layer Names

Keep these abstractions distinct:

- `Primitive`: derive and schema-node primitive vocabulary.
- `FieldKind`: persisted field contract vocabulary.
- `ScalarType`: planner and type-inference grouping vocabulary.

They are not duplicate names for the same concept.
