# Terminology

IcyDB uses separate vocabularies for schema contracts, runtime values, planner
classification, and storage encodings. Do not collapse these names into one
universal type system.

Architectural role names such as `Plan`, `Contract`, `Facts`, `Context`,
`Shape`, `Identity`, and helper verbs are governed by `NAMING.md`. This
document owns domain vocabulary; `NAMING.md` owns role vocabulary.

## Schema Vocabulary

User-facing persisted field contracts use explicit schema labels:

- `int8`, `int16`, `int32`, `int64`, `int128`
- `nat8`, `nat16`, `nat32`, `nat64`, `nat128`
- `u256`

These labels describe fixed-width persisted integer contracts.

Ordinary records, tuples, and newtypes are exact nominal composites. There is
no open-ended persisted field contract.

In Rust schema declarations and derive internals, use the matching enum variant
names such as `Primitive::Int128`, `Primitive::Nat128` and `Primitive::U256`.

## Constraint Vocabulary

Keep these concepts distinct:

- **Database constraint**: accepted-schema-owned primary-key, not-null, unique,
  relation, or check semantics that apply to every supported mutation ingress.
- **Application validator**: generated Rust callback that may reject its typed
  ingress but is not persisted database semantics.
- **Accepted constraint**: validated catalog entry with stable ID, name,
  origin, and one structural or canonical check owner.
- **Durable rule**: bounded authored metadata that becomes one accepted
  root-field-plus-nominal-type constraint. It is proposal input, not a Rust
  runtime validator.
- **Targeted rule**: the accepted durable-rule representation and evaluator
  that visits every finite occurrence of its nominal type below one persisted
  root, including nested and recursive values.
- **Constraint activation**: accepted migration state whose new-write gate is
  authoritative while bounded historical proof is incomplete.
- **Validation job**: durable progress and finding evidence for an activation;
  it is not accepted constraint meaning or planner authority.
- **Candidate generation**: planner-invisible unique-index or reverse-relation
  state built for one activation and made authoritative only by promotion.

Do not use `durable rule`, `validator`, `constraint`, `activation`, and
`validation job` as interchangeable terms.

## Runtime Value Vocabulary

Runtime algebraic values carry execution payloads:

- `Value::Int64`
- `Value::Nat64`
- `Value::Int128`
- `Value::Nat128`
- `Value::U256`

These are runtime carriers, not user-facing schema labels. Comments, docs, and
diagnostics that mention them near schema language should qualify them with
`Value::`.

Runtime integer carriers have fixed numeric ranges.

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
components. Row identity is expressed as `PrimaryKeyComponent`,
`PrimaryKeyValue`, and `EncodedPrimaryKey`; the older decoded scalar
`StorageKey` frame is not a live storage or schema concept.

## Layer Names

Keep these abstractions distinct:

- `Primitive`: derive and schema-node primitive vocabulary.
- `FieldKind`: persisted field contract vocabulary.
- `ScalarType`: planner and type-inference grouping vocabulary.

They are not duplicate names for the same concept.
