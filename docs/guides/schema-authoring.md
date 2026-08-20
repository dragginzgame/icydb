# Schema Authoring

This guide covers the choices that most often cause a schema to fail during
host generation: primary-key types, field names, relations, and the boundary
between a shared model crate and a generated canister. The model compiler and
canonical scalar metadata remain the authority when this guide and code ever
disagree.

## Primary Keys

Declare one required field with `pk(field = "id")`, or two to four distinct
required fields in storage order with `pk(fields = ["tenant_id", "local_id"])`.
Every component has cardinality `One` and uses a primary-key-compatible
primitive. Composite keys are caller-authored; `Unit` is valid only as a sole
scalar key, not as a composite component.

The current authoring primitives accepted as primary-key components are:

<!-- icydb-primary-key-primitives:start -->
`Account`, `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Nat8`, `Nat16`,
`Nat32`, `Nat64`, `Nat128`, `Principal`, `Subaccount`, `Timestamp`, `Ulid`, and
`Unit`.
<!-- icydb-primary-key-primitives:end -->

This list is derived from the canonical scalar registry's
`is_primary_key_component_encodable` metadata. Compatibility means that the
value has a canonical storage-key encoding; it does not make every compatible
primitive a good domain identity.

`Text` is deliberately not a primary-key domain. For a human-readable slug,
keep a durable generated identity such as `Ulid` or `Identity::next` as the
primary key and put the slug in an ordinary indexed field. The slug can change
without changing row identity or every referring relation.

`Principal` is compatible when the external principal really is the durable
identity and its lifecycle matches the row. When it is only an owner, login,
or integration handle, use a generated primary key and an indexed
`owner_principal` field instead. An identifier never proves authorization;
see the [identity and primary-key contract](../contracts/IDENTITY_CONTRACT.md).

## Field And Relation Names

Authored fields use valid Rust `snake_case` identifiers. Required and optional
single relations end in `_id`; collection relations end in `_ids`. The suffix
describes relation shape only and grants no authority.

The model compiler rejects the following union of Rust, Candid, and IcyDB
reserved field identifiers:

<!-- icydb-reserved-field-identifiers:start -->
- Candid: `blob`, `bool`, `composite_query`, `empty`, `float32`, `float64`,
  `func`, `import`, `int`, `int8`, `int16`, `int32`, `int64`, `nat`, `nat8`,
  `nat16`, `nat32`, `nat64`, `null`, `oneway`, `opt`, `principal`, `query`,
  `record`, `reserved`, `service`, `text`, `type`, `variant`, and `vec`.
- IcyDB scalar labels: `int128`, `int_big`, `nat128`, and `nat_big`.
- Rust: `as`, `break`, `const`, `continue`, `crate`, `else`, `enum`, `extern`,
  `false`, `fn`, `for`, `gen`, `if`, `impl`, `in`, `let`, `loop`, `match`,
  `mod`, `move`, `mut`, `pub`, `ref`, `return`, `self`, `Self`, `static`,
  `struct`, `super`, `trait`, `true`, `type`, `unsafe`, `use`, `where`,
  `while`, `async`, `await`, `dyn`, `abstract`, `become`, `box`, `do`,
  `final`, `macro`, `override`, `priv`, `typeof`, `unsized`, `virtual`,
  `yield`, and `try`.
<!-- icydb-reserved-field-identifiers:end -->

Entity, store, canister, and named-type names come from their Rust declarations.
Fields, variants, relations, constraints, and rules use their explicit authored
names; indexes derive their canonical names. Do not create a second name map in
application code.

## Host Build And Runtime Model Boundary

Cargo compiles `build.rs` for the host, even when the library target is Wasm.
The build script loads the model declarations so IcyDB can validate the whole
graph and generate the canister actor. The canister library also depends on
that same schema/model library for its generated Rust types and adapters.
Therefore the schema/model library belongs in both `[dependencies]` and
`[build-dependencies]` of the canister package; use the same IcyDB release and
feature contract on both sides.

A small single-canister workspace normally has this shape:

```text
crates/app-schema/       # canister, store, entity, and named-type declarations
canisters/app/build.rs   # depends on app-schema and calls build_canister!
canisters/app/src/lib.rs # depends on app-schema and runs the generated actor
```

For several canisters, keep declarations shared only where the Rust domain is
actually shared:

```text
crates/domain-model/     # reusable named values, if any
crates/miner-schema/     # miner canister/store/entities
crates/admin-schema/     # admin canister/store/entities
canisters/miner/         # host and Wasm depend on miner-schema
canisters/admin/         # host and Wasm depend on admin-schema
```

Each canister still owns its own IcyDB database and accepted schema. A shared
Rust library does not create cross-canister relations, storage, atomicity, or a
shared database. Put workflow coordination in application code only when the
application has an explicit protocol for it.

Host declarations produce schema proposals and generated adapters. After
acceptance, the accepted schema snapshot is the sole runtime authority for row
layout, constraints, indexes, planning, writes, and recovery. Generated
`EntityModel` and `IndexModel` values may support proposals, reconciliation,
model-only convenience, and tests; the runtime never reconstructs accepted
schema from them.

For a complete starting declaration, see the
[workspace schema example](../../README.md#minimal-schema). For applying an
intentional schema change, see [Schema Migrations](schema-migrations.md).
