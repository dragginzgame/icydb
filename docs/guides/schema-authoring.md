# Schema Authoring

This guide covers the choices that most often cause a schema to fail during
host generation: primary-key types, field names, relations, and the boundary
between a shared model crate and a generated canister. The model compiler and
canonical scalar metadata remain the authority when this guide and code ever
disagree.

Runtime-enabled crates normally use `icydb::model`; this keeps model authoring
and database runtime on one direct `icydb` dependency. Schema-only tooling may
instead depend directly on `icydb-model` without pulling in the runtime.

## Primary Keys

Declare one required field with `pk(field = "id")`, or two to four distinct
required fields in storage order with `pk(fields = ["tenant_id", "local_id"])`.
Every component has cardinality `One` and uses a primary-key-compatible
primitive. Composite keys are caller-authored; `Unit` is valid only as a sole
scalar key, not as a composite component.

The current authoring primitives accepted as primary-key components are:

<!-- icydb-primary-key-primitives:start -->
`Account`, `Int8`, `Int16`, `Int32`, `Int64`, `Int128`, `Nat8`, `Nat16`,
`Nat32`, `Nat64`, `Nat128`, `Principal`, `Subaccount`, `Timestamp`, `U256`,
`Ulid`, and `Unit`.
<!-- icydb-primary-key-primitives:end -->

This list is derived from the canonical scalar registry's
`is_primary_key_component_encodable` metadata. Compatibility means that the
value has a canonical storage-key encoding; it does not make every compatible
primitive a good domain identity.

`U256` is an inline, fixed-width unsigned domain for values from zero through
`2^256-1`. Its public Candid carrier is `nat`, so generated JavaScript and
TypeScript clients use `bigint`; IcyDB rejects an admitted value above the
fixed-width maximum during ingress. Persistence and index keys use IcyDB-owned
32-byte encodings rather than Candid's variable-length wire representation.
Database arithmetic is checked, not Ethereum-style wrapping arithmetic.

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

## Nested Strong Relations

Place `rel` on the scalar leaf whose target must exist for the entire lifetime
of every source state containing it. IcyDB instantiates that annotation once
for each accepted entity root that reaches the reusable value:

```rust
#[record(fields(field(
    name = "asset_id",
    value(item(prim = "Ulid", rel = "Asset"))
)))]
pub struct StorefrontImage {}

#[entity(
    store = "DataStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "images", value(many, item(is = "StorefrontImage")))
    )
)]
pub struct Storefront {}
```

Required and optional named records, enum-variant payloads, list items, set
items, map values, and bounded combinations are supported source paths. Map
keys, tuple positions, opaque payloads, recursive cycles, and nested assembly
of a composite target key are rejected.

Nested means only that the source key is embedded. It is still the ordinary
strong relation contract: writes require every target in the final atomic
image, and a surviving source blocks target deletion. Do not use `rel` for
historical IDs, conditional references, external or cross-canister IDs, or
values allowed to dangle. Nested relations add no member query, join,
multikey-index, or partial-update surface.

## Generated Source References

When the declaring crate depends directly on the `icydb` runtime facade,
entity fields and record members receive public upper-snake `FieldRef`
constants. Entities also implement `EntitySource`; use its fully qualified
constant as the collision-safe authored source name:

```rust
let query = DynamicQuery::new(<User as icydb::traits::EntitySource>::ENTITY)
    .filter(User::ACTIVE.eq(true))
    .order_by(asc(User::ID));

let patch = StructuralPatch::new().field(
    User::DISPLAY_NAME.as_str(),
    WriteCell::Value(InputValue::text("Ada".to_string())),
);
```

If `EntitySource` is in scope and the entity has no field named `entity`,
`User::ENTITY` is available as shorthand. An authored field named `entity`
remains valid and generates the inherent `User::ENTITY: FieldRef`; use the
fully qualified trait constant whenever the entity source is required.

The shorthand requires `EntitySource` in every module that uses it. A parent
module's import is not inherited by a nested module, including a nested test
module. Use either the common prelude or a narrow anonymous trait import in
each such module:

```rust
use icydb::prelude::*;
// or, with narrow imports:
use icydb::traits::EntitySource as _;
```

The constants remove a hand-maintained spelling map; they do not prove that a
field or entity is accepted. Every query and write still binds the source names
against one current accepted-schema snapshot. Schema-only consumers continue
to depend on `icydb-model` without receiving runtime-owned `FieldRef` values.

## Bounded Owner-Local Collections

Use a named list when a small owner-local sequence has a durable item-count
ceiling and is normally read and replaced with its containing row:

```rust
use icydb::model::prelude::*;

#[record(fields(
    field(name = "slot", value(item(prim = "Nat16"))),
    field(name = "quantity", value(item(prim = "Nat64")))
))]
pub struct InventoryStack {}

#[list(
    item(is = "InventoryStack"),
    ty(rule(
        name = "capacity",
        length_range_inclusive(min = 0, max = 51)
    ))
)]
pub struct InventoryStacks {}
```

The accepted `length_range_inclusive` rule is the database safety ceiling.
Anonymous `value(many, ...)` has no inline application capacity; general row,
request, and decode limits still apply. Lower dynamic limits, slot uniqueness,
and cross-entity existence remain application rules unless modeled through an
existing accepted constraint.

At a structural write boundary, pass `InventoryStacks(stacks)` (or an
anonymous `Vec<InventoryStack>`) to `DbSession::bind_typed_input` with the
owning entity's current typed binding. The generated adapter resolves record
member names through accepted source identities and returns one ordinary
`InputValue`; applications do not need to reconstruct each stack as a named
map. Complete-field accepted admission still enforces the named list rule.

If collection members need independent queries, indexes, reverse traversal, or
mutation, model them as entities with an owner key. Lists, sets, and maps stored
in a row remain whole-field aggregates even when an explicit nested scalar
relation enforces target lifetime. The exact decision matrix is maintained in
the [nested storage contract](../contracts/NESTED_STORAGE.md).

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
