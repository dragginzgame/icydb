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

`U256` is an inline, fixed-width unsigned domain from zero through `2^256-1`.
Its native Wire projection is an exact 32-byte big-endian byte string. Human-readable
Serde, including JSON, uses decimal strings. Negative and oversized values reject.
Database arithmetic is checked. Row, index and cursor encodings retain their
separate database-owned contracts.

`Ulid` crosses the Wire boundary as exactly 16 bytes in `Ulid::to_bytes()` order.
Human-readable Serde, display and SQL literals use canonical 26-character text.
Binary Serde emits a byte string and validates its exact width before copying.

`Date` uses signed 32-bit epoch days in Wire/binary Serde and ISO `YYYY-MM-DD`
text in JSON. Its calendar domain remains years 0000–9999. `Subaccount` uses an
exact 32-byte byte string in Wire/binary Serde and a 32-element byte array in JSON.
Account database payloads still reserve their existing fixed 62-byte width.
`Account` owns its bounded ICRC-1 text representation without a ledger dependency.
Applications that need external ledger types construct them from `owner()` and
`subaccount()`; IcyDB provides no upstream ledger conversions.

`Decimal` uses decimal text in Wire and Serde, including CBOR. `Int128` and
`Nat128` retain fixed-width numeric domains and use Postcard's signed/unsigned
integer representations. These atoms do not require an arbitrary-precision
integer adapter in IcyDB.

Generated endpoints use IC Wire method/direction envelopes and bounded native
Postcard projections. Strings and collections are admitted by complete message
bytes, allocation, node count and depth. Native callers share the public Rust
payloads and `icydb::wire::Endpoint`; opaque native projections currently have no
automatically generated TypeScript schema. Changing a payload layout requires a
coordinated method-identity change and matching caller updates.

Regenerate external transport and binary-Serde artifacts when adopting this cut.
Binary Serde for Blob is a byte string and U256 has one exact-width byte shape.
There is no previous-format reader. This endpoint change does not modify custom
row/index/cursor encodings; the pending integer-tag hard cut has its own database
recreation obligations.

Direct row fields declared `Int8`/`Nat8`, `Int16`/`Nat16`, and `Int32`/`Nat32`
store 1, 2, and 4 little-endian payload bytes respectively, plus the two-byte
scalar slot envelope. Null slots use only that envelope. The accepted schema
selects the width, including direct relations with narrow integer keys; writes
reject out-of-range values. Adopting this format requires affected databases
and persisted row/default/catalog data to be recreated. Index keys, decoded
runtime integers, and nested structural values retain their own representations.

Nested fixed-width scalars store one type tag followed by the fixed payload,
without an inner tag or length prefix. Date uses four epoch-day bytes. Account
retains all 62 payload bytes, with or without a subaccount. Decimal stores its
full 16-byte signed mantissa and one scale byte (0–28), preserving both parts;
ordinary structural Decimal wraps these same 17 bytes in a generic byte frame.
Adopting these layouts requires affected data/default/snapshot artifacts to be
recreated. Invalid persisted Decimal scales are rejected rather than normalized.

`Text` is deliberately not a primary-key domain. For a human-readable slug,
keep a durable generated identity such as `Ulid` or `Identity::next` as the
primary key and put the slug in an ordinary indexed field. The slug can change
without changing row identity or every referring relation.

`Principal` is compatible when the external principal really is the durable
identity and its lifecycle matches the row. When it is only an owner, login,
or integration handle, use a generated primary key and an indexed
`owner_principal` field instead. An identifier never proves authorization;
see the [identity and primary-key contract](../contracts/IDENTITY_CONTRACT.md).

## Literal Diagnostics

Quoted Account, Principal, Subaccount and Ulid field defaults are parsed during
macro expansion. Invalid identity text returns a compiler error at the field;
requesting Rust `Default` does not postpone that check or turn it into a panic.
Subaccount literals require 64 ASCII hex characters, optionally prefixed by
lowercase `0x`. The schema scalar owns parsing for both generated defaults and
sealed fragments. Valid identity defaults
construct their canonical bytes without parsing text at runtime.

Generated CHECK and filtered-index numeric literals use the same parser for
validation and source-program lowering. Fractional or negative unsigned integer
literals, integers outside the source literal domain, and nonfinite float or
unrepresentable decimal literals produce compiler errors. Accepted field
contracts remain responsible for their narrower value bounds.

## Field And Relation Names

Authored fields use valid Rust `snake_case` identifiers. Required and optional
single relations end in `_id`; collection relations end in `_ids`. The suffix
describes relation shape only and grants no authority.

The model compiler rejects the following union of Rust and IcyDB
reserved field identifiers:

<!-- icydb-reserved-field-identifiers:start -->
- IcyDB scalar labels: `int128` and `nat128`.
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

## Serializing Generated Values Into Application Blobs

Generated application values implement both `Serialize` and `Deserialize`
automatically; every nested generated value supports the same neutral traits.

```rust
#[record(
    fields(field(name = "rank", value(item(prim = "Nat64"))))
)]
pub struct Profile {}

// Ordinary collection serialization then composes with the generated record.
let bytes = serde_json::to_vec(&vec![Profile { rank: 7 }])?;
let profiles: Vec<Profile> = serde_json::from_slice(&bytes)?;
```

Do not add `Serialize` explicitly through a trait directive or an outer derive.
The model macro owns the generated fields and emits the required serializer.
The compiler emits the serializer through its existing trait-generation path,
including when the IcyDB dependency is renamed. No handwritten field inventory
or direct dependency on IcyDB implementation crates is needed.

The chosen blob codec and its input-size/decode limits belong to the application.
Serde serialization does not validate accepted database constraints, make the
blob's members queryable, or change IcyDB's persisted row encoding. Use the
ordinary typed input adapter when storing structured fields instead of blobs.

## Host Build And Runtime Model Boundary

Cargo compiles `build.rs` for the host, even when the library target is Wasm.
The build script loads the model declarations so IcyDB can validate the whole
graph and generate the canister actor. The runtime compiles those same
declarations for its generated Rust types and adapters. Use the same IcyDB
release and feature contract on both sides.

A single canister can keep its declarations in one package:

```text
canisters/app/Cargo.toml        # [package] build = "src/build.rs"
canisters/app/src/design/mod.rs # canister, store, entity and named-type declarations
canisters/app/src/build.rs      # mod design; main calls build_canister!(design::AppCanister)
canisters/app/src/lib.rs        # mod design; canic_icydb::start!(icydb); application endpoints
```

Put `icydb` in both `[dependencies]` and `[build-dependencies]`. The build
script's `main` returns `Result<(), Box<dyn std::error::Error>>`, prints
`cargo:rerun-if-changed=src/design`, and returns the result of
`icydb::build::build_canister!(design::AppCanister)`. Cargo's explicit build
script path lets both targets use ordinary `mod design;` module discovery.
Keep that module limited to declarations available to both targets, not runtime
endpoint code. Normal canister endpoint dependencies still belong to the runtime.

`build_canister!` resolves the supplied type through its canonical model `Path`,
not its spelling at the call site. Relative paths and imported type names work
without rewriting host crate names or manufacturing aliases. The maintained
[single-package fixture](../../testing/model-facade-only) covers a renamed
IcyDB dependency, generated lifecycle, typed writes/reads and same-code upgrade.

A separate schema library is also supported when sharing it is useful. Put
that library in both dependency sections of each consuming canister:

```text
crates/app-schema/       # canister, store, entity, and named-type declarations
canisters/app/build.rs   # depends on app-schema and calls build_canister!
canisters/app/src/lib.rs # depends on app-schema and runs the generated actor
```

For several canisters, share declarations only where the Rust domain is
actually shared; independent canisters can each use the single-package layout.
A workspace with genuinely shared model libraries might instead use:

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

### Permanent memory identities

Give each database a permanent `memory_namespace` and each journaled store an
explicit key. Namespaces and store keys start with a lowercase ASCII letter,
followed by lowercase letters, digits or underscores. Complete generated role
keys must fit the 128-byte stable-key limit.

```rust,ignore
#[canister(memory_namespace = "app")]
pub struct AppCanister;

#[store(canister = "AppCanister", storage(journaled(key = "transfers")))]
pub struct TransferStore;

// In the canister root, not the schema crate:
fn icydb_memory_pool() -> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new("icydb.app", "icydb.app.")?],
        vec![], // Explicitly exclude physical IDs held by unmanaged host users.
    )
}
```

The standalone host `icydb_memory_pool` provider grants disjoint permanent key
namespaces to their named owners in one shared physical pool. Standalone
lifecycle exports call it explicitly before startup. Participant hosts instead
commit their complete component requests and bind that runtime with
`icydb::db::bind_memory_runtime` before invoking the IcyDB participant. They do
not need an IcyDB pool provider, and database access never constructs a manager. Governance IDs are excluded automatically; add explicit
physical exclusions for unmanaged host memory. All admitted owners share eligible
free slots. Namespaces do not reserve an ID range.
`ic-memory` persists the key-to-ID mapping. The schema does not assign physical
IDs. Use disjoint namespace grants when composing databases
or frameworks. `canic_icydb::start!` exposes `icydb_memory_requests() -> Result<Vec<MemoryRequest>,
MemoryDeclarationError>` at the application root. It describes every permanent
control and journaled store role before any lazy store opens; heap stores add no
stable requests. A composed host combines this list with its other components,
then passes `SealedDeclarationSnapshot::new(requests)` explicitly to Memory
bootstrap. Store initialization only opens committed keys.

A host that bootstraps first must call
`icydb::db::prepare_memory_bootstrap` from its `RuntimeBootstrapPolicy`
preparation hook. Standalone IcyDB bootstrap calls that same hook automatically.

Canister controls use `icydb.app.{commit.control,startup.control,integrity.progress}.v1`.
The store uses `icydb.app.store.transfers.{data,index,schema,journal}.v1`.
Rust renames and declaration order do not change these allocation keys. Accepted
catalogs still bind a store's Rust path, so renaming or moving a populated
journaled store type is unsupported. Startup rejects a changed accepted path
before replay with `RUNTIME_UNSUPPORTED`, without persisting a terminal failure
receipt; restore the original path to resume recovery. Keeping the allocation
key alone does not make a store rename supported. Heap stores have no stable
key. Changing a store key removes one identity and adds another: it
does not rename the store or transfer its rows. Existing retirement checks still
require an empty journal and reject registry changes with a pending marker;
an empty journal does not mean the old store has no data. Retired identities
cannot be reintroduced. Removing an existing database namespace rejects before
allocation commitment, even when a new namespace has a valid grant.

Persisted databases must use the current logical-memory identity contract.
Noncurrent databases require recreation or reinstall under the
[persisted format policy](../contracts/PERSISTED_FORMAT_POLICY.md#current-compatibility-posture).

### Canister memory profiles

The canister declaration selects the bucket size used when IcyDB initializes
the shared memory manager. Omission selects `general`:

```rust,ignore
#[canister(
    memory_namespace = "app",
    memory_profile = "general"
)]
pub struct AppCanister;
```

| Profile | Bucket size | Shared manager capacity |
| --- | ---: | ---: |
| `compact` | 4 pages / 256 KiB | 8 GiB |
| `general` (default) | 16 pages / 1 MiB | 32 GiB |
| `high_headroom` | 128 pages / 8 MiB | 256 GiB |

Capacity is the manager's bucket-table ceiling before platform/backing limits,
not payload capacity. Data, indexes, journals and other users of this manager
share it. Smaller buckets reduce rounding slack, not encoded row size. Use
`compact` for measured small deployments and `high_headroom` when growth needs
more than the general profile's headroom. The
[measured fixture](../reports/investigations/2026/09/13/bucket-sizing/01/report.md)
supports `general` as a starting point, not a universal optimum.

This is one physical bootstrap setting, not per-store configuration or a schema
migration. The build-time model exposes `Canister::with_memory_profile` and
`CanisterMemoryProfile`; generated code uses the same mapping. Existing memory
must match the selected bucket size. Changing it requires recreation/reinstall;
it cannot shrink an existing allocation. For a 128-page IcyDB-owned deployment,
explicitly select `high_headroom` before upgrading to the new default, or recreate
it. That preserves bucket geometry, not compatibility with unrelated format cuts.

When IcyDB-owned bootstrap detects a size mismatch, public startup observation
returns `E274` (`MemoryBucketSizeMismatch`). Its numeric facts are `Expected`
(requested pages) and `Actual` (persisted pages); one page is 64 KiB. The CLI
renders this without a schema artifact. This is a terminal configuration
conflict, not pending recovery: retain the persisted size or explicitly recreate
the database. No allocation is resized by the failed bootstrap.

If the host explicitly bootstraps the shared manager before IcyDB participates,
the host's policy and bucket size take precedence; this setting does not override
them. IcyDB still validates its committed declarations. Inspect the existing
allocation report for effective bucket size and usage. There is no second
profile override on `start!` and no endpoint configuration argument.

Host declarations produce schema proposals and generated adapters. After
acceptance, the accepted schema snapshot is the sole runtime authority for row
layout, constraints, indexes, planning, writes, and recovery. Generated
`EntityModel` and `IndexModel` values may support proposals, reconciliation,
model-only convenience, and tests; the runtime never reconstructs accepted
schema from them.

For a complete starting declaration, see the
[compiled workspace schema example](../../README.md#start-from-a-compiled-example).
For applying an intentional schema change, see [Schema Migrations](schema-migrations.md).
