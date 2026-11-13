# Schema Authoring

Schema declaration macros generate concrete types. Their annotated structs must
not declare type, lifetime or const parameters; these are rejected at the
declaration before code generation. Validator and normalizer hooks preserve
the authored struct and continue to support generic parameters and fields.
Generated schema declarations must have empty Rust bodies; declare their fields
in the macro arguments. Nonempty named and tuple bodies are rejected rather than
silently discarded. Authored outer Rust attributes are forwarded, in order, to
only the main generated type. This includes documentation, lint policy,
`deprecated`, representation and other attribute macros; Rust checks their
compatibility with the generated type. The macro-owned `debug` switch is consumed
rather than forwarded. Hook attributes are emitted once, not duplicated.


This guide covers the choices that most often cause a schema to fail during
host generation: primary-key types, field names, relations, and the boundary
between a shared model crate and a generated canister. The model compiler and
canonical scalar metadata remain the authority when this guide and code ever
disagree.

Runtime-enabled crates normally use `icydb::model`; this keeps model authoring
and database runtime on one direct `icydb` dependency. Schema-only tooling may
instead depend directly on `icydb-model` without pulling in the runtime.
The entity/record macro import is authoritative: direct model macros always emit
schema-only declarations, while the facade's macros emit typed runtime adapters
and field references. Development, optional and target-specific dependencies do
not select emission. When migrating a crate that directly imports model macros
and also uses runtime adapters, replace those imports with
`use icydb::model::prelude::*;` (or the equivalent renamed facade path).

## Generated Rust Names

Generated public types occupy the same Rust module as the entity declaration.
A composite-key entity `User` generates `UserKey`; a scalar-key entity uses its
key atom directly. When runtime adapters are enabled, `User` also generates
`UserInsert`, `UserPatch` and `UserReplace`. These names are part of the public
typed API and follow ordinary Rust type-namespace rules.

Do not declare or import another type with one of those names into that module.
For example, an application DTO named `UserPatch` belongs in a separate module
and can be referenced as `api::UserPatch`. Two entity declarations whose generated
names overlap also need distinct module namespaces. There is no automatic
renaming or helper-name override; downstream code can rely on the conventional
names. Shadowing prelude types such as `Result` or `Option` is supported: macro
expansion qualifies its standard-library and runtime references.

## Whole-Row Write Inputs

Runtime-enabled entities provide `UserInsert::from(row)`,
`UserPatch::from(row)` and `UserReplace::from(row)`. These consume the entity
into the existing generated write input; encode and execute that input through
the usual accepted binding. No second schema parser or field-to-value encoder
is needed.

- Insert copies caller-authored fields and omits generated and managed fields.
  A generated key in the source row is ignored; use the returned inserted row
  to obtain the database-assigned key. Caller-authored scalar and composite
  keys are preserved.
- Patch and replace preserve every primary-key component, including a key
  originally assigned by the database. They supply every authorable non-key
  field. Optional `None` becomes `WriteCell::Null`, not `WriteCell::Omitted`.
  For a partial update, explicitly set the desired patch fields to `Omitted`.
- Managed timestamps and other database-owned values remain absent from the
  authored fields. Scalar relation values become the existing typed IDs;
  optional and repeated relations retain their nulls, order and duplicates.
- `row.into_typed_key()` consumes just the key components into `Id<User>` for
  exact reads or deletion. It supports scalar and composite keys and does not
  encode unrelated row fields.

Conversion does not normalize, validate or authorize application data. Keep
those application hooks explicit before conversion. Encoding and execution
still use the current accepted binding, including renamed-field mappings and
stale-binding refusal. Converting a row does not make an old binding current.

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

Generated `Ulid` keys on IC are predictable public identifiers. The generator
uses the current timestamp and a process-local sequence, rather than random
bits; supplying startup entropy does not change this. Native generation uses
entropy, so a native test does not establish that deployed IDs are unguessable.
Never use possession of an ID as authorization, including for private share
links. Authorize access separately; an application that needs a secret bearer
token must generate and manage that token independently of the row's key.

`Date` uses signed 32-bit epoch days in Wire/binary Serde and ISO `YYYY-MM-DD`
text in JSON. Its calendar domain remains years 0000–9999. `Subaccount` uses an
exact 32-byte byte string in Wire/binary Serde and a 32-element byte array in JSON.
Account database payloads still reserve their existing fixed 62-byte width.
IC Platform Types owns bounded ICRC-1 Account text formatting, parsing and
checksums without a ledger dependency. IcyDB normalizes absent and all-zero
subaccounts to the same database identity at construction, deserialization and
DTO conversion. Equality, hashing, uniqueness, stored/index bytes and text all
use that canonical default representation. Nonzero subaccounts retain all bytes.

```rust
let dto_account: ic_platform_types::Account = database_account.into();
let database_account = icydb::types::Account::from(dto_account);
```

Conversion preserves owner and nonzero subaccount bytes; an explicit zero becomes
absence. Platform may retain an explicit zero in its own DTO even though its
ICRC equality treats it as the default. Both current native codecs encode a
present subaccount as a bounded byte string. IcyDB still owns its schema/codec
contract: decode the declared DTO and convert the value. Stored account frames
with an explicit zero flag are noncanonical and refuse under the new installation
contract. New application ICRC DTOs may use `ic_platform_types::Account` through
a direct Types dependency.
IC Wire owns the optional `platform-types` schema integration. A host schema
producer must select a published Wire adapter for the same canonical Platform
Types generation as its DTO; enabling the feature for another Types generation
does not implement that DTO's schema traits. IcyDB's existing Account schema
projection remains its own.
Applications that need external ledger types construct them from the components
or use Platform's explicitly selected external ledger boundary; IcyDB adds no
ledger dependency or external ledger conversion.

`Decimal` uses decimal text in Wire and Serde. `Int128` and
`Nat128` retain fixed-width numeric domains and use Postcard's signed/unsigned
integer representations. These atoms do not require an arbitrary-precision
integer adapter in IcyDB.

Generated endpoints use IC Wire method/direction envelopes and bounded native
Postcard projections. Strings and collections are admitted by complete message
bytes, allocation, node count and depth. Native callers share the public Rust
payloads and `canic_icydb::wire::Endpoint`; opaque native projections currently have no
automatically generated TypeScript schema. Changing a payload layout requires a
coordinated method-identity change and matching caller updates.

Regenerate external transport and binary-Serde artifacts when adopting this cut.
Binary Serde for Blob is a byte string and U256 has one exact-width byte shape.
There is no previous-format reader. This endpoint change does not modify custom
row/index/cursor encodings; the pending integer-tag hard cut has its own database
recreation obligations.

Schema authoring artifacts also use IC Wire, with separate fragment/proposal/
migration/source-meaning identities. Use the public Schema encoder/decoder APIs
so validation, per-field bounds and canonical byte checks remain enforced.
Regenerate embedded actors and external artifacts together. These bytes determine
retained source-lineage and migration/application digests; they are not a cache
that may be rebuilt from generated models. The 0.291 hard cut requires fresh/reset
installations or a separately reviewed retained-data transition, as described in
the [persisted format inventory](../contracts/PERSISTED_FORMAT_INVENTORY.md#0291-authoring-and-deep-job-codec-cut).

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

Blob field and primitive-newtype defaults use unprefixed, even-length ASCII hex,
matching `fill(literal(blob = "deadbeef"))`. Both uppercase and lowercase digits
are accepted; `""` means empty bytes. `"deadbeef"` represents four bytes
`de ad be ef`, not eight UTF-8 bytes. Decoded literals are capped at 64 KiB.
Whitespace, signs, prefixes, odd lengths and non-hex characters are rejected.
When updating older raw-text defaults, hex-encode the intended bytes: the old
UTF-8 default `"deadbeef"` becomes `"6465616462656566"` to retain its value.
Existing rows are not rewritten; review schema proposals before deployment.
Runtime `Blob::from(bytes)` and wire/storage encodings are unchanged.

Quoted `default` values and `args(...)` entries are always literals, including
text such as `"::1"` or `"a::b"`. Use bare Rust paths for constants and zero-argument
functions: `default = Date::EPOCH`, `default = String::new`, or
`args(crate::LIMIT, crate::default_limit)`. Existing uppercase-final-name constant
and lowercase-final-name function classification applies to those bare paths.
This does not change dedicated path fields such as `validator(path = "...")`
or `generated(insert = "Ulid::generate")`.

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

## Text Expression Indexes

`LOWER`, `UPPER`, `TRIM` and `LOWER(TRIM(...))` use Unicode 18.0.0 data for
persisted keys and query bounds. Core checks the standard library's Unicode
version at compile time, including in downstream canisters. Upgrading a compiler
to different Unicode data therefore requires an explicit retained-index
compatibility review. Do not change the guard merely to make that compiler build.
Golden tests cover every scalar mapping/whitespace property and contextual casing.
No extra mapping tables, runtime check or per-index configuration are introduced.

This freezes the selected compiler's current behavior; it does not prove that
older retained indexes used the same data. Rust 1.91 uses Unicode 16, for example.
For an installation built with different or unknown transform data, establish
its affected-index repair or retirement plan before upgrading. Existing rows,
journal effects and indexes are not disposable. This change neither rebuilds nor
clears them and adds no compatibility reader.

## Prefix Index Declarations

A unique index on `email` and a unique index on `(email, tenant_id)` express
separate constraints; the latter does not enforce email uniqueness by itself.
The model compiler accepts both declarations in either order. It also accepts
prefix-related filtered indexes without treating key prefixes as proof of
identical membership. Predicates and unique constraints remain unchanged in the
generated schema. The existing rejection of unfiltered, non-unique prefix
indexes still applies.

## Index Names

Index names are automatic by default. Add `name` only when an application or
operator needs a particular lookup label:

```rust
index(field = "email")
index(name = "active_email", field = "email", predicate = "active = true")
```

Automatic names combine a readable entity/key label with a 32-hex-digit suffix.
The shared schema owner hashes length-framed exact entity and ordered key terms,
uniqueness and the parsed predicate identity under one versioned domain. Labels
may be shortened to the schema-name byte bound; the suffix always includes the
complete definition. Field/expression distinctions, case-sensitive field names,
key boundaries and different partial predicates therefore participate in naming.
Declaration ordering and unrelated indexes do not participate. Predicate spacing,
keyword case and redundant parentheses do not change names; this is a structural
parsed identity, not a general proof that differently written SQL is equivalent.

Explicit names use the existing bounded schema-name contract and cannot use the
engine-reserved `__icydb_` prefix. An override changes only the lookup label;
uniqueness, membership and accepted physical `IndexId` authority remain separate.
Names must be distinct within the entity, whether automatic or explicit. A digest
is not an injectivity proof: conflicting names still reject at the later declaration
with an override example. Repeating the same parsed index definition also rejects,
even with different explicit names. Model and runtime intake checks remain.

This pre-1.0 hard cut changes automatic names. Update name-based tooling from the
accepted catalog, or explicitly retain a chosen previous label with `name`.
There is no old-name fallback, alias map or declaration-order numbering. A changed
automatic name must follow normal generated schema reconciliation; it does not
authorize dropping an index or treating retained data as disposable.

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
names; indexes derive names unless an explicit `name` overrides the label. Do not create a second name map in
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

Recursive types without reachable relations remain supported. A relation-bearing
cycle is rejected even if a finite sibling path reaches the same target.

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

## Composite Application Hooks

For records, entities, enums and tuples, `ty(normalizer(...))` and
`ty(validator(...))` receive the complete generated value. They run in declaration
order before that node's field or payload callbacks. Enum normalizers may change
the active variant; payload hooks and child traversal then use the resulting
variant. Newtype hooks continue to receive the inner value, and list/set/map
type hooks continue to receive the inner collection.

Tuple-value and enum-payload item hooks follow scalar, optional and repeated
cardinality, just like record/entity field-item hooks. Absent optional values,
empty repeated values and unit variants invoke no payload callback. Diagnostics
retain the declared callback's identity and the tuple position or variant name.
The existing visitor separately runs each child's own hooks after the parent's
attached callbacks. Callback issues are accumulated using the existing visitor
contract; normalization is not a transactional database write.

Earlier releases silently skipped composite type hooks and tuple/enum payload
hooks ([#68](https://github.com/dragginzgame/icydb/issues/68)). Rebuild consumers
and review those declarations: they now execute and may modify values or reject
application validation. There is no compatibility switch to keep skipping them.

## Collection Application Hooks

Normalizers attached to record/entity field items run on the item itself:
scalar fields run once, optional fields run only for `Some`, and repeated
fields run once per element. Multiple callbacks retain declaration order and
field-path diagnostics. A callback on `value(opt, item(...))` therefore implements
`Normalizer<T>`, rather than `Normalizer<Option<T>>`; the same rule applies to
`many` and `Vec<T>`. Use a named collection's container normalizer for whole-list
policy. Existing traversal still runs each child's own hooks.

Explicit `normalize(&mut value)` calls run list-item normalizers and map-value
normalizers in declaration order. Container normalizers run first, followed by
the attached item callbacks and the existing traversal of each child's own
hooks. Map values support scalar, optional and repeated cardinality; absent
values and empty collections invoke no item callback. Callback issues retain
their collection-entry index and normalizer identity.

Set items and map keys cannot declare direct item normalizers: mutating a sorted
key in place would invalidate container ordering. Normalize keys before insertion
or use an explicit container normalizer that rebuilds the collection. Immutable
key validators and container normalizers remain supported.

These application hooks are explicit. Database structural writes, SQL and
recovery do not invoke them; accepted constraints remain the database admission
authority.

## Numeric Application Hooks

Numeric comparison, range, divisor and clamp settings must be representable as
`Decimal`; for example, `u128::MAX` is outside that domain. Decimal-place settings
must convert to `u32`. Constructors retain conversion failure as invalid
configuration: validators report a visitor issue when invoked, and normalizers
return an error before changing the value. Invalid settings never become zero
bounds or zero decimal places. Valid settings and zero-based defaults retain
their behavior.

These are application hooks. Database structural writes, SQL and recovery do not
invoke them; accepted constraints remain the database admission authority. See
the [write-admission contract](../contracts/WRITE_ADMISSION.md).

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

Put `icydb` in both `[dependencies]` and `[build-dependencies]`, selecting
`features = ["build"]` only on the build dependency. The build
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

The schema library forwards the request to each dependency whose declarations
belong to its graph. For a facade-only schema library:

```toml
[features]
default = []
build = ["icydb/build"]

[dependencies]
icydb = { workspace = true }
```

The consuming canister enables `app-schema/build` through its schema
`[build-dependencies]` entry. A schema-only library using `icydb-model` forwards
`build` to `icydb-model/build` instead; shared schema dependencies forward it
recursively. Normal dependency entries do not request it. Cargo separates build
and runtime feature contexts, including native builds.

Without `build`, declarations emit application types and behavior without
process-global registration or authoring descriptors. Host `build`, `node`,
`fragment` and graph error APIs require that feature. Feature selection belongs
to IcyDB's dependency; callers need no feature with a matching name. `wire-schema`
is a separate opt-in for native binding metadata on `icydb`, `icydb-core`,
`icydb-model` and `icydb-schema`, and does not collect the application graph.
It covers `Id<E>` through `E::Key` without requiring entity metadata, `Unit`,
`Blob`, and the model library's transparent base newtypes (including geographic
and web wrappers). Application-defined records/enums still need their own Wire
metadata. Layout metadata follows Serde; it does not run application validation
or normalization. Blob has no universal maximum: concrete endpoints must supply
finite Wire admission limits. These host implementations do not enter Wasm.
Runtime codecs and accepted catalog authority do not depend on either generation
feature.

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
fn icydb_storage_grants() -> Result<icydb::db::AllocationGrants, icydb::db::AdmissionError> {
    icydb::db::AllocationGrants::new(vec![
        icydb::db::MemoryAuthority::new("icydb_app", "icydb.app.")?,
    ])
}
```

The required host `icydb_storage_grants` provider grants disjoint permanent
namespaces to named owners. It is called only for IcyDB-owned cold bootstrap;
an already bound composed host is adopted without invoking it. Storage owns
allocation identities and placement; there are no application-selected physical
slots or numeric exclusions. IcyDB's owner label is `icydb_<namespace>` while
its permanent keys retain the `icydb.<namespace>.` prefix.
`ic-storage` persists the key-to-ID mapping. IDs are opaque 64-bit identities,
not memory addresses or opening capabilities. Use disjoint namespace grants
when composing databases or frameworks. Generated `icydb_memory_requests()`
returns `Result<Vec<MemoryRequest>, AdmissionError>` and describes every permanent
control and journaled store role before any lazy store opens; heap stores add no
stable requests. A composed host combines this list with its other components,
then passes `SealedDeclarationSnapshot::new(requests)` explicitly to Storage
bootstrap. Store initialization only opens committed keys.

A host that bootstraps first must call
`icydb::db::prepare_memory_bootstrap(&snapshot, admission)` from its
`RuntimeBootstrapPolicy` preparation hook, passing the exact source snapshot. Standalone IcyDB bootstrap calls that same hook automatically.

Canister controls use `icydb.app.{commit.control,startup.control,integrity.progress}.v1`.
The store uses `icydb.app.store.transfers.{data,index,schema,journal}.v1`.
Rust renames and declaration order do not change these allocation keys. Accepted
catalogs still bind a store's Rust path, so renaming or moving a populated
journaled store type is unsupported. Startup rejects a changed accepted path
before replay with `RUNTIME_UNSUPPORTED`, without persisting a terminal failure
receipt; restore the original path to resume recovery. Keeping the allocation
key alone does not make a store rename supported. Heap stores have no stable
key. Changing a store key removes one identity and adds another: it
does not rename the store or transfer its rows. Omitted journaled stores become
dormant, retaining every allocation and accepted catalog. Exact quartets may be
restored without clearing, reinitializing or transferring their rows. Generated
reconciliation and exact-submission replay retain their existing owners;
restoration never reconstructs accepted catalogs from declarations.
This allows correction after a failed deployment, including removing and later
restoring an added store with the same keys. Removal/restoration require an empty
journal. Registry changes refuse with a pending marker or pending generated
schema application, so the original registry remains usable. An empty journal
does not mean the old store has no data. All active and dormant quartets continue
to count toward the bounded registry capacity. Removing an existing database namespace rejects before
allocation commitment, even when a new namespace has a valid grant.

Persisted databases must use the current logical-memory identity contract.
Noncurrent databases require recreation or reinstall under the
[persisted format policy](../contracts/PERSISTED_FORMAT_POLICY.md#current-compatibility-posture).

### Storage configuration and installations

Generated standalone startup uses `ic-storage::StorageConfig::default()`.
A composed host may construct and bootstrap Storage with its chosen resource
limits before binding the runtime. IcyDB verifies its admitted declarations
without replacing that configuration or invoking the grants provider again.
Allocation reports expose retained names, opaque identities, committed generation
and logical extent bytes. They do not report physical allocator usage or payload
bytes.

This adoption targets **fresh installations only**. Storage 0.64 rejects earlier
Memory/Storage images without changing their bytes. There is no compatibility
reader, converter or automatic reset. Retain earlier binaries and backups for
required recovery/export; this source update authorizes no deletion or deployment.
Same-contract restarts preserve named allocations and require current admission.

Host declarations produce schema proposals and generated adapters. After
acceptance, the accepted schema snapshot is the sole runtime authority for row
layout, constraints, indexes, planning, writes, and recovery. Generated
`EntityModel` and `IndexModel` values may support proposals, reconciliation,
model-only convenience, and tests; the runtime never reconstructs accepted
schema from them.

For a complete starting declaration, see the
[compiled workspace schema example](../../README.md#start-from-a-compiled-example).
For applying an intentional schema change, see [Schema Migrations](schema-migrations.md).
