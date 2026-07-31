# Public Facade API

This guide describes the maintained application-facing surface after the
0.213 hard cut. Runtime authority always comes from accepted schema. Generated
Rust types are optional adapters at the boundary; they are not planner,
admission, storage, or recovery inputs.

## Read Surfaces

Typed reads require the engine-neutral `query` feature. The `sql` feature
depends on `query`, but typed and dynamic reads do not depend on SQL parser or
response types:

```rust
let rows = db()?
    .query::<User>()?
    .filter(FieldRef::new("active").eq(true))
    .order_by(asc("id"))
    .limit(25)
    .execute_rows()?;
```

`query::<E>()` is generated automatically for entities declared in a crate
that depends on the `icydb` runtime facade. The generated adapter binds
immutable entity and field source keys to the current accepted snapshot. It
then decodes returned public values; it never supplies query semantics.

Every named enum, record, newtype, list, set, map, or tuple implements the
model-owned conversion traits automatically, including IcyDB's built-in model
types. The macros do not synthesize suffix-derived Rust types. Named values
resolve accepted type, member, and variant names through the entity binding
before crossing the existing `InputValue` / `OutputValue` boundary.

`DynamicQuery` is the untyped accepted-schema equivalent. Use
`execute_public_dynamic_query` for caller-facing bounded reads and
`execute_trusted_dynamic_query` only after application-owned admin
authorization.

Ordinary public reads:

- require a positive `LIMIT` no greater than 100;
- require a planner-proven bounded/index-backed route;
- reject full scans and materialized sorts;
- do not expose scalar cursor, page, or offset APIs.

See [READ_ADMISSION.md](../contracts/READ_ADMISSION.md).

## Write Surfaces

`StructuralMutation` is the canonical public dynamic write request. Fields are
named at the boundary and resolved once against accepted schema:

```rust
let patch = StructuralPatch::new().field(
    "name",
    WriteCell::Value(InputValue::Text("Ada".to_string())),
);

let result = db()?.execute_trusted_structural_mutation(
    StructuralMutation::Update {
        entity: "User".to_string(),
        key: InputValue::Ulid(user_id),
        patch,
    },
)?;
```

The four mutation variants are `Insert`, `Update`, `Replace`, and `Delete`.
`WriteCell` keeps omission, explicit `DEFAULT`, explicit `NULL`, and authored
values distinct until accepted write admission.

Generated `Insert`, `Patch`, and `Replace` input types implement
`TypedWriteAdapter` whenever the declaring crate includes the runtime facade.
Bind the generated entity to the current session, encode the input, then call
`execute_trusted_typed_write`. This is an ergonomic projection over the same
structural mutation authority.

`execute_trusted_structural_insert_batch` is the maintained atomic
same-entity insert-batch surface. It either commits every patch or publishes
none.

For conservation-sensitive same-entity changes, submit the complete
insert/update/replace/delete set through
`execute_trusted_structural_mutation_batch`:

```rust
let source = StructuralPatch::new().field(
    "quantity",
    WriteCell::Value(InputValue::Nat64(60)),
);
let output = StructuralPatch::new().field(
    "quantity",
    WriteCell::Value(InputValue::Nat64(40)),
);

let result = db()?.execute_trusted_structural_mutation_batch(vec![
    StructuralMutation::Update {
        entity: "TokenHolding".to_string(),
        key: InputValue::Ulid(source_id),
        patch: source,
    },
    StructuralMutation::Insert {
        entity: "TokenHolding".to_string(),
        patch: output,
    },
])?;
```

The batch uses one accepted snapshot and operation timestamp, validates one
complete final-row overlay, and either publishes every mutation or none.
Split, merge, and transfer logic must not replace this call with sequential
writes or compensation.

In a canister application, authorize first and perform the final
read/calculate/batch sequence synchronously, without an `await` or another
logical interleaving point. If asynchronous work is required, complete it
first, then re-read the holdings and recompute the batch. Atomic publication
does not make a calculation from an earlier, stale read current, and IcyDB
does not infer a hidden retry or cross-entity transaction.

## SQL Surfaces

SQL entry points are explicit trusted/admin lanes:

- `execute_trusted_sql_query`
- `execute_trusted_sql_mutation`
- `execute_trusted_sql_exact_update`
- `execute_trusted_sql_prefix_update`
- `prepare_trusted_sql_resumable_update`
- `resume_trusted_sql_resumable_update`
- `execute_admin_sql_ddl`
- `execute_admin_integrity_sql`

They resolve entity identity from the SQL statement against accepted catalog
authority. They are not safe templates for caller-controlled SQL. Generated
`icydb_query`, `icydb_ddl`, and optional update endpoints remain
controller-gated.

## Schema And Integrity

Schema proposals come from `icydb-schema`. Application-model declarations in
`icydb-model` lower into that bounded contract, but only the accepted snapshot
is runtime authority.

Integrity requests use the typed `IntegrityCheckRequest` protocol or the
admin SQL integrity surface. They never accept caller-authored checkpoints,
proof vectors, or physical traversal state.

## Current Construction Surfaces

Use generated `*Insert` inputs for application-authored inserts. Runtime reads
and writes enter through accepted-schema typed queries, structural mutations,
or explicit SQL lanes; generated declarations remain proposal and
reconciliation input rather than runtime authority.
