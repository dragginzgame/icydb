# Public Facade API

This guide describes the maintained application-facing surface after the
0.213 hard cut. Runtime authority always comes from accepted schema. Generated
Rust types are optional adapters at the boundary; they are not planner,
admission, storage, or recovery inputs.

Generated IcyDB endpoints enter one request scope automatically. Manual
IC-CDK, Canic, lifecycle, and timer entries put
`#[icydb::request_execution]` outside the framework attribute. Zero-argument
`db!()` calls in nested helpers share its monotonic counters across the whole
sync or async invocation. Synchronous unit tests use `#[icydb::test]`. The
snippets below assume that default scope.

Use `with_request_execution_root` plus `db!(&request_root)` only when a
low-level adapter intentionally owns and passes scope ownership itself. The
argument selects the existing root; it never creates or resets a budget and
is rejected if another root is active. It is not shared with the called
canister, which has a separate IcyDB instance and request scope.

## Request Entry

The boundary attribute is framework-neutral and must appear outside the
framework export attribute:

```rust,ignore
#[icydb::request_execution]
#[canic_query(requires(auth::authenticated()))]
async fn refresh() -> Result<Receipt, Error> {
    authorize().await?;
    refresh_with(db!()?).await
}
```

Conceptually, a sync function calls `with_request_execution(|| body)` and an
async function awaits `with_request_execution_async(async move { body })`.
The async future retains its root counters across suspension, installs that
root immediately before each poll, and removes it immediately afterward.
Nested attributed helpers reuse the active root. Caller authorization remains
application-owned; the execution root supplies database accounting only.

The explicit argument form is for integrations that deliberately retain the
root themselves:

```rust,ignore
let task = icydb::db::with_request_execution_root(|root| async move {
    let before = db!(&root)?.get(user_id)?;
    call_another_canister().await?;
    let after = db!(&root)?.get(user_id)?;
    Ok::<_, icydb::Error>((before, after))
});
let result = task.await?;
```

Every explicit lookup reuses the root's cumulative counters. It is rejected
if a different request root is already active. Ordinary endpoints should not
use this form merely because they contain `.await`.

## Read Surfaces

Typed and dynamic reads are part of base IcyDB and do not depend on SQL parser
or response types. The optional `sql` feature adds a frontend over the same
engine-neutral query runtime:

```rust
let page = db!()?
    .query::<User>()?
    .filter(FieldRef::new("active").eq(true))
    .order_by(asc("id"))
    .limit(25)
    .execute_live_page(None)?;
```

`query::<E>()` is generated automatically for entities declared in a crate
that depends on the `icydb` runtime facade. The generated adapter resolves the
authored entity and field names through the current accepted source bindings.
It then decodes returned public values; it never supplies query semantics.

Every named enum, record, newtype, list, set, map, or tuple implements the
model-owned conversion traits automatically, including IcyDB's built-in model
types. The macros do not synthesize suffix-derived Rust types. Named values
resolve accepted type, member, and variant names through the entity binding
before crossing the existing `InputValue` / `OutputValue` boundary.

`DynamicQuery` is the untyped accepted-schema equivalent. Use
`execute_live_page(&request, continuation)` for caller-facing bounded reads and
`execute_trusted_live_page` only after application-owned admin authorization.
Always return or consume `page.continuation`; a non-null value proves the read
has not yet established exhaustion.

Grouped reads use the same accepted-schema planner, plan cache, executor, and
public-value conversion as SQL. They do not require the SQL parser or SQL
response types:

```rust
let page = db!()?
    .query::<User>()?
    .group_by("country")
    .aggregate(count())
    .grouped_limits(100, 64 * 1024)
    .limit(25)
    .execute_grouped()?;
```

`grouped_limits` is mandatory and bounds total groups and bytes per group. A
positive `limit` bounds the current page; pass a returned `next_cursor` back
through `.cursor(...)` for the next page. Group keys and aggregates define the
output in declaration order, so grouped queries do not also use `.select(...)`.
Dynamic callers use `execute_public_dynamic_grouped_query` or the explicitly
trusted `execute_trusted_dynamic_grouped_query` terminal.

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

let result = db!()?.execute_trusted_structural_mutation(
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

let result = db!()?.execute_trusted_structural_mutation_batch(vec![
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

## Public Errors

Public `icydb::Error` values carry a stable numeric E-code, compact class and
origin codes, and a bounded sequence of `DiagnosticFact { tag, value }`
records. The E-code owns the reason; facts contain only numeric parameters such
as positions, counts, limits, versions, and accepted IDs.

Ordinary errors do not carry schema-specific names, values, keys, rows, SQL
text, principals, or diagnostic prose. Use `Error::facts()` and the
production-safe numeric identities under `icydb::diagnostic` for machine
handling. The CLI owns human-readable labels and always retains a numeric
fallback for unknown tags.

Historical constraint-validation findings remain explicit operational output
because their bounded row locator is needed for acknowledgement and repair.
They are not embedded in ordinary `Error` values.

## Current Construction Surfaces

Use generated `*Insert` inputs for application-authored inserts. Runtime reads
and writes enter through accepted-schema typed queries, structural mutations,
or explicit SQL lanes; generated declarations remain proposal and
reconciliation input rather than runtime authority.
