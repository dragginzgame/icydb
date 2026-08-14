# Public Facade API

This guide describes the maintained application-facing surface after the
0.213 hard cut. Runtime authority always comes from accepted schema. Generated
Rust types are optional adapters at the boundary; they are not planner,
admission, storage, or recovery inputs.

Generated IcyDB endpoints enter one request scope automatically. Manual
IC-CDK, Canic, and timer entries put `#[icydb::request_execution]` outside the
framework attribute. Application lifecycle callbacks declared through the
composed `icydb::start!` form receive that scope automatically after IcyDB has
registered its startup watchdog. Zero-argument `db!()` calls in nested helpers
share its monotonic counters across the whole sync or async invocation.
Synchronous unit tests use `#[icydb::test]`. The snippets below assume that
default scope.

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

Use `execute_exhaustive_page` for validation, export, or any operation that
must prove it visited one complete unchanged set. Its page additionally
returns a `ReadSetRevisionProof`. Persist that proof beside the continuation
and pass both back unchanged. For work that reads several entities, capture
the complete proof before the first page:

```rust,ignore
let proof = db!()?.capture_read_set_revision_proof(&[
    "Token",
    "GeneratorTokenProvenance",
    "GeneratorExportSnapshot",
])?;

let page = db!()?.execute_exhaustive_page(
    &request,
    prior_continuation.as_deref(),
    Some(&proof),
)?;
```

A source-row, accepted-root, or physical access-state change returns a typed
revision error; restart from a new proof rather than retrying the same page.
Mutations in stores absent from the proof do not invalidate it. Heap stores
may be used for one-call exhaustive reads but cannot back durable cross-call
jobs.

Long-running update workflows use `start_resumable_job` and
`compare_proof_and_advance`. The latter runs one synchronous page closure,
rechecks the proof, and atomically retains the next bounded application state
and receipt in IcyDB's excluded progress domain. Replaying the same sequence
and idempotency key returns the retained receipt without executing the closure
again. The application still owns job authorization, accumulator meaning, and
restart policy. A null continuation commits `Completed`; only exact replay or
sequence-checked `acknowledge_resumable_job` remains. Acknowledgement is
terminal-only and idempotent so its lost reply can be retried while freeing
bounded progress capacity. Proof capture and every progress-control operation
charge the same request-wide execution counter used by ordinary database
calls; opening another session cannot reset that allowance.

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

- return at most 100 rows per page; an optional query `LIMIT` bounds the total
  window across all pages;
- require a planner-proven bounded/index-backed route;
- reject full scans and materialized sorts;
- expose only IcyDB-issued scalar continuation; callers cannot supply offsets
  or page-policy controls.

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
- `start_trusted_sql_mutation_job`
- `mutation_job_state`
- `advance_trusted_mutation_job`
- `acknowledge_mutation_job`
- `execute_admin_sql_ddl`
- `execute_admin_integrity_sql`

They resolve entity identity from the SQL statement against accepted catalog
authority. They are not safe templates for caller-controlled SQL. Generated
`icydb_ddl` and optional update endpoints remain controller-gated.
`icydb_query` is controller-gated by default; a source declaration may instead
replace controller authority with one synchronous application guard over the
caller and complete SQL read surface. `icydb_schema` independently requires an
explicit `public`, `controller`, or `guard(path)` choice. Its guard sees the
`Schema` surface and protects only that dedicated accepted-schema method; SQL
`SHOW`, `DESCRIBE`, and `EXPLAIN` remain under the SQL guard.

Durable mutation jobs retain the canonical fixed-update intent and private
engine continuation inside IcyDB. Callers keep only the job id, expected
sequence, and a bounded idempotency key. Each advance performs at most one
256-key/64-update Forward step or one 256-key Verify step, and exact request
replay returns the retained receipt without scanning or mutating again. Verify
completes only after a clean exhaustive pass at one unchanged durable target
revision. A target revision change or residual row restarts Forward; accepted
authority or internal batch-policy drift yields a typed terminal
`RestartRequired` state. Callers acknowledge a consumed terminal sequence to
remove its retained record; repeating acknowledgement after response loss is
safe.

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
