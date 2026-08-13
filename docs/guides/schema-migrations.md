# Schema Migrations

IcyDB schema migrations are explicit, sequential deployments between adjacent
per-entity source versions. The accepted schema remains runtime authority;
generated declarations describe the proposed successor and one source-owned
migration plan explains every intentional rename or physical rewrite.

Schema migration is an optional canister capability. Enable `icydb/migration`
only in Wasm artifacts that need to adopt or advance a migration, and declare
both maintained controller endpoints in source:

```rust
icydb::start!();

icydb::endpoints! {
    icydb_schema_migration;
    icydb_schema_migrate;
}
```

Compiling the capability does not publish an endpoint, and declaring an
endpoint without the capability fails compilation. The CLI consults only the
deployed canister; it does not infer a migration from local source files.

After an upgrade, application lifecycle code must keep database-dependent
schedulers deferred while `startup_state()` reports `Recovering`. An
authorized controller may inspect and advance the existing migration status
during that window. Continue polling typed readiness and restore application
state only after the migration is terminal and startup reports `Ready`; do not
infer readiness from a delay or a generic conflict. See
[startup-readiness.md](startup-readiness.md) for the composed lifecycle form.

## Declare The Current Version

Every entity has its own positive source version. Start a new entity at
version 1:

```rust
#[entity(
    store = "AppStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "age", value(item(prim = "Int32"))),
        field(name = "rank", value(item(prim = "Int32")))
    )
)]
pub struct User {}
```

Versions are not database-wide release numbers. Increment only an entity that
participates in the next plan, and increment it by exactly one.

## Adopt An Existing Database Once

A database created before IcyDB 0.218 has no accepted source-lineage record.
Deploy a migration-capable Wasm that still declares the exact version-1
schema, then inspect and adopt it:

```bash
icydb schema migration status app
icydb schema migration adopt app --yes
```

Adoption succeeds only when the deployed version-1 proposal is an exact no-op
against accepted authority. It records the existing accepted IDs and source
digests; it does not rewrite rows or infer a later schema. A database first
created by current IcyDB already records version-1 lineage and does not need
this operation.

Do not combine adoption with the version-2 deployment. Observe successful
adoption first, then build and deploy the successor artifact.

## Declare One Adjacent Migration

The canister owns one coordinated migration plan. This example renames
`rank` to `score` while preserving its accepted field identity, and rewrites
`age` from `Int32` to `Nat16` with checked exact conversion:

```rust
#[canister(
    migrations(
        entity_migration(
            entity = "User",
            from = 1,
            renames(field(from = "rank", to = "score")),
            transforms(
                rewrite(
                    from = "age",
                    to = "age",
                    checked_cast(to = "Nat16")
                )
            )
        )
    ),
    memory_namespace = "app",
    memory_min = 100,
    memory_max = 104,
    commit_memory_id = 104
)]
pub struct AppCanister {}

#[entity(
    store = "AppStore",
    version = 2,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "age", value(item(prim = "Nat16"))),
        field(name = "score", value(item(prim = "Int32")))
    )
)]
pub struct User {}
```

The plan must account exactly for the predecessor-to-successor difference.
There is no implicit rename matching, source-key override, version gap,
application callback, SQL migration language, or compatibility alias.

## Deploy And Run

Build the successor Wasm with its migration capability and source endpoints,
then upgrade the canister. Inspect the exact deployed plan before advancing:

```bash
icydb schema migration status app
icydb schema migration run app
icydb schema migration status app
```

`run` issues repeated bounded `Advance` operations while the database,
accepted head, deployment, and plan identities remain unchanged. A single
bounded step is also available:

```bash
icydb schema migration advance app
```

The database remains available while the plan is merely `Prepared`. From
validation through final publication, ordinary reads, writes, DDL, relation
work, and schema mutation return the typed migration-in-progress error. Final
publication atomically switches accepted schema, source lineage, receipt, and
terminal migration authority. The predecessor source name is then absent;
callers must use `score`, not `rank`.

## Resume Or Abort

Every phase and cursor is durable. If an upgrade or trap interrupts the job,
deploy the exact same successor plan and run it again:

```bash
icydb schema migration status app
icydb schema migration run app
```

Changing the proposal or plan while a migration is active fails closed. There
is no regeneration, local-source fallback, or invisible retry under different
code.

Abort is available only before row rewriting begins:

```bash
icydb schema migration abort app --yes
```

Once rewriting has started, resume the exact plan to completion. A rejected
validation remains unavailable until the controller either performs the
permitted pre-rewrite abort or deploys the exact plan needed to inspect and
resolve it.

After an `Applied` result has been verified, a later same-version deployment
may omit the migration endpoints and capability. The terminal accepted schema
and lineage remain durable authority; the old plan is not a runtime fallback.

## Operational Checklist

1. Back up and identify the exact deployed Wasm and accepted head.
2. Adopt a pre-0.218 version-1 database in a separate deployment.
3. Build one adjacent successor and review its complete migration plan.
4. Deploy the successor, inspect `status`, then run bounded advancement.
5. Keep the exact successor Wasm available until `Applied` is observed.
6. Verify reads through the new names and types before removing migration-only
   capabilities from a later deployment.

Migration is deliberately offline and controller-operated. It is not an
online rolling-schema protocol, a cross-canister transaction, or an import and
restore facility.
