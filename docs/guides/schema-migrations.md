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
    pk(fields = ["id"]),
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

## Adoption Is Not A Format Upgrade

Current IcyDB databases record initial source lineage at creation. They do not
need a separate adoption step. For a current-format database whose typed status
reports unadopted lineage, the existing adoption operation accepts only the
exact initial source proposal:

```bash
icydb schema migration status app
icydb schema migration adopt app --yes
```

Adoption is an exact no-op against accepted schema authority. It records accepted
IDs and source digests; it does not rewrite rows, decode retired formats or infer
a successor schema. Observe accepted lineage before proposing an adjacent source
revision. Incompatible pre-1.0 storage/ledger formats require recreation or
reinstall, not adoption or migration rehearsal.

Entity source revisions are distinct from internal encoding discriminators:
the adjacent source revision in the example below changes while internal formats
remain at their maintained version 1. The
[0.259 rehearsal design](../design/0.259-migration-rehearsal/0.259-design.md)
defines isolated qualification within that current-format boundary. The
maintained fixture below is not a production-data migration tool.

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
    memory_namespace = "app"
)]
pub struct AppCanister {}

#[entity(
    store = "AppStore",
    version = 2,
    pk(fields = ["id"]),
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

### Entity rename with inbound relations

For a same-store `Item` to `CatalogItem` rename, keep all fields, indexes and
durable namespace/store keys unchanged. Rename the declaration and update its
self-reference target spelling. Advance its source version from 1 to 2.
Every inbound owner whose target spelling changes also advances explicitly:
`Holder(item_id -> Item.id)` becomes `Holder(item_id -> CatalogItem.id)` at
source version 2, even though Holder's rows do not change.

Declare the two transitions together on the canister:

```rust
migrations(
    entity_migration(entity = "CatalogItem", from = 1, from_name = "Item"),
    entity_migration(entity = "Holder", from = 1)
)
```

The companion is not permission for arbitrary empty migrations. The planner
requires its complete accepted source meaning to match after reversing only
relation-target entity names explicitly renamed by this plan. Unrelated field,
type, index or constraint changes still reject, as do meaningless version bumps.
Do not add a fake transform, rename Holder's own fields, or omit its transition.
See the compiled [Item/Holder declarations](../../schema/test/sql/src/entity_rename.rs)
and [canister plan](../../schema/test/sql/src/sql.rs).

This metadata-only transition publishes atomically in one `Advance`, preserving
accepted identities and stored rows/indexes. It does not enter physical rewrite
phases. Retrying the exact command after a lost response returns the same receipt;
keep its original database, predecessor head and plan digest. Startup readiness
still governs ordinary requests, including after deploying the successor.

The existing replicated watchdog prepares the accepted runtime root before
becoming quiescent. The rename rehearsal qualifies retained preparation after
publication and restart; it does not promise warm caches before the callback
or after every later schema change. See the
[current measurement and lifecycle costs](../design/0.261-entity-rename/c37-runtime-preparation.md).

### Controller workflow

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

The core migration gate permits ordinary row operations during `Prepared` when
the migration capability is present, but this is not application readiness:
the unpublished generated successor reports startup `Recovering`. Generated ordinary
endpoints require startup `Ready`; never resume application work from the
migration phase alone. From validation through final publication, the migration
gate also blocks ordinary reads, writes, DDL and relation work. Final
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

Abort retains the accepted source schema; it does not change the successor
artifact's generated API. To resume the original application's reads after
aborting, redeploy the exact source artifact using the same current storage
format, then allow startup recovery to finish.

After an `Applied` result has been verified, a later same-version deployment
may omit the migration endpoints and capability. The terminal accepted schema
and lineage remain durable authority; the old plan is not a runtime fallback.

## Operational Checklist

1. Back up and identify the exact deployed Wasm and accepted head.
2. Verify accepted source lineage; use adoption only for a current-format
   unadopted database with an exact initial proposal.
3. Build one adjacent successor and review its complete migration plan.
4. Deploy the successor, inspect `status`, then run bounded advancement.
5. Keep the exact successor Wasm available until `Applied` is observed.
6. Verify reads through the new names and types before removing migration-only
   capabilities from a later deployment.

Migration is deliberately offline and controller-operated. It is not an
online rolling-schema protocol, a cross-canister transaction, or an import and
restore facility.

## Rehearse The Maintained Example

The focused integration target builds both `SqlTestUser` source revisions from
the current checkout through the retained Cargo/post-link pipeline. It needs the
ordinary Wasm build tools and PocketIC setup, but no externally prepared Wasm
paths or ignored-test flag:

```bash
cargo test -p icydb-testing-integration --test schema_migration_closeout -- --nocapture
```

Three physical-migration tests cover uninterrupted success, upgrades during both `Prepared` and
gated validation, and a rejected Int32-to-Nat16 cast.
They verify preserved row IDs, typed output, indexed lookup, accepted-head and
receipt bindings, retained progress, and source data after the permitted abort.
The source/successor actors use the same current storage format, not different
IcyDB releases. The host bounds advancement to 32 calls; exhausting this fixture
limit fails qualification rather than claiming successful migration. Two further
tests rehearse the populated Item/Holder rename, with a restart before admission,
exact-command lost-response retries, and restart after publication. They check
self/inbound relation validation, restrictive deletion and uniqueness, without
combining the rename with the physical cast. Run only that slice with the
`entity_rename::` test-name filter. Native compound-marker interruption remains
separate from generated-canister restarts; see the
[0.261 tracker](../design/0.261-entity-rename/0.261-status.md).

The target prints raw Wasm sizes, artifact hashes and whole-call cycle charges
around explicit migration commands. These charges include ingress and any work
scheduled during the call, not just the migration function. They exclude install,
upgrade, seeding, separate reads and startup delivery outside the command.
Migration-body instruction counts are not instrumented. The rename tests also
use the existing SQL query instruction probe before/after publication and after
restart; its interval is not the migration command or whole request envelope.
No numeric performance ceiling or optimization claim is inferred from this small
dataset. Raw actor-size differences include fixture declaration/plan reachability,
not just runtime-library code. The target creates disposable local
PocketIC canisters and uses fixture controls for seeding and SQL/typed inspection;
migration commands remain typed. Fixed rename controls seed through accepted
structural writes and check restrictive deletes through the trusted mutation
API; `icydb_update` owns the public UPDATE checks, not INSERT/DELETE setup.
It does not inspect application
data, deploy a production canister or prove another dataset will migrate.

A valid pending migration
keeps ordinary requests gated while explicit controller commands own progress;
it does not become a terminal unsupported-schema startup failure. The watchdog
does not keep retrying that pending plan, including after a `Prepared` restart.
The [status tracker](../design/0.259-migration-rehearsal/0.259-status.md) owns current
qualification; historical reports retain their exact inputs and observations.

## Adapt The Rehearsal To Your Application

Start from the maintained [integration test](../../testing/integration/tests/schema_migration_closeout.rs),
not a new migration runner. The fixture does not qualify your schema, framework
lifecycle or production rows.

1. **Freeze both actors.** Build current and adjacent successor source schemas
   against one lockfile, toolchain and current IcyDB/storage format. Preserve the
   namespace and durable store keys. Record the revision, dirty patch, features
   and exact artifact hashes. Retain build owners until bytes are read; do not
   reopen a mutable Cargo output after another build. Incompatible pre-1.0 data
   needs explicit recreation/regeneration, not this test as an upgrade bridge.
2. **Seed through your domain API.** Install a disposable local actor, wait for
   typed readiness and use application-owned writes with ordinary authorization
   and accepted-value admission. SQL is only the fixture's choice; applications
   need not enable it. Keep test controls out of production exports. Use synthetic
   representative values and conversion boundaries, not imported production data
   or hand-written stable memory.
3. **Specify results before upgrading.** Retain row IDs within each run, accepted
   head, logical values and expected indexed/domain reads. Compare logical values
   across independently installed control/interrupted runs, not random IDs or
   allocation details. Add relation/constraint expectations when they are part
   of your application's transition.
4. **Run the same three scenarios.** Complete an uninterrupted control, repeat
   its logical seed with an upgrade at an observed durable gated phase, then seed
   a value the successor must reject. Bind commands to database/head/plan values
   returned by status. Resume with the exact successor artifact, not a rebuilt
   or edited plan. Check the receipt and final rows, not just `Applied`.
5. **Handle rejection honestly.** Require the expected typed finding and no
   premature publication. Abort only where the phase permits; redeploy the exact
   source actor to inspect retained source data after abort. Once rewriting
   starts, resume the exact plan rather than deploying the source as a rollback.
   The example has one finding page; larger cases must follow the maintained
   bounded page/acknowledgement contract. Finite host advancement-limit exhaustion
   means incomplete qualification, not success or permission to raise runtime
   limits.
6. **Keep attributable evidence.** Record selected tests and passed/failed/ignored
   counts, seed coverage, artifact hashes, raw Wasm sizes and available IC
   cycle/instruction measurements. State exclusions and whether charges cover
   whole calls or instrumented bodies. Do not use native timing or extrapolate
   this three-row fixture's cost into production estimates or ceilings.

Verify your framework's synchronous lifecycle and memory-admission integration
separately; see [startup readiness](startup-readiness.md) and
[schema authoring](schema-authoring.md). A local pass proves those exact actors
and seeds, not permission to deploy, full application CI, or backup/restore.
