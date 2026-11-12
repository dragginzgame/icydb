# IcyDB

Public runtime facade for IcyDB.

This crate exposes accepted-schema database sessions, structural writes,
optional SQL and typed reads, shared runtime types, and the host build facade
used by generated canister actor glue.

Public IC endpoint exports belong to the separate `canic-icydb` adapter, with
explicit declarations and IC Wire payloads. This library can be used without
mounting endpoints; enabling a feature does not export a public method.

Ordinary database-owning canisters depend on `icydb` alone. Author declarations
through `icydb::model::prelude::*` and use this facade's runtime, scalar types
and build surfaces. The runtime-only `#[icydb::request_execution]` and
`#[icydb::test]` entry attributes are exposed directly.

Lightweight model or shared-contract crates may instead depend on `icydb-model`
alone: declarations use `icydb_model::prelude::*`, and schema-owned scalar
types are available through re-exports such as `icydb_model::schema::Ulid`.
Scalar/proposal-only tooling may use `icydb-schema` alone when model macros
are unnecessary. These are supported narrower dependency choices; disabling
the facade's default features still includes the database runtime.

Database writes use only the accepted schema snapshot for admission. They do
not invoke `icydb-model` application validators or normalizers. Applications
that want those Rust behaviors must call `normalize`, `validate`, or
`normalize_and_validate` explicitly before constructing a write.

## Filter literals

Filter builders accept `Into<icydb::value::InputValue>`. Typed scalars retain
their kind; construct lists, maps and enum payloads with `InputValue` as well.
Enum identity is admitted against the accepted schema. Serialized filters use
PublicValue's representation; clients must regenerate requests from the retired
FilterValue contract rather than reuse its serialized payloads.

## Startup in native canister tests

Enable `testing` in the test application's native dependency on `icydb` to use
`icydb::testing::startup_poll`. It projects an authorized read-only observation
returning `Result<DatabaseStartupState, E>`: `Ready` completes, `Recovering` permits
another round, and errors retain their original typed value. An ordinary-work
endpoint that returns a startup-pending error is not a readiness observation.

Select Testkit separately in the native test harness. Compose its existing
bounded driver with the typed predicate:

```rust,ignore
use std::time::Duration;

use ic_testkit::pic::tick_until;
use icydb::testing::startup_poll;

// Keep the PocketIcInstance owner alive throughout polling. Application transport
// preserves both transport failures and StartupFailure as typed errors.
let pic = instance.instance();
tick_until(pic, 60, Duration::from_secs(1), async |_| {
    startup_poll(observe_database_startup(pic, canister_id).await)
}).await?;
```

This replaces the previous Testkit-bound `drive_startup` wrapper and its
`PocketIcInstance`/`TickUntilError` re-exports. Import those types from
`ic_testkit::pic`; use `ic_testkit::pocket_ic` for the selected upstream client.
No separate PocketIC dependency or IcyDB-specific simulator adapter is needed.
The [native qualification fixture](../../qualification/startup/tests/startup_polling.rs)
compiles and exercises this composition.

The same composition works after installation and upgrade. Testkit checks
readiness before progression, then advances simulated time by one second and
ticks once per pending round. A zero budget still checks once, and readiness on
the last allowed round succeeds. Exhaustion is Testkit's
`TickUntilError::ProgressLimit`; observation failures are `Failed`. Upstream
PocketIC transport panics propagate. Never retry business writes through the
observation closure. Select the round bound for the fixture's workload.

The application owns the observation endpoint, authorization and async runtime.
Keep the Testkit instance owner alive throughout polling and explicitly close it
with `instance.close().await`. Testkit retains provisioning, lifecycle and polling
ownership. IcyDB's `testing` feature has no simulator dependency and exports no IC
method, starts no server and registers no watchdog. Private Canic integration
fixtures retain their separately qualified stack until that composition is updated.

For full setup, examples, and release notes:

- Workspace README: `../../README.md`
- [Dependency and installation guide](../../INSTALLING.md)
- Changelog: `../../CHANGELOG.md`
