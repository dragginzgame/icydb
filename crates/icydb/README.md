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

## Startup in native canister tests

Enable `testing` in the test application's native dependency on `icydb` to use
`icydb::testing::drive_startup`. Pass Testkit's `PocketIcInstance` lifecycle owner, a maximum
round count, and an async closure that observes the generated `startup_state()` through
your application's authorized read-only endpoint. The observation returns
`Result<DatabaseStartupState, E>`: `Ready` completes, `Recovering` permits another
round, and errors retain their original typed value. An ordinary-work endpoint
that returns a startup-pending error is not a readiness observation.

```rust,ignore
use icydb::testing::drive_startup;

// Application transport decodes the existing DatabaseStartupState and preserves
// both transport failures and the existing StartupFailure as typed errors.
let pic = instance.instance();
drive_startup(&instance, 60, async || {
    observe_database_startup(pic, canister_id).await
}).await?;
```

The same helper works after installation and upgrade. Testkit checks readiness
before progression, then advances simulated time by one second and ticks once
per pending round. A zero budget still checks once, and readiness on the last
allowed round succeeds. Exhaustion is `TickUntilError::ProgressLimit`; observation
failures are `Failed`. Upstream PocketIC transport panics propagate. Do not retry business writes
inside the observation closure. Select the round bound for the fixture's workload;
this helper does not declare a universal recovery deadline.

The application owns the observation endpoint and its authorization. Enabling
`testing` exports no IC method, starts no server and registers no watchdog.
The caller owns the async runtime and explicitly closes its Testkit instance
with `instance.close().await` after using it. Testkit remains the provisioning,
lifecycle and polling owner. The optional
dependency is native-only and absent from ordinary runtime/Wasm graphs. Existing
private integration fixtures retain their separately qualified Canic/Testkit
stack until that composition is updated.

For full setup, examples, and release notes:

- Workspace README: `../../README.md`
- [Dependency and installation guide](../../INSTALLING.md)
- Changelog: `../../CHANGELOG.md`
