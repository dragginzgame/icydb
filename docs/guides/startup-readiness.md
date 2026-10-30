# Startup Readiness

Generated IcyDB canisters recover through a replicated startup watchdog after
installation and upgrade. Applications must treat database-dependent timer,
cache, and scheduler restoration as readiness work rather than assuming the
database is immediately available.

Startup obtains IC `raw_rand` entropy before initializing database controls.
While the request is pending or needs retrying, readiness remains `Recovering`
and the existing watchdog retries; applications need no entropy hook or seed.
Admission replaces the persisted cursor key on each new boot while preserving
database identity, rows and pending recovery work. Saved scalar and grouped
cursors expire on upgrade: restart pagination after startup becomes ready.

PocketIC setup must deliver the asynchronous entropy reply and advance virtual
time through the watchdog's one-second retry cadence. Execution ticks alone do
not make that retry due. Use the shared fixture startup helper for ordinary
tests; tests that inspect lifecycle transitions must drive readiness explicitly.

## Declare One Lifecycle Owner

Canisters without application lifecycle hooks use the default declaration:

```rust
canic_icydb::start!(icydb);
```

If the application only needs install or post-upgrade callbacks, declare both
paths through the composed form and leave those functions free of IC-CDK
lifecycle attributes:

```rust,ignore
canic_icydb::start! {
    icydb;
    init(args: InitArgs) => application::init;
    post_upgrade() => application::post_upgrade;
}
```

The callback functions must be reachable from the crate root; callbacks in a
child module therefore normally use `pub(crate)`. IcyDB registers or
reconstructs its watchdog before calling them. The application callback runs
in the lifecycle message, never inside a recovery-page callback. The default
and composed declarations own both lifecycle exports. Application-owned
lifecycle exports require participant mode.

If the application or an independent framework must own the complete IC
lifecycle root, select participant mode and invoke these plain hooks from its
native lifecycle exports. The [compiled composition fixture](../../canisters/test/lifecycle_participant/src/lib.rs)
uses Canic exports and bounded Wire post-upgrade argument decoding. Platform
0.1.10 provides lifecycle entrypoint macros, while IcyDB's supported composition
continues to use Canic as the host owner:

```rust,ignore
canic_icydb::start!(icydb; participant);

fn application_init() {
    restore_ingress_local_state();
    crate::__icydb_lifecycle_participant::init();
    reconcile_other_synchronous_framework_state();
    schedule_deferred_database_readiness_work();
}

fn application_post_upgrade() {
    restore_ingress_local_state();
    crate::__icydb_lifecycle_participant::post_upgrade();
    reconcile_other_synchronous_framework_state();
    schedule_deferred_database_readiness_work();
}
```

The root must call the matching participant synchronously in both lifecycle
phases, including while the application is inactive, `Prepared`, or not yet
activated. Restore local state needed to authorize ingress first; invoke IcyDB
before scheduling any deferred database work; finish other synchronous
framework reconciliation before the lifecycle message returns. A participant
trap aborts the complete lifecycle message normally.

The two participant functions are hidden, crate-visible Rust functions with no
IC export. Calls after a completed participant are inert, but this is
duplicate safety rather than an invitation to call twice. The IC cannot prove
that the first in-crate call came from the correct lifecycle phase, so the
application-owned root remains responsible for exact phase placement.

Composed lifecycle hooks use bounded Wire requests. Encode each hook's argument
tuple, including `()`, with `canic_icydb::wire::encode_init_arguments` or
`canic_icydb::wire::encode_post_upgrade_arguments`, passing
`icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)` as the second argument.
Supply the resulting raw install or upgrade bytes. The plain `start!()` lifecycle
roots perform no decoding.

Do not use participant mode merely to attach ordinary application callbacks;
the composed form above is smaller for that case. Do not claim framework
composition unless that framework exposes a synchronous root or participant
seam with this ordering. A deferred framework callback is insufficient because
a later message can run after application work or roll back independently.

## Poll Typed Readiness

Each lifecycle callback should inspect the generated root-level
`startup_state()` control. It is a bounded Rust function, not an automatically
exported endpoint:

```rust,ignore
match startup_state() {
    Ok(icydb::db::DatabaseStartupState::Ready) => restore_application_state(),
    Ok(icydb::db::DatabaseStartupState::Recovering) => schedule_readiness_poll(),
    Err(failure) => retain_and_expose_startup_failure(failure),
}
```

A readiness poll may use an application-owned bounded retry timer. Every retry
must inspect `startup_state()` again; a fixed delay by itself is not readiness.
Restore database-dependent application state exactly once after observing
`Ready`. On `StartupFailure`, stop blind retries and retain the bounded kind,
diagnostic, and facts for an application-authorized operational surface.

Deterministic migration-planning rejections, such as an unadopted source or a
version gap, stop the generated startup driver through its existing failure
receipt. They retain their specific migration diagnostic and facts. Correct the
deployment instead of repeatedly polling the same rejected plan. A valid
migration already in progress still uses the explicit controller lane below.
Retryable schema errors are returned to the adapter for its existing logged
retry cadence; they do not request an immediate progress callback.

For example, `E274` reports a memory bucket-size conflict with requested
(`Expected`) and persisted (`Actual`) page counts. It requires configuration
correction or explicit recreation, not another recovery poll. See
[canister memory profiles](schema-authoring.md#canister-memory-profiles).

Memory bootstrap and host-adoption rejection use the same bounded diagnostic
payload in `startup_state()`, ordinary `db!()` calls, and startup failure
observation. `StartupFailure::kind()` is `DatabaseControl`; `error()`,
`diagnostic()` and `facts()` retain the specific code and available numeric
parameters. Match the typed leaf code, not its broad class or display text.

| Code | Rejected boundary | Numeric evidence and correction |
| --- | --- | --- |
| E276 | Logical allocation resolution or host grant | Optional `ActualMemoryId`; check the authority and eligible free slots. Fresh allocations need an explicit namespace grant. |
| E277 | Historical IcyDB namespace removed | Restore the permanent namespace and its three controls. |
| E278 | Incomplete allocation roles | `ExpectedCount` is 3 for database controls or 4 for a store; restore the complete role set. |
| E279 | Invalid declarations | Check current role/key grammar, logical requests and namespace authority. |
| E280 | Declaration snapshot or adoption requirements differ | Reconcile current declarations, pool and policy with committed host authority. |
| E281 | Historical journal allocation unavailable | Optional `ActualMemoryId`; restore its current host authority and resolve the rejected historical allocation lifecycle. |

The declaration/admission owners still reject these failures before IcyDB opens
ordinary database work. Startup observation does not grant memory or repair a
rejected declaration. Unknown upstream variants and unrelated runtime
internal faults retain E23, with runtime origin. No unbounded upstream error text,
namespace, authority or stable-key strings enter the public error record.

The host grants permanent namespaces and excludes unmanaged physical IDs in
one checked allocation pool. Supply it before cold startup seals the linked
declaration snapshot:

```rust,ignore
fn icydb_memory_pool() -> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new("icydb.app", "icydb.app.")?],
        vec![], // Explicitly exclude physical IDs held by unmanaged host users.
    )
}
```

The final host grants namespaces through one checked allocation pool. All owners
share eligible free IDs; unmanaged physical memory requires explicit exclusions.
The provider is not invoked when adopting an already committed host. Keep the
same keys and bucket geometry during upgrades; existing assignments remain.
See [compact diagnostics](diagnostics.md) for offline code
and fact interpretation.

A journaled store whose registered Rust path differs from its verified accepted
catalog is a deployment mismatch. The recovery driver returns
`RUNTIME_UNSUPPORTED` before replay, without a durable terminal failure receipt;
startup observation remains `Recovering`. Restore the original store path to
resume recovery. Repeated polling cannot correct that configuration. This guard
does not clear failure receipts already recorded by an older deployment.

Do not match `RUNTIME_CONFLICT`, error text, or a guessed delay. Ordinary
database calls return the dedicated retryable
`RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING` diagnostic while recovery
is incomplete, but that error is admission feedback rather than the
application readiness control.

IcyDB persists neither application callbacks nor application timer policy.
Application timers are lost on upgrade, so the composed callback or
application-owned participant root must recreate readiness polling before any
scheduler restoration can occur.

Before becoming quiescent, the existing replicated watchdog also prepares the
accepted-schema runtime root in heap. This avoids repeating that preparation
in query-only traffic after the callback has completed. It uses the current
accepted snapshots and cache owner, not generated-model reconstruction.
It also restores the schema store's verified bundle and cardinality projections
when the runtime root is already current. Folding may invalidate those store
projections independently, so a runtime-cache hit alone cannot finish preparation.
Deterministic preparation failures use the existing schema-failure receipt;
retryable errors retain their existing classification.

Accepted schema publication re-arms this watchdog for both heap and journaled
stores. Heap-only DDL therefore schedules replicated runtime preparation even
without journal debt. A completed heap metadata migration schedules generated
schema reconciliation through the same callback; it needs no second upgrade.

`Ready` remains the recovery/schema-admission condition, not a promise that
every cache is warm: it can be observed before that callback, and later schema
publication invalidates cached metadata. There is no new endpoint, timer, or
readiness state. The [rename measurement](../design/0.261-entity-rename/c37-runtime-preparation.md)
separates query instruction savings from the replicated preparation cost.

## Explicit Schema Migration

`Recovering` can also mean that the accepted migration authority requires the
existing explicit controller lane. A migration-capable application should:

1. inspect the authorized `icydb_schema_migration` status;
2. let the controller adopt or advance the exact current migration plan;
3. continue bounded readiness polling while that status is non-terminal; and
4. restore application state only after migration completes and
   `startup_state()` returns `Ready`.

The application must not add an automatic migration runner or a second
recovery executor. See [schema-migrations.md](schema-migrations.md) for the
controller workflow.

## Native Tests Must Advance Convergence

Native libtests do not run IC lifecycle hooks or timer callbacks. Generated
canister crates expose the test-only Rust helper
`crate::__icydb_generated::__drive_native_database_for_tests()` to drive the
same startup and online-convergence owner, then check ordinary admission.
Call it before the first database access and periodically **between simulated
messages**, not just once during setup. Wrap each simulated message in
`icydb::db::with_request_execution` so requests retain their normal budgets.
Use plain `#[test]` for this multi-message simulation. Do not enclose it in
`#[icydb::test]` or one outer request wrapper: nested wrappers reuse the
active budget, so the whole simulation would count as one request.

The maintained native fixture advances the driver every sixteen writes and
after its final write, and verifies 129 committed rows. Sixteen is a fixture
cadence, not a production configuration or guarantee for arbitrary payloads.
The 64-batch backlog limit and other production bounds remain active: do not
raise them to make a long test pass. Driver errors must fail the test rather
than being ignored. The helper does not reset stored data, register a second
scheduler, or simulate IC timing; use canister integration tests for timer and
lifecycle behavior. It is available only in native test builds, not as an
endpoint or a downstream integration-test API for a prebuilt dependency.

Generated stores, bootstrap state and the bound memory runtime are thread-local
in native execution. Separate native test threads do not share a generated
database. For multiple independent fixtures inside one test, run each fixture
on a fresh joined thread, keeping its database setup and operations there.
Repeated driver calls on the same thread intentionally retain its rows; advancing
convergence is not a request to clear storage. A global mutex or an in-place
reset of the runtime and its live exclusive memory handles is unnecessary for
thread isolation. Tests that explicitly share another resource still own that
resource's isolation. The [generated facade fixture](../../testing/model-facade-only/src/lib.rs)
qualifies fresh worker databases and preservation of the caller's rows.

## Readiness And Schema Receipt Retention

The exact terminal receipt for the current generated submission proves that
source was reconciled. Receipt-capacity turnover preserves that record while
recycling other eligible terminal records; Pending records are also ineligible.
If no eligible terminal receipt remains, a new submission returns the existing
capacity conflict before publication. Exact submission replay needs no new slot.

Generated canister wiring supplies the receipt key through `CanisterKind` and
the selected database's existing commit context. Key derivation uses the current
incarnation, store authority and immutable generated submission; it runs only
when retention needs a victim. A generated-source change updates that owner
without replacing the physical control-memory lease. Handwritten
`CanisterKind` implementations that use generated-receipt readiness must supply
that same read-only hook; its default applies to canisters without generated
readiness. This adds no persisted readiness record or compatibility reader.
It prevents future eviction; it does not reconstruct a receipt already lost
under an earlier binary or silently reapply generated source to repair one.

## Exposure And Authorization

IcyDB exports no startup-status or startup-failure endpoint automatically.
Applications decide whether to expose readiness, who may call that endpoint,
and whether the compact typed failure belongs in an operational report. Normal
queries, writes, schema operations, integrity checks, and database diagnostics
remain gated while startup is incomplete.

For composed hosts, collect the generated `icydb_memory_requests()` together
with other component requests before sealing Memory’s explicit snapshot. Keep
`prepare_memory_bootstrap` in the host policy’s preparation hook. Generated
IcyDB startup checks its own requirements against the committed host once; it
does not register keys, invoke the pool provider or bootstrap the host again.

## Host entropy admission

The Canic adapter acquires a secure 32-byte boot seed from a replicated timer
callback and submits it to `icydb::db::provide_boot_entropy`. Startup remains
pending until admission succeeds. Identical submissions are idempotent; zero
seeds and rotation during one boot are refused. The adapter retries failed or
cancelled acquisition without duplicating pending requests. Native execution
continues to use operating-system entropy. Custom hosts must supply the same
secure seed contract before driving readiness.

Use `canic_icydb::start!(icydb)` for standalone lifecycle ownership, or its
participant form under an application-owned lifecycle. This adapter does not
mount Canic's managed runtime; full managed memory composition remains tracked
in [Canic #590](https://github.com/dragginzgame/canic/issues/590).
