# Startup Readiness

Generated IcyDB canisters recover through a replicated startup watchdog after
installation and upgrade. Applications must treat database-dependent timer,
cache, and scheduler restoration as readiness work rather than assuming the
database is immediately available.

## Declare One Lifecycle Owner

Canisters without application lifecycle hooks use the default declaration:

```rust
icydb::start!();
```

If the application only needs install or post-upgrade callbacks, declare both
paths through the composed form and leave those functions free of IC-CDK
lifecycle attributes:

```rust,ignore
icydb::start! {
    init(args: InitArgs) => application::init;
    post_upgrade() => application::post_upgrade;
}
```

The callback functions must be reachable from the crate root; callbacks in a
child module therefore normally use `pub(crate)`. IcyDB registers or
reconstructs its watchdog before calling them. The application callback runs
in the lifecycle message, never inside a recovery-page callback. The old
combination of `icydb::start!()` with separate `#[ic_cdk::init]` or
`#[ic_cdk::post_upgrade]` exports is removed by the pre-1.0 hard cut because it
creates duplicate lifecycle exports.

If the application or an independent framework must own the complete IC
lifecycle root, select participant mode instead:

```rust,ignore
icydb::start!(participant);

#[ic_cdk::init]
fn application_init() {
    restore_ingress_local_state();
    crate::__icydb_lifecycle_participant::init();
    reconcile_other_synchronous_framework_state();
    schedule_deferred_database_readiness_work();
}

#[ic_cdk::post_upgrade]
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

The two participant functions are hidden crate-visible Rust functions, not IC
or Candid methods. Calls after a completed participant are inert, but this is
duplicate safety rather than an invitation to call twice. The IC cannot prove
that the first in-crate call came from the correct lifecycle phase, so the
application-owned root remains responsible for exact phase placement.

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

Do not match `RUNTIME_CONFLICT`, error text, or a guessed delay. Ordinary
database calls return the dedicated retryable
`RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING` diagnostic while recovery
is incomplete, but that error is admission feedback rather than the
application readiness control.

IcyDB persists neither application callbacks nor application timer policy.
Application timers are lost on upgrade, so the composed callback or
application-owned participant root must recreate readiness polling before any
scheduler restoration can occur.

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

## Exposure And Authorization

IcyDB exports no startup-status or startup-failure endpoint automatically.
Applications decide whether to expose readiness, who may call that endpoint,
and whether the compact typed failure belongs in an operational report. Normal
queries, writes, schema operations, integrity checks, and database diagnostics
remain gated while startup is incomplete.
