//! Framework-neutral lifecycle-participant composition evidence canister.

// IC counter 1 preserves the call-context sample boundary through Platform.
// Native system calls provide no IC measurement evidence.
use ic_platform::ic0::performance_counter;

use std::{cell::RefCell, time::Duration};

use icydb::{
    db::TypedOperationError,
    types::{Id, Ulid},
};
#[cfg(feature = "population-seed")]
use icydb::{
    db::{StructuralPatch, WriteCell},
    value::{InputValue, PublicValue},
};
use icydb_testing_audit_one_simple_fixtures::one_simple::OneSimpleEntity01;

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb.one_simple",
            "icydb.one_simple.",
        )?],
        vec![],
    )
}

canic_icydb::start!(icydb; participant);

const APPLICATION_STARTUP_RETRY: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
enum LifecycleHook {
    Init,
    PostUpgrade,
}

#[derive(Clone, Copy, Debug, serde::Deserialize, Eq, PartialEq, serde::Serialize)]
enum ApplicationActivationState {
    Prepared,
    Active,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct LifecycleCompositionSnapshot {
    hook: LifecycleHook,
    activation: ApplicationActivationState,
    ingress_authority_order: u8,
    participant_order: u8,
    participant_instructions: u64,
    startup_after_participant:
        Option<Result<icydb::db::DatabaseStartupState, icydb::db::StartupFailure>>,
    watchdog_armed_after_participant: bool,
    framework_order: u8,
    deferred_schedule_order: u8,
    deferred_runs: u32,
    database_work_runs: u32,
    startup_failure: Option<icydb::db::StartupFailure>,
    database_failure: Option<icydb::Error>,
}

impl LifecycleCompositionSnapshot {
    const fn new(hook: LifecycleHook) -> Self {
        Self {
            hook,
            activation: ApplicationActivationState::Prepared,
            ingress_authority_order: 0,
            participant_order: 0,
            participant_instructions: 0,
            startup_after_participant: None,
            watchdog_armed_after_participant: false,
            framework_order: 0,
            deferred_schedule_order: 0,
            deferred_runs: 0,
            database_work_runs: 0,
            startup_failure: None,
            database_failure: None,
        }
    }
}

std::thread_local! {
    static COMPOSITION: RefCell<LifecycleCompositionSnapshot> =
        const { RefCell::new(LifecycleCompositionSnapshot::new(LifecycleHook::Init)) };
    static DEFERRED_REGISTRATION: RefCell<Option<ic_timers::OnceRegistration>> =
        const { RefCell::new(None) };
}

fn with_composition_mut(update: impl FnOnce(&mut LifecycleCompositionSnapshot)) {
    COMPOSITION.with(|slot| {
        let Ok(mut snapshot) = slot.try_borrow_mut() else {
            ic_platform::ic0::trap(b"lifecycle composition state is already mutably borrowed");
        };
        update(&mut snapshot);
    });
}

fn begin_lifecycle(hook: LifecycleHook) {
    COMPOSITION.with(|slot| {
        let Ok(mut snapshot) = slot.try_borrow_mut() else {
            ic_platform::ic0::trap(b"lifecycle composition state is already mutably borrowed");
        };
        *snapshot = LifecycleCompositionSnapshot::new(hook);
        snapshot.ingress_authority_order = 1;
    });
}

fn complete_synchronous_lifecycle(participant_instructions: u64) {
    let startup = startup_state();
    let watchdog_armed = engine_startup_watchdog_armed();
    let may_schedule_deferred = startup.is_ok();

    with_composition_mut(|snapshot| {
        snapshot.participant_order = 2;
        snapshot.participant_instructions = participant_instructions;
        snapshot.startup_after_participant = Some(startup);
        snapshot.watchdog_armed_after_participant = watchdog_armed;

        snapshot.framework_order = 3;
    });

    if may_schedule_deferred {
        schedule_deferred_database_work();
    }
}

fn engine_startup_watchdog_armed() -> bool {
    let Ok(identity) = ic_timers::TimerIdentity::try_new("icydb", "startup", "recovery") else {
        ic_platform::ic0::trap(b"IcyDB startup watchdog identity is invalid");
    };
    match ic_timers::timer_snapshot(&identity) {
        Ok(Some(snapshot)) => snapshot.next_deadline_ns().is_some(),
        Ok(None) => false,
        Err(_) => ic_platform::ic0::trap(b"IcyDB startup watchdog observation failed"),
    }
}

fn application_timer_identity() -> ic_timers::TimerIdentity {
    match ic_timers::TimerIdentity::try_new("application", "startup", "readiness") {
        Ok(identity) => identity,
        Err(_) => ic_platform::ic0::trap(b"lifecycle composition timer identity is invalid"),
    }
}

fn schedule_deferred_database_work() {
    let Ok(registration) = ic_timers::register_once(
        application_timer_identity(),
        ic_timers::TimerStart::Retained {
            initial: Some(ic_timers::TimerSchedule::After(APPLICATION_STARTUP_RETRY)),
        },
        |_context| async { run_deferred_database_work() },
    ) else {
        ic_platform::ic0::trap(b"lifecycle composition timer registration failed");
    };
    DEFERRED_REGISTRATION.with(|slot| {
        let Ok(mut retained) = slot.try_borrow_mut() else {
            ic_platform::ic0::trap(
                b"lifecycle composition timer state is already mutably borrowed",
            );
        };
        *retained = Some(registration);
    });
    with_composition_mut(|snapshot| {
        snapshot.deferred_schedule_order = 4;
    });
}

fn increment(counter: &mut u32, context: &str) {
    let Some(next) = counter.checked_add(1) else {
        ic_platform::ic0::trap(context.as_bytes());
    };
    *counter = next;
}

fn run_deferred_database_work() -> ic_timers::OnceRunResult {
    with_composition_mut(|snapshot| {
        increment(
            &mut snapshot.deferred_runs,
            "lifecycle composition deferred-run count overflowed",
        );
    });

    match startup_state() {
        Ok(icydb::db::DatabaseStartupState::Recovering) => ic_timers::OnceRunResult::new(
            ic_timers::TimerCompletion::retryable_failure(0),
            ic_timers::OnceDecision::RetryAfter(APPLICATION_STARTUP_RETRY),
        ),
        Ok(icydb::db::DatabaseStartupState::Ready) => {
            let opened = icydb::db::with_request_execution(|| db().map(|_| ()));
            match opened {
                Ok(()) => {
                    with_composition_mut(|snapshot| {
                        increment(
                            &mut snapshot.database_work_runs,
                            "lifecycle composition database-work count overflowed",
                        );
                        snapshot.activation = ApplicationActivationState::Active;
                    });
                    ic_timers::OnceRunResult::new(
                        ic_timers::TimerCompletion::success(1),
                        ic_timers::OnceDecision::Stop,
                    )
                }
                Err(error) => {
                    with_composition_mut(|snapshot| snapshot.database_failure = Some(error));
                    ic_timers::OnceRunResult::new(
                        ic_timers::TimerCompletion::invariant_failure(0),
                        ic_timers::OnceDecision::Stop,
                    )
                }
            }
        }
        Err(failure) => {
            with_composition_mut(|snapshot| snapshot.startup_failure = Some(failure));
            ic_timers::OnceRunResult::new(
                ic_timers::TimerCompletion::invariant_failure(0),
                ic_timers::OnceDecision::Stop,
            )
        }
    }
}

#[canic_icydb::__private::native_entry(init, name = "", sdk = crate::__icydb_host_sdk)]
fn application_init() {
    begin_lifecycle(LifecycleHook::Init);
    let start = performance_counter(1);
    crate::__icydb_lifecycle_participant::init();
    let participant_instructions = performance_counter(1).saturating_sub(start);
    complete_synchronous_lifecycle(participant_instructions);
}

fn application_post_upgrade(trap_after_participant: Option<bool>) {
    begin_lifecycle(LifecycleHook::PostUpgrade);
    let start = performance_counter(1);
    crate::__icydb_lifecycle_participant::post_upgrade();
    let participant_instructions = performance_counter(1).saturating_sub(start);
    if trap_after_participant.unwrap_or(false) {
        ic_platform::ic0::trap(b"lifecycle participant post-upgrade rollback probe");
    }
    complete_synchronous_lifecycle(participant_instructions);
}

#[canic_icydb::__private::native_entry(post_upgrade, name = "", sdk = crate::__icydb_host_sdk)]
fn __application_post_upgrade_export() {
    let bytes = canic_icydb::read_arguments(icydb::wire::MAX_REQUEST_BYTES)
        .unwrap_or_else(|| ic_platform::ic0::trap(b"lifecycle argument admission failed"));
    let (trap_after_participant,): (Option<bool>,) =
        canic_icydb::wire::decode_post_upgrade_arguments(
            &bytes,
            icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
        )
        .unwrap_or_else(|_| ic_platform::ic0::trap(b"invalid lifecycle Wire arguments"));
    application_post_upgrade(trap_after_participant);
}

fn lifecycle_composition_snapshot() -> LifecycleCompositionSnapshot {
    COMPOSITION.with(|slot| {
        let Ok(snapshot) = slot.try_borrow() else {
            ic_platform::ic0::trap(b"lifecycle composition state is mutably borrowed");
        };
        snapshot.clone()
    })
}
icydb_testing_wasm_helpers::export_wire!(
    query,
    "lifecycle_composition_snapshot",
    LifecycleCompositionSnapshot,
    lifecycle_composition_snapshot,
    (),
    LifecycleCompositionSnapshot
);

fn lifecycle_database_probe() -> Result<bool, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let _database = db()?;
        Ok(true)
    })
}
icydb_testing_wasm_helpers::export_wire!(query, "lifecycle_database_probe", LifecycleDatabaseProbe, lifecycle_database_probe, (), Result<bool, icydb::Error>);

#[cfg(feature = "population-seed")]
fn lifecycle_insert_probe_row() -> Result<Ulid, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let patch = StructuralPatch::new()
            .field(
                "name",
                WriteCell::Value(InputValue::text("lifecycle-probe".to_string())),
            )
            .field("profiles", WriteCell::Value(InputValue::list(Vec::new())));
        let output =
            db()?.execute_trusted_structural_insert_batch("OneSimpleEntity01", vec![patch])?;
        let Some(id_slot) = output.columns.iter().position(|column| column == "id") else {
            ic_platform::ic0::trap(b"lifecycle probe insert omitted its identity column");
        };
        let Some(value) = output.rows.first().and_then(|row| row.get(id_slot)) else {
            ic_platform::ic0::trap(b"lifecycle probe insert omitted its generated identity");
        };
        let PublicValue::Ulid(id) = value.as_public() else {
            ic_platform::ic0::trap(b"lifecycle probe insert omitted its generated identity");
        };
        Ok(*id)
    })
}
#[cfg(feature = "population-seed")]
icydb_testing_wasm_helpers::export_wire!(update, "lifecycle_insert_probe_row", LifecycleInsertProbeRow, lifecycle_insert_probe_row, (), Result<Ulid, icydb::Error>);

fn lifecycle_probe_row_exists(id: Ulid) -> Result<bool, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let database = db()?;
        match database.get::<OneSimpleEntity01>(Id::from_key(id)) {
            Ok(row) => Ok(row.is_some()),
            Err(TypedOperationError::Database(error)) => Err(error),
            Err(TypedOperationError::Adapter(_)) => {
                ic_platform::ic0::trap(b"lifecycle probe row failed accepted-model decoding")
            }
        }
    })
}
icydb_testing_wasm_helpers::export_wire!(query, "lifecycle_probe_row_exists", LifecycleProbeRowExists, lifecycle_probe_row_exists, (id: Ulid), Result<bool, icydb::Error>);
