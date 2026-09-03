//! Framework-neutral lifecycle-participant composition evidence canister.

use std::{cell::RefCell, time::Duration};

use candid::CandidType;
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

icydb::start!(participant);

const APPLICATION_STARTUP_RETRY: Duration = Duration::from_secs(1);

#[derive(CandidType, Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleHook {
    Init,
    PostUpgrade,
}

#[derive(CandidType, Clone, Copy, Debug, Eq, PartialEq)]
enum ApplicationActivationState {
    Prepared,
    Active,
}

#[derive(CandidType, Clone, Debug)]
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
            ic_cdk::trap("lifecycle composition state is already mutably borrowed");
        };
        update(&mut snapshot);
    });
}

fn begin_lifecycle(hook: LifecycleHook) {
    COMPOSITION.with(|slot| {
        let Ok(mut snapshot) = slot.try_borrow_mut() else {
            ic_cdk::trap("lifecycle composition state is already mutably borrowed");
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
        ic_cdk::trap("IcyDB startup watchdog identity is invalid");
    };
    match ic_timers::timer_snapshot(&identity) {
        Ok(Some(snapshot)) => snapshot.next_deadline_ns().is_some(),
        Ok(None) => false,
        Err(_) => ic_cdk::trap("IcyDB startup watchdog observation failed"),
    }
}

fn application_timer_identity() -> ic_timers::TimerIdentity {
    match ic_timers::TimerIdentity::try_new("application", "startup", "readiness") {
        Ok(identity) => identity,
        Err(_) => ic_cdk::trap("lifecycle composition timer identity is invalid"),
    }
}

fn schedule_deferred_database_work() {
    let Ok(registration) = ic_timers::register_once(
        application_timer_identity(),
        ic_timers::DeclarationLifetime::Retained,
        |_context| async { run_deferred_database_work() },
    ) else {
        ic_cdk::trap("lifecycle composition timer registration failed");
    };
    if registration
        .ensure_scheduled(ic_timers::TimerSchedule::After(APPLICATION_STARTUP_RETRY))
        .is_err()
    {
        ic_cdk::trap("lifecycle composition timer scheduling failed");
    }
    DEFERRED_REGISTRATION.with(|slot| {
        let Ok(mut retained) = slot.try_borrow_mut() else {
            ic_cdk::trap("lifecycle composition timer state is already mutably borrowed");
        };
        *retained = Some(registration);
    });
    with_composition_mut(|snapshot| {
        snapshot.deferred_schedule_order = 4;
    });
}

fn increment(counter: &mut u32, context: &str) {
    let Some(next) = counter.checked_add(1) else {
        ic_cdk::trap(context);
    };
    *counter = next;
}

fn run_deferred_database_work() -> ic_timers::TimerRunResult {
    with_composition_mut(|snapshot| {
        increment(
            &mut snapshot.deferred_runs,
            "lifecycle composition deferred-run count overflowed",
        );
    });

    match startup_state() {
        Ok(icydb::db::DatabaseStartupState::Recovering) => ic_timers::TimerRunResult::new(
            ic_timers::TimerCompletion::retryable_failure(0),
            ic_timers::TimerDirective::RetryAfter(APPLICATION_STARTUP_RETRY),
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
                    ic_timers::TimerRunResult::new(
                        ic_timers::TimerCompletion::success(1),
                        ic_timers::TimerDirective::Stop,
                    )
                }
                Err(error) => {
                    with_composition_mut(|snapshot| snapshot.database_failure = Some(error));
                    ic_timers::TimerRunResult::new(
                        ic_timers::TimerCompletion::invariant_failure(0),
                        ic_timers::TimerDirective::Stop,
                    )
                }
            }
        }
        Err(failure) => {
            with_composition_mut(|snapshot| snapshot.startup_failure = Some(failure));
            ic_timers::TimerRunResult::new(
                ic_timers::TimerCompletion::invariant_failure(0),
                ic_timers::TimerDirective::Stop,
            )
        }
    }
}

#[ic_cdk::init]
fn application_init() {
    begin_lifecycle(LifecycleHook::Init);
    let start = ic_cdk::api::performance_counter(1);
    crate::__icydb_lifecycle_participant::init();
    let participant_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    complete_synchronous_lifecycle(participant_instructions);
}

#[ic_cdk::post_upgrade]
fn application_post_upgrade(trap_after_participant: Option<bool>) {
    begin_lifecycle(LifecycleHook::PostUpgrade);
    let start = ic_cdk::api::performance_counter(1);
    crate::__icydb_lifecycle_participant::post_upgrade();
    let participant_instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    if trap_after_participant.unwrap_or(false) {
        ic_cdk::trap("lifecycle participant post-upgrade rollback probe");
    }
    complete_synchronous_lifecycle(participant_instructions);
}

#[ic_cdk::query]
fn lifecycle_composition_snapshot() -> LifecycleCompositionSnapshot {
    COMPOSITION.with(|slot| {
        let Ok(snapshot) = slot.try_borrow() else {
            ic_cdk::trap("lifecycle composition state is mutably borrowed");
        };
        snapshot.clone()
    })
}

#[ic_cdk::query]
fn lifecycle_database_probe() -> Result<bool, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let _database = db()?;
        Ok(true)
    })
}

#[ic_cdk::update]
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
            ic_cdk::trap("lifecycle probe insert omitted its identity column");
        };
        let Some(value) = output.rows.first().and_then(|row| row.get(id_slot)) else {
            ic_cdk::trap("lifecycle probe insert omitted its generated identity");
        };
        let PublicValue::Ulid(id) = value.as_public() else {
            ic_cdk::trap("lifecycle probe insert omitted its generated identity");
        };
        Ok(*id)
    })
}

#[ic_cdk::query]
fn lifecycle_probe_row_exists(id: Ulid) -> Result<bool, icydb::Error> {
    icydb::db::with_request_execution(|| {
        let database = db()?;
        match database.get::<OneSimpleEntity01>(Id::from_key(id)) {
            Ok(row) => Ok(row.is_some()),
            Err(TypedOperationError::Database(error)) => Err(error),
            Err(TypedOperationError::Adapter(_)) => {
                ic_cdk::trap("lifecycle probe row failed accepted-model decoding")
            }
        }
    })
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
