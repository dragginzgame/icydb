//! Real-canister timer semantics probe for startup-recovery design evidence.

use candid::CandidType;
use ic_cdk::{query, update};
use ic_timers::{
    AfterCompletionRegistration, DeclarationLifetime, TimerCadence, TimerCompletion,
    TimerDirective, TimerIdentity, TimerRunResult, WatchdogDecision, WatchdogRegistration,
    WatchdogRunResult, initialize_runtime, register_after_completion, register_watchdog,
    timer_snapshot,
};
use std::{
    cell::{Cell, RefCell},
    time::Duration,
};

const WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);
const WATCHDOG_INTERVAL_NANOS: u64 = 1_000_000_000;
const MAX_RECORDED_CALLBACKS: usize = 512;

thread_local! {
    static WATCHDOG_TIMER: RefCell<Option<WatchdogRegistration>> = const { RefCell::new(None) };
    static APPLICATION_TIMER: RefCell<Option<AfterCompletionRegistration>> = const { RefCell::new(None) };
    static WATCHDOG_CALLBACKS: Cell<u32> = const { Cell::new(0) };
    static APPLICATION_CALLBACKS: Cell<u32> = const { Cell::new(0) };
    static WATCHDOG_INSTRUCTIONS: Cell<u64> = const { Cell::new(0) };
    static APPLICATION_INSTRUCTIONS: Cell<u64> = const { Cell::new(0) };
    static WATCHDOG_TIMES: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
    static APPLICATION_TIMES: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
    static TRAP_AT: Cell<Option<u64>> = const { Cell::new(None) };
    static EXHAUST_AT: Cell<Option<u64>> = const { Cell::new(None) };
}

#[derive(CandidType, Clone, Debug, Eq, PartialEq)]
struct TimerProbeSnapshot {
    watchdog_registered: bool,
    application_registered: bool,
    watchdog_callbacks: u32,
    application_callbacks: u32,
    watchdog_instructions: u64,
    application_instructions: u64,
    watchdog_times: Vec<u64>,
    application_times: Vec<u64>,
    trap_at: Option<u64>,
    exhaust_at: Option<u64>,
}

#[inline(never)]
fn exhaust_message_instructions() -> ! {
    loop {
        std::hint::black_box(ic_cdk::api::performance_counter(0));
    }
}

fn record_callback(
    callbacks: &'static std::thread::LocalKey<Cell<u32>>,
    instructions: &'static std::thread::LocalKey<Cell<u64>>,
    times: &'static std::thread::LocalKey<RefCell<Vec<u64>>>,
) {
    let start = ic_cdk::api::performance_counter(1);
    callbacks.with(|count| count.set(count.get().saturating_add(1)));
    times.with_borrow_mut(|observed| {
        if observed.len() < MAX_RECORDED_CALLBACKS {
            observed.push(ic_cdk::api::time());
        }
    });
    instructions.with(|count| {
        count.set(
            ic_cdk::api::performance_counter(1)
                .saturating_sub(start)
                .saturating_add(count.get()),
        );
    });
}

#[update]
fn startup_timer_probe_start() -> bool {
    if WATCHDOG_TIMER.with_borrow(Option::is_some) {
        return false;
    }
    if initialize_runtime().is_err() {
        ic_cdk::trap("startup timer probe runtime initialization failed");
    }
    let cadence = timer_cadence();

    let Ok(watchdog) = register_watchdog(
        watchdog_identity(),
        cadence,
        DeclarationLifetime::Retained,
        |_context| {
            let now = ic_cdk::api::time();
            let should_trap = TRAP_AT.with(|trap_at| {
                trap_at.get().is_some_and(|start| {
                    now >= start && now < start.saturating_add(WATCHDOG_INTERVAL_NANOS)
                })
            });
            if should_trap {
                ic_cdk::trap("startup watchdog trap probe");
            }
            let should_exhaust = EXHAUST_AT.with(|at| {
                at.get().is_some_and(|start| {
                    now >= start && now < start.saturating_add(WATCHDOG_INTERVAL_NANOS)
                })
            });
            if should_exhaust {
                exhaust_message_instructions();
            }
            record_callback(&WATCHDOG_CALLBACKS, &WATCHDOG_INSTRUCTIONS, &WATCHDOG_TIMES);
            WatchdogRunResult::new(TimerCompletion::success(1), WatchdogDecision::Continue)
        },
    ) else {
        ic_cdk::trap("startup watchdog registration failed");
    };
    if watchdog.ensure_scheduled().is_err() {
        ic_cdk::trap("startup watchdog scheduling failed");
    }
    WATCHDOG_TIMER.with_borrow_mut(|slot| *slot = Some(watchdog));

    let Ok(application) = register_after_completion(
        application_identity(),
        cadence,
        DeclarationLifetime::Retained,
        |_context| async {
            record_callback(
                &APPLICATION_CALLBACKS,
                &APPLICATION_INSTRUCTIONS,
                &APPLICATION_TIMES,
            );
            TimerRunResult::new(
                TimerCompletion::success(1),
                TimerDirective::RecurAfterCompletion,
            )
        },
    ) else {
        ic_cdk::trap("application timer registration failed");
    };
    if application.ensure_scheduled().is_err() {
        ic_cdk::trap("application timer scheduling failed");
    }
    APPLICATION_TIMER.with_borrow_mut(|slot| *slot = Some(application));
    true
}

#[update]
fn startup_timer_probe_stop() {
    WATCHDOG_TIMER.with_borrow_mut(|slot| {
        if let Some(registration) = slot.take()
            && registration.unregister().is_err()
        {
            ic_cdk::trap("startup watchdog removal failed");
        }
    });
    APPLICATION_TIMER.with_borrow_mut(|slot| {
        if let Some(registration) = slot.take()
            && registration.unregister().is_err()
        {
            ic_cdk::trap("application timer removal failed");
        }
    });
}

#[update]
fn startup_timer_probe_arm_trap() -> u64 {
    let trap_at = ic_cdk::api::time().saturating_add(WATCHDOG_INTERVAL_NANOS);
    TRAP_AT.with(|slot| slot.set(Some(trap_at)));
    trap_at
}

#[update]
fn startup_timer_probe_arm_exhaustion() -> u64 {
    let exhaust_at = ic_cdk::api::time().saturating_add(WATCHDOG_INTERVAL_NANOS);
    EXHAUST_AT.with(|slot| slot.set(Some(exhaust_at)));
    exhaust_at
}

#[query]
fn startup_timer_probe_snapshot() -> TimerProbeSnapshot {
    TimerProbeSnapshot {
        watchdog_registered: timer_is_scheduled(&watchdog_identity()),
        application_registered: timer_is_scheduled(&application_identity()),
        watchdog_callbacks: WATCHDOG_CALLBACKS.with(Cell::get),
        application_callbacks: APPLICATION_CALLBACKS.with(Cell::get),
        watchdog_instructions: WATCHDOG_INSTRUCTIONS.with(Cell::get),
        application_instructions: APPLICATION_INSTRUCTIONS.with(Cell::get),
        watchdog_times: WATCHDOG_TIMES.with_borrow(Clone::clone),
        application_times: APPLICATION_TIMES.with_borrow(Clone::clone),
        trap_at: TRAP_AT.with(Cell::get),
        exhaust_at: EXHAUST_AT.with(Cell::get),
    }
}

fn timer_cadence() -> TimerCadence {
    match TimerCadence::new(WATCHDOG_INTERVAL) {
        Ok(cadence) => cadence,
        Err(_) => ic_cdk::trap("startup timer probe cadence is invalid"),
    }
}

fn watchdog_identity() -> TimerIdentity {
    match TimerIdentity::try_new("icydb", "startup-probe", "watchdog") {
        Ok(identity) => identity,
        Err(_) => ic_cdk::trap("startup watchdog identity is invalid"),
    }
}

fn application_identity() -> TimerIdentity {
    match TimerIdentity::try_new("icydb", "startup-probe", "application") {
        Ok(identity) => identity,
        Err(_) => ic_cdk::trap("application timer identity is invalid"),
    }
}

fn timer_is_scheduled(identity: &TimerIdentity) -> bool {
    timer_snapshot(identity)
        .ok()
        .flatten()
        .and_then(|snapshot| snapshot.next_deadline_ns())
        .is_some()
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
