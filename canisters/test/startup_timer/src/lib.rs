//! Real-canister timer semantics probe for startup-recovery design evidence.

use candid::CandidType;
use ic_cdk::{query, update};
use ic_cdk_timers::TimerId;
use std::{
    cell::{Cell, RefCell},
    time::Duration,
};

const WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);
const WATCHDOG_INTERVAL_NANOS: u64 = 1_000_000_000;
const MAX_RECORDED_CALLBACKS: usize = 512;

thread_local! {
    static WATCHDOG_TIMER: RefCell<Option<TimerId>> = const { RefCell::new(None) };
    static APPLICATION_TIMER: RefCell<Option<TimerId>> = const { RefCell::new(None) };
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

    let watchdog = ic_cdk_timers::set_timer_interval_serial(WATCHDOG_INTERVAL, async || {
        let should_trap = TRAP_AT.with(|trap_at| trap_at.get() == Some(ic_cdk::api::time()));
        if should_trap {
            ic_cdk::trap("0.225 watchdog trap probe");
        }
        let should_exhaust = EXHAUST_AT.with(|at| at.get() == Some(ic_cdk::api::time()));
        if should_exhaust {
            exhaust_message_instructions();
        }
        record_callback(&WATCHDOG_CALLBACKS, &WATCHDOG_INSTRUCTIONS, &WATCHDOG_TIMES);
    });
    WATCHDOG_TIMER.with_borrow_mut(|slot| *slot = Some(watchdog));

    let application = ic_cdk_timers::set_timer_interval_serial(WATCHDOG_INTERVAL, async || {
        record_callback(
            &APPLICATION_CALLBACKS,
            &APPLICATION_INSTRUCTIONS,
            &APPLICATION_TIMES,
        );
    });
    APPLICATION_TIMER.with_borrow_mut(|slot| *slot = Some(application));
    true
}

#[update]
fn startup_timer_probe_stop() {
    WATCHDOG_TIMER.with_borrow_mut(|slot| {
        if let Some(timer) = slot.take() {
            ic_cdk_timers::clear_timer(timer);
        }
    });
    APPLICATION_TIMER.with_borrow_mut(|slot| {
        if let Some(timer) = slot.take() {
            ic_cdk_timers::clear_timer(timer);
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
        watchdog_registered: WATCHDOG_TIMER.with_borrow(Option::is_some),
        application_registered: APPLICATION_TIMER.with_borrow(Option::is_some),
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

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
