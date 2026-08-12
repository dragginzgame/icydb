use candid::{CandidType, Deserialize, Principal};
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb_testing_integration::{install_fixture_canister, upgrade_fixture_canister};
use std::time::Duration;

const TIMER_EXECUTOR_METHOD: &str = "<ic-cdk internal> timer_executor";
const ONE_SECOND_NANOS: u64 = 1_000_000_000;

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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

#[test]
fn serial_watchdog_baseline_survives_instruction_exhaustion() {
    let fixture = install_fixture_canister("startup_timer");
    assert!(start(&fixture));

    let exhaust_at: u64 = fixture
        .update_candid("startup_timer_probe_arm_exhaustion", ())
        .expect("instruction-exhaustion arming should decode");
    advance_timer_round(&fixture, Duration::from_secs(1));
    // Production instruction limits use deterministic time slicing. Drive
    // enough rounds for the message-level limit and the independently queued
    // application callback, without advancing IC time.
    for _ in 0..24 {
        fixture.pocket_ic().tick();
    }
    let exhausted = snapshot(&fixture);
    assert_eq!(exhausted.exhaust_at, Some(exhaust_at));
    assert_eq!(exhausted.watchdog_callbacks, 0);
    assert_eq!(exhausted.application_callbacks, 1);

    advance_timer_round(&fixture, Duration::from_secs(1));
    let recovered = snapshot(&fixture);
    assert_eq!(recovered.watchdog_callbacks, 1);
    assert_eq!(recovered.application_callbacks, 2);
    let resumed_at = recovered.watchdog_times[0];
    assert!(resumed_at > exhaust_at);
    assert!(resumed_at <= exhaust_at.saturating_add(ONE_SECOND_NANOS + 100));

    println!(
        "icydb_0225_timer_exhaustion exhaust_at={exhaust_at} resumed_at={resumed_at} next_wakeup_survived=true",
    );
}

fn snapshot(fixture: &StandaloneCanisterFixture) -> TimerProbeSnapshot {
    fixture
        .query_candid("startup_timer_probe_snapshot", ())
        .expect("timer probe snapshot should decode")
}

fn start(fixture: &StandaloneCanisterFixture) -> bool {
    fixture
        .update_candid("startup_timer_probe_start", ())
        .expect("timer probe start should decode")
}

fn advance_timer_round(fixture: &StandaloneCanisterFixture, duration: Duration) {
    fixture.pocket_ic().advance_time(duration);
    fixture.pocket_ic().tick();
    fixture.pocket_ic().tick();
}

#[test]
fn serial_watchdog_baseline_survives_trap_and_upgrade() {
    let fixture = install_fixture_canister("startup_timer");
    assert!(start(&fixture));
    assert!(
        !start(&fixture),
        "one retained TimerId must deduplicate registration"
    );

    let before_external = snapshot(&fixture);
    let external = fixture.pocket_ic().update_call(
        fixture.canister_id(),
        Principal::anonymous(),
        TIMER_EXECUTOR_METHOD,
        0_u64.to_be_bytes().to_vec(),
    );
    assert!(
        external.is_err(),
        "the CDK timer executor must reject external ingress"
    );
    assert_eq!(snapshot(&fixture), before_external);

    let trap_at: u64 = fixture
        .update_candid("startup_timer_probe_arm_trap", ())
        .expect("trap arming should decode");
    advance_timer_round(&fixture, Duration::from_secs(1));
    let trapped = snapshot(&fixture);
    assert_eq!(trapped.trap_at, Some(trap_at));
    assert_eq!(trapped.watchdog_callbacks, 0);
    assert_eq!(trapped.application_callbacks, 1);

    advance_timer_round(&fixture, Duration::from_secs(1));
    let recovered = snapshot(&fixture);
    assert_eq!(recovered.watchdog_callbacks, 1);
    assert_eq!(recovered.application_callbacks, 2);
    let resumed_at = recovered.watchdog_times[0];
    assert!(resumed_at > trap_at);
    assert!(resumed_at <= trap_at.saturating_add(ONE_SECOND_NANOS + 100));
    assert!(recovered.application_times[0] >= trap_at);
    assert!(recovered.application_times[0] <= trap_at.saturating_add(100));
    assert!(recovered.application_times[1] > recovered.application_times[0]);
    assert!(
        recovered.application_times[1]
            <= recovered.application_times[0].saturating_add(ONE_SECOND_NANOS + 100)
    );
    assert!(recovered.watchdog_instructions > 0);
    assert!(recovered.application_instructions > 0);

    upgrade_fixture_canister(&fixture, "startup_timer");
    let upgraded = snapshot(&fixture);
    assert!(!upgraded.watchdog_registered);
    assert!(!upgraded.application_registered);
    assert_eq!(upgraded.watchdog_callbacks, 0);
    assert_eq!(upgraded.application_callbacks, 0);

    assert!(start(&fixture));
    advance_timer_round(&fixture, Duration::from_secs(1));
    let reregistered = snapshot(&fixture);
    assert_eq!(reregistered.watchdog_callbacks, 1);
    assert_eq!(reregistered.application_callbacks, 1);

    fixture
        .update_candid::<(), _>("startup_timer_probe_stop", ())
        .expect("timer probe stop should decode");
    let stopped = snapshot(&fixture);
    assert!(!stopped.watchdog_registered);
    assert!(!stopped.application_registered);

    println!(
        "icydb_0225_timer trap_at={} resumed_at={} watchdog_instructions={} application_instructions={} upgrade_reregistered=true external_executor_rejected=true",
        trap_at, resumed_at, recovered.watchdog_instructions, recovered.application_instructions,
    );
}

#[test]
fn overdue_serial_timers_have_a_bounded_shared_dispatch_burst() {
    let fixture = install_fixture_canister("startup_timer");
    assert!(start(&fixture));

    fixture.pocket_ic().advance_time(Duration::from_mins(5));
    fixture.pocket_ic().tick();
    let overdue = snapshot(&fixture);
    let delivered = overdue
        .watchdog_callbacks
        .saturating_add(overdue.application_callbacks);
    assert!(delivered > 0);
    assert!(
        delivered <= 250,
        "one overdue dispatch burst must honor the pinned shared cap"
    );
    assert!(overdue.watchdog_callbacks > 0);
    assert!(overdue.application_callbacks > 0);

    println!(
        "icydb_0225_timer_overdue requested_seconds=300 watchdog_callbacks={} application_callbacks={} shared_delivered={} pinned_shared_cap=250",
        overdue.watchdog_callbacks, overdue.application_callbacks, delivered,
    );
}
