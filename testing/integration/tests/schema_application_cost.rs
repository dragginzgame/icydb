use candid::{CandidType, Deserialize, Principal};
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb_testing_integration::{install_fixture_canister, upgrade_fixture_canister};
use std::time::Duration;

const TIMER_EXECUTOR_METHOD: &str = "<ic-cdk internal> timer_executor";

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct SchemaApplicationPerfResult {
    local_instructions: u64,
    reconcile_checks: u64,
    first_create: u64,
    exact_match: u64,
}

#[derive(CandidType, Debug, Deserialize, Eq, PartialEq)]
struct StartupObservationPerfResult {
    state: icydb::db::DatabaseStartupState,
    local_instructions: u64,
}

fn query_probe(fixture: &StandaloneCanisterFixture) -> SchemaApplicationPerfResult {
    let result: Result<SchemaApplicationPerfResult, icydb::Error> = fixture
        .query_candid("measure_schema_application_query", ())
        .expect("schema-application query probe should decode");
    result.expect("schema-application query probe should succeed")
}

fn update_probe(fixture: &StandaloneCanisterFixture) -> SchemaApplicationPerfResult {
    let result: Result<SchemaApplicationPerfResult, icydb::Error> = fixture
        .update_candid("measure_schema_application_update", ())
        .expect("schema-application update probe should decode");
    result.expect("schema-application update probe should succeed")
}

fn startup_observation(fixture: &StandaloneCanisterFixture) -> StartupObservationPerfResult {
    let result: Result<StartupObservationPerfResult, icydb::db::StartupFailure> = fixture
        .query_candid("measure_startup_observation", ())
        .expect("startup observation should decode");
    result.expect("startup observation should not fail")
}

fn register_watchdog(fixture: &StandaloneCanisterFixture) -> bool {
    let result: Result<bool, icydb::db::StartupFailure> = fixture
        .update_candid("register_dormant_startup_watchdog", ())
        .expect("watchdog registration should decode");
    result.expect("watchdog registration should not observe terminal failure")
}

fn watchdog_registered(fixture: &StandaloneCanisterFixture) -> bool {
    fixture
        .query_candid("dormant_startup_watchdog_registered", ())
        .expect("watchdog state should decode")
}

fn advance_watchdog(fixture: &StandaloneCanisterFixture) {
    advance_watchdog_by(fixture, Duration::from_secs(1));
}

fn advance_watchdog_by(fixture: &StandaloneCanisterFixture, duration: Duration) {
    fixture.pocket_ic().advance_time(duration);
    fixture.pocket_ic().tick();
    fixture.pocket_ic().tick();
}

#[test]
fn dormant_driver_is_private_idempotent_and_reconstructable_after_upgrade() {
    let fixture = install_fixture_canister("sql_perf");
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Recovering,
    );
    assert!(register_watchdog(&fixture));
    assert!(!register_watchdog(&fixture));
    assert!(watchdog_registered(&fixture));

    let external = fixture.pocket_ic().update_call(
        fixture.canister_id(),
        Principal::anonymous(),
        TIMER_EXECUTOR_METHOD,
        0_u64.to_be_bytes().to_vec(),
    );
    assert!(
        external.is_err(),
        "timer executor must reject external ingress"
    );
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Recovering,
        "rejected ingress must not advance recovery",
    );

    advance_watchdog(&fixture);
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Ready,
    );
    assert!(!watchdog_registered(&fixture));

    upgrade_fixture_canister(&fixture, "sql_perf");
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Recovering,
    );
    assert!(!watchdog_registered(&fixture));
    assert!(register_watchdog(&fixture));
    advance_watchdog_by(&fixture, Duration::from_secs(300));
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Ready,
    );
    assert!(!watchdog_registered(&fixture));
}

#[test]
fn startup_observation_is_bounded_before_after_readiness_and_upgrade() {
    const OBSERVATION_INSTRUCTION_CEILING: u64 = 25_000_000;

    let fixture = install_fixture_canister("sql_perf");
    let fresh = startup_observation(&fixture);
    assert_eq!(fresh.state, icydb::db::DatabaseStartupState::Recovering);
    assert!(fresh.local_instructions > 0);
    assert!(fresh.local_instructions <= OBSERVATION_INSTRUCTION_CEILING);

    let initialized: Result<(), icydb::Error> = fixture
        .update_candid("initialize_startup_observation_fixture", ())
        .expect("startup initialization should decode");
    initialized.expect("predecessor admission should prepare readiness evidence");
    let ready = startup_observation(&fixture);
    assert_eq!(ready.state, icydb::db::DatabaseStartupState::Ready);
    assert!(ready.local_instructions <= OBSERVATION_INSTRUCTION_CEILING);

    upgrade_fixture_canister(&fixture, "sql_perf");
    let upgraded = startup_observation(&fixture);
    assert_eq!(upgraded.state, icydb::db::DatabaseStartupState::Recovering);
    assert!(upgraded.local_instructions <= OBSERVATION_INSTRUCTION_CEILING);

    println!(
        "icydb_0225_startup_observation fresh_instructions={} ready_instructions={} post_upgrade_instructions={} ceiling={}",
        fresh.local_instructions,
        ready.local_instructions,
        upgraded.local_instructions,
        OBSERVATION_INSTRUCTION_CEILING,
    );
}

#[test]
fn schema_application_lifecycle_distinguishes_query_rollback_update_and_upgrade() {
    let fixture = install_fixture_canister("sql_perf");

    let first_query = query_probe(&fixture);
    let second_query = query_probe(&fixture);
    eprintln!("schema application install queries: first={first_query:?} second={second_query:?}");
    assert!(first_query.local_instructions > 0);
    assert_eq!(first_query.reconcile_checks, first_query.first_create);
    assert!(first_query.first_create > 0);
    assert_eq!(first_query.exact_match, 0);
    assert_eq!(second_query.reconcile_checks, second_query.first_create);
    assert_eq!(second_query.first_create, first_query.first_create);
    assert_eq!(second_query.exact_match, 0);

    let update = update_probe(&fixture);
    eprintln!("schema application update: {update:?}");
    assert_eq!(update.reconcile_checks, update.first_create);
    assert_eq!(update.first_create, first_query.first_create);
    assert_eq!(update.exact_match, 0);

    let post_update_query = query_probe(&fixture);
    eprintln!("schema application post-update query: {post_update_query:?}");
    assert_eq!(
        post_update_query.reconcile_checks,
        post_update_query.exact_match
    );
    assert_eq!(post_update_query.first_create, 0);
    assert!(post_update_query.exact_match > 0);

    upgrade_fixture_canister(&fixture, "sql_perf");
    let post_upgrade_query = query_probe(&fixture);
    let repeated_post_upgrade_query = query_probe(&fixture);
    eprintln!(
        "schema application upgrade queries: first={post_upgrade_query:?} second={repeated_post_upgrade_query:?}"
    );
    assert_eq!(
        post_upgrade_query.reconcile_checks,
        post_upgrade_query.exact_match
    );
    assert!(post_upgrade_query.exact_match > 0);
    assert_eq!(post_upgrade_query.first_create, 0);
    assert_eq!(
        repeated_post_upgrade_query.reconcile_checks,
        repeated_post_upgrade_query.exact_match,
    );
    assert_eq!(
        repeated_post_upgrade_query.exact_match,
        post_upgrade_query.exact_match,
    );
    assert_eq!(repeated_post_upgrade_query.first_create, 0);

    eprintln!(
        "schema application lifecycle: first_query={} second_query={} update={} post_update={} post_upgrade={} repeated_post_upgrade={}",
        first_query.local_instructions,
        second_query.local_instructions,
        update.local_instructions,
        post_update_query.local_instructions,
        post_upgrade_query.local_instructions,
        repeated_post_upgrade_query.local_instructions,
    );
}
