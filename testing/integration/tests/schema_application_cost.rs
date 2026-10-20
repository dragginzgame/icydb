use ic_platform::principal::ExternalPrincipal as Principal;
use ic_testkit::pic::{StandaloneCanisterFixture, advance_and_tick};
use icydb_testing_audit_sql_perf_fixtures::contracts::{
    AcceptedSchemaReadInstructionResult, ApplicationStartupHook, ApplicationStartupSnapshot,
    SchemaApplicationPerfResult, StartupObservationPerfResult,
};
use icydb_testing_integration::WireCallExt as _;
use icydb_testing_integration::{
    install_fixture_canister, install_fixture_canister_without_startup_delivery,
    upgrade_fixture_canister,
};

use std::time::Duration;

const TIMER_EXECUTOR_METHOD: &str = "<ic-timers internal> dispatch";

#[derive(serde::Deserialize, serde::Serialize)]
struct StartupFailureInput {
    kind: icydb::db::StartupFailureKind,
    diagnostic: icydb::Error,
}

fn query_probe_result(
    fixture: &StandaloneCanisterFixture,
) -> Result<SchemaApplicationPerfResult, icydb::Error> {
    fixture
        .pocket_ic()
        .query_wire(
            fixture.canister_id(),
            "measure_schema_application_query",
            (),
        )
        .expect("schema-application query probe should decode")
}

fn query_probe(fixture: &StandaloneCanisterFixture) -> SchemaApplicationPerfResult {
    query_probe_result(fixture).expect("schema-application query probe should succeed")
}

fn update_probe(fixture: &StandaloneCanisterFixture) -> SchemaApplicationPerfResult {
    let result: Result<SchemaApplicationPerfResult, icydb::Error> = fixture
        .pocket_ic()
        .update_wire(
            fixture.canister_id(),
            "measure_schema_application_update",
            (),
        )
        .expect("schema-application update probe should decode");
    result.expect("schema-application update probe should succeed")
}

fn startup_observation(fixture: &StandaloneCanisterFixture) -> StartupObservationPerfResult {
    let result: Result<StartupObservationPerfResult, icydb::db::StartupFailure> = fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "measure_startup_observation", ())
        .expect("startup observation should decode");
    result.expect("startup observation should not fail")
}

fn accepted_schema_read(
    fixture: &StandaloneCanisterFixture,
    entity: &str,
) -> AcceptedSchemaReadInstructionResult {
    let result: Result<AcceptedSchemaReadInstructionResult, icydb::Error> = fixture
        .pocket_ic()
        .query_wire(
            fixture.canister_id(),
            "measure_accepted_schema_read_instructions",
            (entity.to_string(),),
        )
        .expect("accepted schema read should decode");
    result.expect("accepted schema read should succeed")
}

fn application_startup_contract(fixture: &StandaloneCanisterFixture) -> ApplicationStartupSnapshot {
    fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "application_startup_contract", ())
        .expect("application startup contract should decode")
}

fn inject_application_startup_failure(fixture: &StandaloneCanisterFixture) {
    let failure = StartupFailureInput {
        kind: icydb::db::StartupFailureKind::JournalRecovery,
        diagnostic: icydb::Error::from_error_code(
            icydb::ErrorCode::STORE_CORRUPTION,
            icydb::ErrorOrigin::Recovery,
        ),
    };
    fixture
        .pocket_ic()
        .update_wire::<(), _>(
            fixture.canister_id(),
            "observe_application_startup_for_tests",
            (Result::<icydb::db::DatabaseStartupState, StartupFailureInput>::Err(failure),),
        )
        .expect("typed application startup failure should decode");
}

fn advance_watchdog(fixture: &StandaloneCanisterFixture) {
    for _ in 0..8 {
        advance_watchdog_by(fixture, Duration::from_secs(1));
        if startup_observation(fixture).state == icydb::db::DatabaseStartupState::Ready {
            // The pre-armed Watchdog scheduler and its work run in separate
            // messages. An independent application poll due at the same time
            // may observe Recovering between them and retry one cadence later.
            advance_watchdog_by(fixture, Duration::from_secs(1));
            return;
        }
    }
    panic!("startup watchdog should reach ready within eight delivered ticks");
}

fn advance_watchdog_by(fixture: &StandaloneCanisterFixture, duration: Duration) {
    advance_and_tick(fixture.pocket_ic(), duration, 4);
}

#[test]
fn application_readiness_restores_only_after_ready_and_stops_on_typed_failure() {
    let fixture = install_fixture_canister_without_startup_delivery("sql_perf");
    let installed = application_startup_contract(&fixture);
    assert_eq!(installed.hook, ApplicationStartupHook::Init);
    assert!(installed.engine_registered_before_hook);
    assert_eq!(installed.observations, 1);
    assert_eq!(installed.recovering_observations, 1);
    assert_eq!(installed.ready_observations, 0);
    assert_eq!(installed.restorations, 0);
    assert!(installed.retry_scheduled);
    assert!(installed.failure.is_none());

    advance_watchdog(&fixture);
    let installed_ready = application_startup_contract(&fixture);
    assert_eq!(installed_ready.ready_observations, 1);
    assert_eq!(installed_ready.restorations, 1);
    assert!(!installed_ready.retry_scheduled);

    upgrade_fixture_canister(fixture.pocket_ic(), fixture.canister_id(), "sql_perf");
    let upgraded = application_startup_contract(&fixture);
    assert_eq!(upgraded.hook, ApplicationStartupHook::PostUpgrade);
    assert!(upgraded.engine_registered_before_hook);
    assert_eq!(upgraded.recovering_observations, 1);
    assert_eq!(upgraded.restorations, 0);
    assert!(upgraded.retry_scheduled);
    advance_watchdog(&fixture);
    let upgraded_ready = application_startup_contract(&fixture);
    assert_eq!(upgraded_ready.ready_observations, 1);
    assert_eq!(upgraded_ready.restorations, 1);

    upgrade_fixture_canister(fixture.pocket_ic(), fixture.canister_id(), "sql_perf");
    inject_application_startup_failure(&fixture);
    let failed = application_startup_contract(&fixture);
    let failure = failed
        .failure
        .as_ref()
        .expect("application should retain the typed startup failure");
    assert_eq!(
        failure.kind(),
        icydb::db::StartupFailureKind::JournalRecovery
    );
    assert_eq!(failure.error().code(), icydb::ErrorCode::STORE_CORRUPTION);
    assert_eq!(failed.restorations, 0);
    assert!(!failed.retry_scheduled);
    let observations = failed.observations;

    advance_watchdog_by(&fixture, Duration::from_mins(5));
    advance_watchdog(&fixture);
    let after_failure = application_startup_contract(&fixture);
    assert_eq!(after_failure.observations, observations);
    assert_eq!(after_failure.restorations, 0);
    assert_eq!(after_failure.failure, failed.failure);
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Ready,
        "stopping the application retry must not stop the engine watchdog",
    );
}

#[test]
fn lifecycle_driver_is_private_and_reconstructable_after_upgrade() {
    let fixture = install_fixture_canister_without_startup_delivery("sql_perf");
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Recovering,
    );
    let external = fixture.pocket_ic().update_call(
        fixture.canister_id(),
        Principal::anonymous(),
        TIMER_EXECUTOR_METHOD,
        0_u64.to_le_bytes().to_vec(),
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
    upgrade_fixture_canister(fixture.pocket_ic(), fixture.canister_id(), "sql_perf");
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Recovering,
    );
    advance_watchdog_by(&fixture, Duration::from_mins(5));
    advance_watchdog(&fixture);
    assert_eq!(
        startup_observation(&fixture).state,
        icydb::db::DatabaseStartupState::Ready,
    );
}

#[test]
fn startup_observation_is_bounded_before_after_readiness_and_upgrade() {
    const OBSERVATION_INSTRUCTION_CEILING: u64 = 25_000_000;

    let fixture = install_fixture_canister_without_startup_delivery("sql_perf");
    let fresh = startup_observation(&fixture);
    assert_eq!(fresh.state, icydb::db::DatabaseStartupState::Recovering);
    assert!(fresh.local_instructions > 0);
    assert!(fresh.local_instructions <= OBSERVATION_INSTRUCTION_CEILING);

    let pending: Result<(), icydb::Error> = fixture
        .pocket_ic()
        .update_wire(
            fixture.canister_id(),
            "initialize_startup_observation_fixture",
            (),
        )
        .expect("ordinary pending probe should decode");
    let pending = pending.expect_err("ordinary work must reject while recovery is pending");
    assert_eq!(
        pending.code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );
    assert_eq!(startup_observation(&fixture).state, fresh.state);
    advance_watchdog(&fixture);
    let ready = startup_observation(&fixture);
    assert_eq!(ready.state, icydb::db::DatabaseStartupState::Ready);
    assert!(ready.local_instructions <= OBSERVATION_INSTRUCTION_CEILING);

    upgrade_fixture_canister(fixture.pocket_ic(), fixture.canister_id(), "sql_perf");
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
fn maximum_accepted_schema_reopen_read_is_bounded_at_sixty_four_indexes() {
    let fixture = install_fixture_canister("sql_perf");
    let measured = accepted_schema_read(&fixture, "PerfAuditMaxFanout");
    assert_eq!(measured.description.indexes().len(), 64);
    assert!(measured.local_instructions > 0);
    assert!(measured.local_instructions < 40_000_000_000);

    println!(
        "0.231 accepted-schema reopen: maximum_indexes=64 instructions={}",
        measured.local_instructions,
    );
}

#[test]
fn schema_application_is_driver_owned_and_ordinary_probes_are_state_only() {
    let fixture = install_fixture_canister_without_startup_delivery("sql_perf");

    let pending = query_probe_result(&fixture)
        .expect_err("ordinary query-side schema application must be removed");
    assert_eq!(
        pending.code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );
    advance_watchdog(&fixture);

    let first_ready_query = query_probe(&fixture);
    let second_ready_query = query_probe(&fixture);
    assert_eq!(
        first_ready_query.reconcile_checks,
        first_ready_query.exact_match
    );
    assert_eq!(first_ready_query.first_create, 0);
    assert!(first_ready_query.exact_match > 0);
    assert_eq!(
        second_ready_query.reconcile_checks,
        second_ready_query.exact_match
    );
    assert_eq!(second_ready_query.first_create, 0);

    let update = update_probe(&fixture);
    assert_eq!(update.reconcile_checks, update.exact_match);
    assert_eq!(update.first_create, 0);
    assert!(update.exact_match > 0);

    upgrade_fixture_canister(fixture.pocket_ic(), fixture.canister_id(), "sql_perf");
    let post_upgrade_pending = query_probe_result(&fixture)
        .expect_err("post-upgrade query must not drive recovery or schema reconciliation");
    assert_eq!(
        post_upgrade_pending.code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );
    advance_watchdog(&fixture);
    let post_upgrade_query = query_probe(&fixture);
    let repeated_post_upgrade_query = query_probe(&fixture);
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

    println!(
        "schema application lifecycle: first_ready_query={} second_ready_query={} update={} post_upgrade={} repeated_post_upgrade={}",
        first_ready_query.local_instructions,
        second_ready_query.local_instructions,
        update.local_instructions,
        post_upgrade_query.local_instructions,
        repeated_post_upgrade_query.local_instructions,
    );
}
