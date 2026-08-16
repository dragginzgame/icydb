use std::{collections::BTreeSet, time::Duration};

use candid::{CandidType, Deserialize};
use ic_testkit::pic::{
    CanisterInstallExt, ErrorCode, RejectResponse, RetryPolicy, StandaloneCanisterFixture,
};
use icydb::types::Ulid;
use icydb_testing_integration::{
    CanisterBuildOptions, CanisterBuildProfile, build_fixture_canister_wasm_bytes_with_options,
    canister_artifact::{CanisterMethod, CanisterMethodMode, inspect_wasm_methods},
    install_fixture_canister_without_startup_delivery,
};

const PARTICIPANT_INSTRUCTION_CEILING: u64 = 10_750_000;
const INSTALL_CODE_RETRY_LIMIT: usize = 4;
const INSTALL_CODE_COOLDOWN: Duration = Duration::from_secs(5 * 60);

fn production_build_options() -> CanisterBuildOptions {
    CanisterBuildOptions {
        build_profile: CanisterBuildProfile::Production,
        ..CanisterBuildOptions::default()
    }
}

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum LifecycleHook {
    Init,
    PostUpgrade,
}

#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum ApplicationActivationState {
    Prepared,
    Active,
}

#[derive(CandidType, Clone, Debug, Deserialize)]
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

fn snapshot(fixture: &StandaloneCanisterFixture) -> LifecycleCompositionSnapshot {
    fixture
        .query_candid("lifecycle_composition_snapshot", ())
        .expect("lifecycle composition snapshot should decode")
}

fn database_probe(fixture: &StandaloneCanisterFixture) -> Result<bool, icydb::Error> {
    fixture
        .query_candid("lifecycle_database_probe", ())
        .expect("lifecycle database probe should decode")
}

fn insert_probe_row(fixture: &StandaloneCanisterFixture) -> Result<Ulid, icydb::Error> {
    fixture
        .update_candid("lifecycle_insert_probe_row", ())
        .expect("lifecycle probe-row insert should decode")
}

fn probe_row_exists(fixture: &StandaloneCanisterFixture, id: Ulid) -> Result<bool, icydb::Error> {
    fixture
        .query_candid("lifecycle_probe_row_exists", (id,))
        .expect("lifecycle probe-row query should decode")
}

fn assert_synchronous_ordering(
    snapshot: &LifecycleCompositionSnapshot,
    expected_hook: LifecycleHook,
    phase: &str,
) {
    assert_eq!(snapshot.hook, expected_hook);
    assert_eq!(snapshot.activation, ApplicationActivationState::Prepared);
    assert_eq!(snapshot.ingress_authority_order, 1);
    assert_eq!(snapshot.participant_order, 2);
    assert!(snapshot.participant_instructions > 0);
    assert!(
        snapshot.participant_instructions <= PARTICIPANT_INSTRUCTION_CEILING,
        "{phase} participant instructions {} exceed the frozen {} ceiling",
        snapshot.participant_instructions,
        PARTICIPANT_INSTRUCTION_CEILING,
    );
    assert_eq!(
        snapshot.startup_after_participant,
        Some(Ok(icydb::db::DatabaseStartupState::Recovering)),
    );
    assert!(snapshot.watchdog_armed_after_participant);
    assert_eq!(snapshot.framework_order, 3);
    assert_eq!(snapshot.deferred_schedule_order, 4);
    assert_eq!(snapshot.deferred_runs, 0);
    assert_eq!(snapshot.database_work_runs, 0);
    assert!(snapshot.startup_failure.is_none());
    assert!(snapshot.database_failure.is_none());
}

fn advance_until_active(fixture: &StandaloneCanisterFixture) -> LifecycleCompositionSnapshot {
    for _ in 0..12 {
        fixture.pocket_ic().advance_time(Duration::from_secs(1));
        for _ in 0..4 {
            fixture.pocket_ic().tick();
        }
        let current = snapshot(fixture);
        if current.activation == ApplicationActivationState::Active {
            return current;
        }
    }
    panic!("deferred application work should activate within twelve ticks");
}

fn install_code_retry_policy() -> RetryPolicy {
    RetryPolicy::try_new(INSTALL_CODE_RETRY_LIMIT, INSTALL_CODE_COOLDOWN)
        .expect("install-code retry policy should be valid")
}

fn upgrade_with_retry(
    fixture: &StandaloneCanisterFixture,
    wasm: &[u8],
    args: &[u8],
) -> Result<(), RejectResponse> {
    fixture
        .pocket_ic()
        .retry_install_code(install_code_retry_policy(), || {
            fixture.pocket_ic().upgrade_canister(
                fixture.canister_id(),
                wasm.to_vec(),
                args.to_vec(),
                None,
            )
        })
}

fn upgrade_with_wasm(fixture: &StandaloneCanisterFixture, wasm: Vec<u8>) {
    let args = candid::encode_args((Some(false),))
        .expect("non-trapping lifecycle upgrade args should encode");
    upgrade_with_retry(fixture, &wasm, &args)
        .expect("lifecycle participant upgrade should succeed");
}

fn upgrade_preserving_stable_extent(
    fixture: &StandaloneCanisterFixture,
    upgrade: impl FnOnce(),
    context: &str,
) -> usize {
    let before = fixture
        .pocket_ic()
        .get_stable_memory(fixture.canister_id())
        .len();
    upgrade();
    let after = fixture
        .pocket_ic()
        .get_stable_memory(fixture.canister_id())
        .len();
    assert_eq!(after, before, "{context}");
    before
}

#[test]
fn framework_neutral_root_orders_both_lifecycle_phases_before_deferred_database_work() {
    let fixture = install_fixture_canister_without_startup_delivery("lifecycle_participant");

    let installed = snapshot(&fixture);
    assert_synchronous_ordering(&installed, LifecycleHook::Init, "init");
    let pending = database_probe(&fixture)
        .expect_err("ordinary database work must remain pending after synchronous lifecycle");
    assert_eq!(
        pending.code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );

    let installed_active = advance_until_active(&fixture);
    assert_eq!(installed_active.database_work_runs, 1);
    assert!(installed_active.deferred_runs >= 1);
    assert!(database_probe(&fixture).expect("ready database probe should succeed"));
    assert!(
        !probe_row_exists(&fixture, Ulid::MIN).expect("empty participant query should succeed")
    );

    let local_wasm = build_fixture_canister_wasm_bytes_with_options(
        "lifecycle_participant",
        CanisterBuildOptions::default(),
    );
    let empty_stable_bytes_before_upgrade = upgrade_preserving_stable_extent(
        &fixture,
        || upgrade_with_wasm(&fixture, local_wasm),
        "participant reconstruction must not allocate additional empty stable memory",
    );
    let upgraded = snapshot(&fixture);
    assert_synchronous_ordering(&upgraded, LifecycleHook::PostUpgrade, "empty post-upgrade");
    let post_upgrade_pending = database_probe(&fixture)
        .expect_err("post-upgrade database work must wait for engine readiness");
    assert_eq!(
        post_upgrade_pending.code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );

    let upgraded_active = advance_until_active(&fixture);
    assert_eq!(upgraded_active.database_work_runs, 1);
    assert!(upgraded_active.deferred_runs >= 1);
    assert!(database_probe(&fixture).expect("post-upgrade ready probe should succeed"));
    assert!(
        !probe_row_exists(&fixture, Ulid::MIN).expect("empty row state should survive upgrade")
    );

    let probe_row_id = insert_probe_row(&fixture).expect("bounded probe-row insert should succeed");
    assert!(
        probe_row_exists(&fixture, probe_row_id).expect("inserted probe row should be observable")
    );
    let production_wasm = build_fixture_canister_wasm_bytes_with_options(
        "lifecycle_participant",
        production_build_options(),
    );

    let populated_stable_bytes_before_upgrade = upgrade_preserving_stable_extent(
        &fixture,
        || upgrade_with_wasm(&fixture, production_wasm.clone()),
        "participant reconstruction must not allocate additional populated stable memory",
    );
    let populated_upgrade = snapshot(&fixture);
    assert_synchronous_ordering(
        &populated_upgrade,
        LifecycleHook::PostUpgrade,
        "populated post-upgrade",
    );
    let populated_active = advance_until_active(&fixture);
    assert_eq!(populated_active.database_work_runs, 1);
    assert!(
        probe_row_exists(&fixture, probe_row_id).expect("populated row should survive upgrade")
    );

    upgrade_preserving_stable_extent(
        &fixture,
        || upgrade_with_wasm(&fixture, production_wasm),
        "same-shaped participant reconstruction must not allocate stable memory",
    );
    let converged_upgrade = snapshot(&fixture);
    assert_synchronous_ordering(
        &converged_upgrade,
        LifecycleHook::PostUpgrade,
        "converged post-upgrade",
    );
    let converged_active = advance_until_active(&fixture);
    assert!(
        probe_row_exists(&fixture, probe_row_id)
            .expect("populated row should survive a same-shaped upgrade")
    );

    println!(
        "icydb_0227_lifecycle_participant init_instructions={} empty_post_upgrade_instructions={} populated_post_upgrade_instructions={} converged_post_upgrade_instructions={} init_deferred_runs={} empty_post_upgrade_deferred_runs={} populated_post_upgrade_deferred_runs={} converged_post_upgrade_deferred_runs={} empty_stable_bytes={} populated_stable_bytes={} ceiling={}",
        installed.participant_instructions,
        upgraded.participant_instructions,
        populated_upgrade.participant_instructions,
        converged_upgrade.participant_instructions,
        installed_active.deferred_runs,
        upgraded_active.deferred_runs,
        populated_active.deferred_runs,
        converged_active.deferred_runs,
        empty_stable_bytes_before_upgrade,
        populated_stable_bytes_before_upgrade,
        PARTICIPANT_INSTRUCTION_CEILING,
    );
}

#[test]
fn trapped_post_upgrade_rolls_back_participation_and_allows_a_clean_retry() {
    let fixture = install_fixture_canister_without_startup_delivery("lifecycle_participant");
    let active = advance_until_active(&fixture);
    assert_eq!(active.database_work_runs, 1);
    let probe_row_id = insert_probe_row(&fixture).expect("bounded probe-row insert should succeed");
    let production_wasm = build_fixture_canister_wasm_bytes_with_options(
        "lifecycle_participant",
        production_build_options(),
    );
    let stable_bytes_before = fixture
        .pocket_ic()
        .get_stable_memory(fixture.canister_id())
        .len();

    let trap_args =
        candid::encode_args((Some(true),)).expect("trapping lifecycle upgrade args should encode");
    let failed_upgrade = upgrade_with_retry(&fixture, &production_wasm, &trap_args)
        .expect_err("post-upgrade must propagate a trap after participant execution");
    assert_eq!(
        failed_upgrade.error_code,
        ErrorCode::CanisterCalledTrap,
        "the rollback probe must fail from its lifecycle trap, not install-code throttling",
    );
    assert_eq!(
        fixture
            .pocket_ic()
            .get_stable_memory(fixture.canister_id())
            .len(),
        stable_bytes_before,
        "a trapped lifecycle message must roll back stable-state changes",
    );
    assert!(database_probe(&fixture).expect("the pre-upgrade module should remain ready"));
    assert!(
        probe_row_exists(&fixture, probe_row_id)
            .expect("the pre-upgrade row should survive a trapped upgrade")
    );

    upgrade_with_wasm(&fixture, production_wasm);
    assert_eq!(
        fixture
            .pocket_ic()
            .get_stable_memory(fixture.canister_id())
            .len(),
        stable_bytes_before,
        "a clean retry must preserve the stable-memory extent",
    );
    let retried = snapshot(&fixture);
    assert_synchronous_ordering(&retried, LifecycleHook::PostUpgrade, "retried post-upgrade");
    let retried_active = advance_until_active(&fixture);
    assert_eq!(retried_active.database_work_runs, 1);
    assert!(
        probe_row_exists(&fixture, probe_row_id)
            .expect("the populated row should survive the clean retry")
    );
}

#[test]
fn participant_functions_are_absent_from_the_complete_ingress_surface() {
    let wasm = build_fixture_canister_wasm_bytes_with_options(
        "lifecycle_participant",
        CanisterBuildOptions::default(),
    );
    let methods = inspect_wasm_methods(&wasm).expect("participant fixture Wasm should inspect");

    assert_eq!(
        methods,
        BTreeSet::from([
            CanisterMethod {
                name: "<ic-cdk internal> timer_executor".to_string(),
                mode: CanisterMethodMode::Update,
            },
            CanisterMethod {
                name: "lifecycle_composition_snapshot".to_string(),
                mode: CanisterMethodMode::Query,
            },
            CanisterMethod {
                name: "lifecycle_database_probe".to_string(),
                mode: CanisterMethodMode::Query,
            },
            CanisterMethod {
                name: "lifecycle_insert_probe_row".to_string(),
                mode: CanisterMethodMode::Update,
            },
            CanisterMethod {
                name: "lifecycle_probe_row_exists".to_string(),
                mode: CanisterMethodMode::Query,
            },
        ]),
    );

    let production_wasm = build_fixture_canister_wasm_bytes_with_options(
        "lifecycle_participant",
        production_build_options(),
    );
    let production_methods =
        inspect_wasm_methods(&production_wasm).expect("production participant Wasm should inspect");
    assert_eq!(
        production_methods,
        BTreeSet::from([
            CanisterMethod {
                name: "<ic-cdk internal> timer_executor".to_string(),
                mode: CanisterMethodMode::Update,
            },
            CanisterMethod {
                name: "lifecycle_composition_snapshot".to_string(),
                mode: CanisterMethodMode::Query,
            },
            CanisterMethod {
                name: "lifecycle_database_probe".to_string(),
                mode: CanisterMethodMode::Query,
            },
            CanisterMethod {
                name: "lifecycle_probe_row_exists".to_string(),
                mode: CanisterMethodMode::Query,
            },
        ]),
    );
}
