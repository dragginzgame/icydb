//! Module: integration
//! Responsibility: fixture installation and PocketIC harness contracts.
//! Does not own: canister build/report tooling or product runtime semantics.
//! Boundary: consumes retained build artifacts and Testkit-owned simulator instances.

#[cfg(test)]
mod pocketic_startup_tests;

pub mod durable_mutation_job_contract;
pub mod fixture_variants;
pub mod group_path_contract;
pub mod nested_relation_contract;
mod wire;
pub use wire::WireCallExt;

use std::time::Duration;

use ic_platform::{
    pocket_ic::{PocketIc, PocketIcBuilder},
    principal::ExternalPrincipal as Principal,
};
use ic_testkit::pic::{
    InstallSpec, PocketIcInstance, PocketIcStartupConfig, StandaloneCanisterFixture,
};
use icydb::{Error, ErrorCode};
pub use icydb_testing_audit_sql_perf_fixtures::contracts::StartupWatchdogPerfSnapshot;
use icydb_testing_build::{
    CanisterBuildOptions, local_fixture_wasm_bytes, local_fixture_wasm_bytes_with_options,
    resolve_fixture_canister_build_configuration,
};

const FIXTURE_INSTALL_CYCLES: u128 = 100_000_000_000_000;
const FIXTURE_STARTUP_MESSAGE_COMPLETION_TICKS: usize = 4;
const POCKET_IC_INSTANCE_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
// A production watchdog callback may consume its full 30-billion-instruction
// allocation under deterministic time slicing. The maintained timer-
// exhaustion proof uses this same zero-time completion envelope.
const WATCHDOG_MESSAGE_COMPLETION_TICKS: usize = 24;

/// Maximum watchdog deliveries in the frozen normal convergence residual proof.
///
/// This is `B_0 + C_driver`, or `64 + 6`, for the maximum admitted backlog:
/// the original four driver deliveries plus separate replay and verification.
/// Ordinary online convergence still needs at most one fold per batch.
pub const MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES: usize = 70;

/// Deliver pending startup-watchdog messages in PocketIC without advancing time.
pub fn deliver_startup_watchdog_message(pic: &PocketIc) {
    // Normal progress schedules zero-delay successor messages. These bounded
    // ticks let PocketIC deliver them and finish deterministic time slicing
    // without admitting a cadence-backed retry.
    for _ in 0..WATCHDOG_MESSAGE_COMPLETION_TICKS {
        pic.tick();
    }
}

/// Decode the SQL audit canister's canonical startup-watchdog evidence.
///
/// # Panics
///
/// Panics when the canister response does not decode as the maintained
/// watchdog snapshot contract.
#[must_use]
pub fn startup_watchdog_perf_snapshot(
    fixture: &StandaloneCanisterFixture,
) -> StartupWatchdogPerfSnapshot {
    fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "startup_watchdog_perf_snapshot", ())
        .expect("startup watchdog performance snapshot should decode")
}

/// Report whether the SQL audit canister's generated startup watchdog is armed.
///
/// # Panics
///
/// Panics when the canister response does not decode as a boolean armed state.
#[must_use]
pub fn startup_watchdog_armed(fixture: &StandaloneCanisterFixture) -> bool {
    fixture
        .pocket_ic()
        .query_wire(fixture.canister_id(), "startup_watchdog_armed", ())
        .expect("startup watchdog scheduling state should decode")
}

/// Deliver bounded startup-watchdog messages until the SQL audit fixture
/// admits ordinary work.
///
/// # Panics
///
/// Panics when the ordinary-work probe cannot be decoded, returns a terminal
/// error, or remains recovery-blocked beyond the maintained delivery bound.
pub fn advance_startup_watchdog_until_ready(fixture: &StandaloneCanisterFixture) {
    for delivered in 0..=MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
        let probe: Result<(), Error> = fixture
            .pocket_ic()
            .update_wire(
                fixture.canister_id(),
                "initialize_startup_observation_fixture",
                (),
            )
            .expect("ordinary startup probe should decode");
        match probe {
            Ok(()) => return,
            Err(error)
                if error.code()
                    == ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING =>
            {
                if delivered == MAX_NORMAL_CONVERGENCE_WATCHDOG_DELIVERIES {
                    break;
                }
                deliver_startup_watchdog_message(fixture.pocket_ic());
                // A new boot first waits for raw_rand; completed replies do
                // not make the cadence-backed retry immediately due.
                fixture.pocket_ic().advance_time(Duration::from_secs(1));
            }
            Err(error) => panic!("startup driver returned terminal error: {error}"),
        }
    }
    panic!("startup driver should finish within its frozen residual delivery bound");
}

/// Install already-built fixture WASM into one fresh standalone PocketIC instance.
///
/// # Panics
///
/// Panics if the canister name is unsupported, PocketIC cannot start, unit
/// lifecycle arguments cannot be encoded, or installation fails.
#[must_use]
pub fn install_prebuilt_fixture_canister(
    canister_name: &str,
    wasm: Vec<u8>,
) -> StandaloneCanisterFixture {
    let fixture = install_prebuilt_fixture_canister_without_startup_delivery(canister_name, wasm);
    deliver_fixture_startup_watchdog(fixture.pocket_ic());
    fixture
}

/// Install already-built fixture WASM without delivering its startup watchdog.
///
/// This is reserved for lifecycle tests that must observe the generated
/// canister before its first startup callback. Ordinary integration
/// tests should use [`install_prebuilt_fixture_canister`].
///
/// # Panics
///
/// Panics if the canister name is unsupported, PocketIC cannot start, unit
/// lifecycle arguments cannot be encoded, or installation fails.
#[must_use]
pub fn install_prebuilt_fixture_canister_without_startup_delivery(
    canister_name: &str,
    wasm: Vec<u8>,
) -> StandaloneCanisterFixture {
    resolve_fixture_canister_build_configuration(canister_name, CanisterBuildOptions::default())
        .unwrap_or_else(|error| panic!("fixture canister should be supported: {error}"));
    StandaloneCanisterFixture::try_install(
        start_fixture_pocket_ic(),
        InstallSpec {
            wasm,
            init_bytes: icydb::wire::encode_init_arguments(())
                .expect("unit init arguments should encode"),
            cycles: FIXTURE_INSTALL_CYCLES,
            install_sender: None,
            label: Some(canister_name.to_string()),
        },
    )
    .unwrap_or_else(|error| panic!("install {canister_name} fixture: {error}"))
}

/// Build one supported canister and install it into a fresh standalone fixture
/// with unit init arguments encoded through the lifecycle Wire contract.
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, unit
/// lifecycle arguments cannot be encoded, or installation fails.
#[must_use]
pub fn install_fixture_canister(canister_name: &str) -> StandaloneCanisterFixture {
    let fixture = install_fixture_canister_without_startup_delivery(canister_name);
    deliver_fixture_startup_watchdog(fixture.pocket_ic());
    fixture
}

/// Build and install one fixture without delivering its startup watchdog.
///
/// This is reserved for lifecycle tests that must observe the generated
/// canister before its first startup callback. Ordinary integration
/// tests should use [`install_fixture_canister`].
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, unit
/// lifecycle arguments cannot be encoded, or installation fails.
#[must_use]
pub fn install_fixture_canister_without_startup_delivery(
    canister_name: &str,
) -> StandaloneCanisterFixture {
    install_fixture_canister_with_options_and_optional_progress(
        canister_name,
        CanisterBuildOptions::default(),
        None,
    )
}

fn install_fixture_canister_with_options_and_optional_progress(
    canister_name: &str,
    options: CanisterBuildOptions,
    progress_label: Option<&str>,
) -> StandaloneCanisterFixture {
    if let Some(label) = progress_label {
        eprintln!("{label}: resolving/building local {canister_name} wasm");
    }
    let wasm = local_fixture_wasm_bytes_with_options(canister_name, options);
    if let Some(label) = progress_label {
        eprintln!(
            "{label}: local {canister_name} wasm ready ({} bytes)",
            wasm.len(),
        );
        eprintln!("{label}: handing off to PocketIC install/startup");
    }

    let fixture = StandaloneCanisterFixture::try_install(
        start_fixture_pocket_ic(),
        InstallSpec {
            wasm,
            init_bytes: icydb::wire::encode_init_arguments(())
                .expect("unit init arguments should encode"),
            cycles: FIXTURE_INSTALL_CYCLES,
            install_sender: None,
            label: Some(canister_name.to_string()),
        },
    )
    .unwrap_or_else(|error| panic!("install {canister_name} fixture: {error}"));
    if let Some(label) = progress_label {
        eprintln!("{label}: installed {canister_name} canister in PocketIC");
    }
    fixture
}

/// Create an application-subnet instance using Testkit's explicit environment contract.
///
/// Connect to `IC_TESTKIT_POCKET_IC_URL` when selected, otherwise use the
/// prepared `POCKET_IC_BIN`. No executable discovery or download occurs.
///
/// # Panics
/// Panics if the selected configuration is invalid or instance startup fails.
pub fn start_fixture_pocket_ic() -> PocketIcInstance<PocketIc> {
    let config = PocketIcStartupConfig::from_env(POCKET_IC_INSTANCE_STARTUP_TIMEOUT)
        .unwrap_or_else(|error| panic!("configure fixture PocketIC: {error}"));
    config
        .build_instance(PocketIcBuilder::new().with_application_subnet())
        .unwrap_or_else(|error| panic!("start fixture PocketIC on governed server: {error}"))
}

/// Deliver a bounded set of generated startup-watchdog messages.
///
/// This helper is used after installation or upgrade when a test needs an
/// ordinary-work-ready canister but does not inspect the startup control
/// surface itself.
pub fn deliver_fixture_startup_watchdog(pic: &PocketIc) {
    // Drain asynchronous entropy replies before admitting the next one-second
    // watchdog retry. Ticks alone advance only deterministic execution slices,
    // so they cannot finish startup after its first entropy-pending result.
    // Keep setup bounded for both install and upgrade.
    for _ in 0..8 {
        pic.advance_time(Duration::from_secs(1));
        for _ in 0..FIXTURE_STARTUP_MESSAGE_COMPLETION_TICKS {
            pic.tick();
        }
    }
}

/// Reset and reload the generated IcyDB fixture set on one installed canister.
///
/// # Panics
///
/// Panics if the reset or load calls fail to decode or return fixture errors.
pub fn reset_icydb_fixtures(pic: &PocketIc, canister_id: Principal) {
    let reset: Result<(), Error> = pic
        .update_wire(canister_id, "icydb_fixtures_reset", ())
        .expect("icydb_fixtures_reset should decode");
    reset.expect("icydb_fixtures_reset should succeed");

    let load: Result<(), Error> = pic
        .update_wire(canister_id, "icydb_fixtures_load", ())
        .expect("icydb_fixtures_load should decode");
    load.expect("icydb_fixtures_load should succeed");
}

/// Build and upgrade one installed fixture canister with the current local WASM.
///
/// # Panics
///
/// Panics if the canister cannot be built, the built WASM cannot be read, unit
/// lifecycle arguments cannot be encoded, or PocketIC rejects the upgrade.
pub fn upgrade_fixture_canister(pic: &PocketIc, canister_id: Principal, canister_name: &str) {
    let wasm = local_fixture_wasm_bytes(canister_name);
    let args = icydb::wire::encode_post_upgrade_arguments(())
        .expect("unit post-upgrade arguments should encode");

    pic.upgrade_canister(canister_id, wasm, args, None)
        .unwrap_or_else(|err| panic!("{canister_name} canister upgrade should succeed: {err}"));
}
