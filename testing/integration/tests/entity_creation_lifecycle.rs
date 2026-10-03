//! Matched lifecycle envelopes; no instruction-to-cycle conversion is assumed.

use super::{migration_status, projection};
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{Error, ErrorCode, db::RowProjectionOutput, metrics::SchemaLifecycleMetrics};
use icydb_testing_integration::{
    CanisterWasmProfile, build_entity_creation_lifecycle_fixture_wasms,
    deliver_fixture_startup_watchdog, deliver_startup_watchdog_message,
    install_prebuilt_fixture_canister,
};

const ROWS: &str = "SELECT id, key, label, parent_id FROM Item ORDER BY id";

// Measurement endpoints return three Candid arguments. The fixture's ordinary
// update helper decodes one argument and is therefore unsuitable here.
fn measured_update<T>(fixture: &StandaloneCanisterFixture, method: &str) -> T
where
    T: for<'de> candid::utils::ArgumentDecoder<'de>,
{
    let bytes = fixture
        .pocket_ic()
        .update_call(
            fixture.canister_id(),
            candid::Principal::anonymous(),
            method,
            candid::encode_args(()).expect("empty arguments should encode"),
        )
        .expect("measurement update should succeed");
    candid::decode_args(&bytes).expect("measurement arguments should decode")
}

fn cycles(fixture: &StandaloneCanisterFixture) -> u128 {
    fixture.pocket_ic().cycle_balance(fixture.canister_id())
}

fn report_cycles(fixture: &StandaloneCanisterFixture, before: u128, label: &str) {
    // Preserve refunds of pending execution reservations as signed data.
    let decrease = i128::try_from(before).expect("fixture balance fits i128")
        - i128::try_from(cycles(fixture)).expect("fixture balance fits i128");
    eprintln!("creation lifecycle cycles: label={label} balance_decrease={decrease}");
}

fn lifecycle_metrics(fixture: &StandaloneCanisterFixture) -> SchemaLifecycleMetrics {
    let result: Result<SchemaLifecycleMetrics, Error> = fixture
        .query_candid("entity_creation_lifecycle_metrics", ())
        .expect("fixed lifecycle counters should decode");
    result.expect("canonical journal controls should remain readable")
}

fn assert_query_keeps_lifecycle_metrics(fixture: &StandaloneCanisterFixture) {
    // Query heap preparation is discarded and must not record owner costs.
    let before = lifecycle_metrics(fixture);
    projection(fixture, ROWS);
    assert_eq!(lifecycle_metrics(fixture), before);
}

fn report_lifecycle(metrics: &SchemaLifecycleMetrics, label: &str) {
    for (owner, counter) in [
        ("lowering", metrics.lowering()),
        ("publication", metrics.publication()),
        ("runtime-compilation", metrics.runtime_compilation()),
        ("cardinality", metrics.cardinality()),
        ("startup-recovery", metrics.startup_recovery()),
    ] {
        eprintln!(
            "creation lifecycle owner: label={label} owner={owner} samples={} instructions_total={} instructions_max={}",
            counter.samples(),
            counter.instructions_total(),
            counter.instructions_max()
        );
    }
}

fn snapshot(fixture: &StandaloneCanisterFixture, label: &str) -> (u64, u64) {
    let result: Result<(bool, u64, u64, u64, u64, u64), Error> = fixture
        .query_candid("entity_creation_startup_snapshot", ())
        .expect("startup snapshot should decode");
    let (ready, samples, work, scheduler, started, completed) =
        result.expect("startup should remain observable");
    eprintln!(
        "creation lifecycle instructions: label={label} ready={ready} work_samples={samples} work_total={work} scheduler_total={scheduler} started={started} completed={completed}"
    );
    (started, completed)
}

// Every scenario starts with identical source Wasm, fixed rows and write history.
fn seed(wasm: &[u8]) -> (StandaloneCanisterFixture, RowProjectionOutput) {
    let fixture = install_prebuilt_fixture_canister("sql", wasm.to_vec());
    projection(&fixture, "SELECT id FROM Item");
    for method in ["seed_entity_rename", "seed_entity_creation_target"] {
        let result: Result<(), Error> = fixture
            .update_candid(method, ())
            .expect("source seeding should decode");
        result.expect("fixed source rows should seed");
    }
    // Fold source writes outside all upgrade intervals.
    deliver_fixture_startup_watchdog(&fixture);
    let rows = projection(&fixture, ROWS);
    assert_eq!(rows.rows.len(), 3);
    (fixture, rows)
}

fn raw_upgrade(fixture: &StandaloneCanisterFixture, wasm: &[u8], label: &str) {
    let before = cycles(fixture);
    fixture
        .pocket_ic()
        .upgrade_canister(
            fixture.canister_id(),
            wasm.to_vec(),
            candid::encode_args(()).expect("empty args should encode"),
            None,
        )
        .expect("matched upgrade should succeed");
    report_cycles(fixture, before, &format!("{label}/upgrade-api"));
    snapshot(fixture, &format!("{label}/after-upgrade-api"));
}

fn natural_upgrade(fixture: &StandaloneCanisterFixture, wasm: &[u8], label: &str) {
    raw_upgrade(fixture, wasm, label);
    let before = cycles(fixture);
    deliver_fixture_startup_watchdog(fixture);
    report_cycles(fixture, before, &format!("{label}/watchdog-delivery"));
    snapshot(fixture, &format!("{label}/after-watchdog"));
    // Finish callbacks and deterministic execution slices without advancing
    // the retry cadence. Pending reservations are not burned cycles.
    let before = cycles(fixture);
    deliver_startup_watchdog_message(fixture);
    report_cycles(fixture, before, &format!("{label}/zero-time-drain"));
    let (started, completed) = snapshot(fixture, &format!("{label}/after-drain"));
    assert_eq!(started, completed, "observed work callbacks should finish");
    let owners = lifecycle_metrics(fixture);
    report_lifecycle(&owners, label);
    assert!(owners.runtime_compilation().samples() > 0);
    assert!(owners.cardinality().samples() > 0);
    let before = cycles(fixture);
    projection(fixture, ROWS);
    report_cycles(fixture, before, &format!("{label}/readiness"));
    assert_eq!(lifecycle_metrics(fixture), owners);
}

fn assert_rows(fixture: &StandaloneCanisterFixture, rows: &RowProjectionOutput, added: bool) {
    assert_eq!(projection(fixture, ROWS), *rows);
    assert_eq!(projection(fixture, "SELECT id FROM Holder").rows.len(), 1);
    if added {
        assert!(projection(fixture, "SELECT id FROM Quest").rows.is_empty());
    }
    let result: Result<(), Error> = fixture
        .query_candid("check_entity_rename_bindings", ())
        .expect("old generated bindings should decode");
    result.expect("old generated bindings should remain valid");
}

// Natural callbacks remain enabled. The snapshot reveals any work already
// completed inside the host upgrade envelope; it is never charged twice here.
fn natural_case(source: &[u8], target: &[u8], label: &str, added: bool) {
    let (fixture, rows) = seed(source);
    let source_head = migration_status(&fixture).accepted_head().clone();
    natural_upgrade(&fixture, target, label);
    let target_head = migration_status(&fixture).accepted_head().clone();
    assert_eq!(target_head == source_head, !added);
    assert_rows(&fixture, &rows, added);
    natural_upgrade(&fixture, target, &format!("{label}/same-wasm-restart"));
    assert_eq!(migration_status(&fixture).accepted_head(), &target_head);
    assert_rows(&fixture, &rows, added);
}

// Probe application admission, then measure maintained driver pages and an
// exact application replay. Recovery can own publication before the independent
// application endpoint is admitted; rejected work is reported rather than
// mislabelled as the initial publication cost.
fn application_case(source: &[u8], target: &[u8], label: &str, added: bool) {
    let (fixture, rows) = seed(source);
    let source_head = migration_status(&fixture).accepted_head().clone();
    raw_upgrade(&fixture, target, label);
    let application_and_recovery_before = cycles(&fixture);
    let before = cycles(&fixture);
    let (result, instructions, owners): (Result<(), Error>, u64, SchemaLifecycleMetrics) =
        measured_update(&fixture, "measure_entity_creation_schema_application");
    report_lifecycle(&owners, &format!("{label}/before-recovery"));
    report_cycles(&fixture, before, &format!("{label}/schema-application"));
    assert!(instructions > 0);
    eprintln!(
        "creation lifecycle application: label={label} phase=before-recovery instructions={instructions} outcome={result:?}"
    );
    if let Err(error) = result {
        assert_eq!(
            error.code(),
            ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
        );
    }
    let mut terminal = false;
    for page in 0..32 {
        let before = cycles(&fixture);
        let (result, instructions, owners): (
            Result<(bool, bool, bool), Error>,
            u64,
            SchemaLifecycleMetrics,
        ) = measured_update(&fixture, "measure_entity_creation_startup_step");
        report_lifecycle(&owners, &format!("{label}/driver-page-{page}"));
        report_cycles(&fixture, before, &format!("{label}/driver-page-{page}"));
        assert!(instructions > 0);
        eprintln!(
            "creation lifecycle driver: label={label} page={page} instructions={instructions} outcome={result:?}"
        );
        match result {
            Ok((before_ready, after_ready, complete)) => {
                if !before_ready && after_ready {
                    assert_query_keeps_lifecycle_metrics(&fixture);
                }
                if complete {
                    assert!(after_ready);
                    terminal = true;
                    break;
                }
            }
            Err(error) => {
                assert_eq!(
                    error.code(),
                    ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
                );
                // Entropy replies may be pending; zero-time ticks preserve the
                // retry cadence while allowing already queued replies to finish.
                let before = cycles(&fixture);
                for _ in 0..4 {
                    fixture.pocket_ic().tick();
                }
                report_cycles(
                    &fixture,
                    before,
                    &format!("{label}/driver-page-{page}-drain"),
                );
            }
        }
    }
    assert!(terminal, "bounded canonical driver should become quiescent");
    assert_eq!(
        migration_status(&fixture).accepted_head() == &source_head,
        !added
    );
    let before = cycles(&fixture);
    let (result, instructions, owners): (Result<(), Error>, u64, SchemaLifecycleMetrics) =
        measured_update(&fixture, "measure_entity_creation_schema_application");
    report_lifecycle(&owners, &format!("{label}/after-replay"));
    report_cycles(&fixture, before, &format!("{label}/application-replay"));
    eprintln!(
        "creation lifecycle application: label={label} phase=ready-replay instructions={instructions} outcome={result:?}"
    );
    result.expect("exact generated application replay should succeed");
    assert_rows(&fixture, &rows, added);
    deliver_startup_watchdog_message(&fixture);
    let (started, completed) = snapshot(&fixture, &format!("{label}/after-explicit-pages"));
    assert_eq!(started, completed, "explicit probe callbacks should finish");
    report_cycles(
        &fixture,
        application_and_recovery_before,
        &format!("{label}/application-recovery-replay-total"),
    );
}

#[test]
#[ignore = "manual matched Debug/release lifecycle cycle and instruction attribution"]
fn matched_entity_creation_lifecycle_costs() {
    // Finish all builds before constructing instances; retain exact immutable
    // bytes through each read and every subsequent install/upgrade.
    let artifacts = [CanisterWasmProfile::Debug, CanisterWasmProfile::WasmRelease].map(|profile| {
        let wasms = build_entity_creation_lifecycle_fixture_wasms(profile)
            .expect("matched lifecycle actors should build");
        for (label, wasm) in ["source", "unchanged-schema", "additive"]
            .into_iter()
            .zip(&wasms)
        {
            eprintln!(
                "creation lifecycle artifact: profile={} label={label} raw_wasm_bytes={} blake3={}",
                profile.as_str(),
                wasm.len(),
                blake3::hash(wasm)
            );
        }
        assert_ne!(wasms[0], wasms[1], "control must have distinct Wasm");
        (profile, wasms)
    });
    for (profile, [source, control, additive]) in artifacts {
        for (case, target, added) in [
            ("same-source", &source, false),
            ("unchanged-schema", &control, false),
            ("additive", &additive, true),
        ] {
            natural_case(
                &source,
                target,
                &format!("{}/{case}", profile.as_str()),
                added,
            );
        }
        for (case, target, added) in [
            ("application-control", &control, false),
            ("application-additive", &additive, true),
        ] {
            application_case(
                &source,
                target,
                &format!("{}/{case}", profile.as_str()),
                added,
            );
        }
    }
}
