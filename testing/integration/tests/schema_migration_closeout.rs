//! Rehearse the maintained adjacent schema transition using current-format actors.
//! Owns disposable seeds and outcome assertions, not migration semantics.

use std::{sync::OnceLock, time::Duration};

use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error,
    db::{
        RowProjectionOutput, SchemaMigrationCommand, SchemaMigrationFindingKind,
        SchemaMigrationPhase, SchemaMigrationStatusPage, SchemaMigrationStatusRequest,
        sql::SqlQueryResult,
    },
    value::OutputValue,
};
use icydb_testing_integration::{
    build_schema_migration_fixture_wasms, deliver_fixture_startup_watchdog,
    install_prebuilt_fixture_canister,
};

const ADVANCE_LIMIT: usize = 32;
const SOURCE_ROWS: &str = "SELECT id, name, age, rank FROM SqlTestUser ORDER BY name ASC";
const SUCCESSOR_ROWS: &str = "SELECT id, name, age, score FROM SqlTestUser ORDER BY name ASC";

#[test]
fn maintained_migration_matches_uninterrupted_control_after_upgrade() {
    let (control, control_seed) = seeded_fixture();
    let control_status = complete_migration(&control, None);
    let control_rows = assert_migrated_rows(&control, &control_seed);

    let (interrupted, interrupted_seed) = seeded_fixture();
    let interrupted_status =
        complete_migration(&interrupted, Some(SchemaMigrationPhase::Validating));
    let interrupted_rows = assert_migrated_rows(&interrupted, &interrupted_seed);

    // IDs are generated per installation: each run preserves its own IDs, while
    // the two runs must agree on all logical values and accepted schema heads.
    for (control_row, interrupted_row) in control_rows.rows.iter().zip(&interrupted_rows.rows) {
        assert_eq!(&control_row[1..], &interrupted_row[1..]);
    }
    assert_eq!(
        control_status.accepted_head(),
        interrupted_status.accepted_head()
    );
    assert_eq!(
        control_status.transitions(),
        interrupted_status.transitions()
    );
}

#[test]
fn prepared_restart_waits_without_background_work_and_resumes() {
    let (fixture, seed) = seeded_fixture();
    complete_migration(&fixture, Some(SchemaMigrationPhase::Prepared));
    assert_migrated_rows(&fixture, &seed);
}

#[test]
fn rejected_checked_cast_preserves_source_rows_and_can_abort() {
    let (fixture, seed) = seeded_fixture();
    let id = &seed.rendered_rows()[0][0];
    let updated: Result<SqlQueryResult, Error> = fixture
        .update_candid(
            "icydb_update",
            (format!(
                "UPDATE SqlTestUser SET age = 65536 WHERE id = '{id}'"
            ),),
        )
        .expect("source update response should decode");
    updated.expect("out-of-Nat16 value is valid Int32 source data");
    let source_rows = projection(&fixture, SOURCE_ROWS);
    assert_eq!(source_rows.rows[0][2], OutputValue::int64(65536));
    let source_status = migration_status(&fixture);

    upgrade(&fixture, artifacts().1.clone());
    let mut status = migration_status(&fixture);
    let plan = status.plan_digest();
    for _ in 0..ADVANCE_LIMIT {
        if status.phase() == SchemaMigrationPhase::Rejected {
            break;
        }
        status = advance(&fixture, &status);
        assert_eq!(status.accepted_head(), source_status.accepted_head());
        assert_eq!(status.plan_digest(), plan);
        assert_eq!(status.rows_rewritten(), 0);
        assert_eq!(status.indexes_rebuilt(), 0);
    }
    assert_eq!(status.phase(), SchemaMigrationPhase::Rejected);
    assert_eq!(status.rows_validated(), 3);
    assert_eq!(status.findings().len(), 1);
    assert_eq!(
        status.findings()[0].kind(),
        SchemaMigrationFindingKind::Transform
    );
    assert!(!status.findings()[0].primary_key().is_empty());
    assert!(
        status.next_cursor().is_none(),
        "one finding fits in one bounded page"
    );
    assert!(status.terminal_receipt().is_none());

    // Rejected validation is before the physical rewrite boundary. Use the
    // maintained Abort command, never reinstall or decode memory in the test.
    status = migration_command(
        &fixture,
        SchemaMigrationCommand::Abort {
            expected_database: status.database_identity(),
            expected_head: status.accepted_head().clone(),
            expected_plan: plan.expect("rejected migration retains its plan"),
        },
    );
    assert_eq!(status.phase(), SchemaMigrationPhase::Aborted);
    assert_eq!(status.accepted_head(), source_status.accepted_head());
    // Abort preserves source authority, not the successor's generated API.
    // Restore the exact current-format source actor before reading that API.
    upgrade(&fixture, artifacts().0.clone());
    assert_eq!(projection(&fixture, SOURCE_ROWS), source_rows);
}

fn artifacts() -> &'static (Vec<u8>, Vec<u8>) {
    static WASMS: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
    WASMS.get_or_init(|| {
        let wasms = build_schema_migration_fixture_wasms().expect("migration actors should build");
        for (label, wasm) in [("source", &wasms.0), ("successor", &wasms.1)] {
            eprintln!(
                "migration {label}: raw_wasm_bytes={} blake3={}",
                wasm.len(),
                blake3::hash(wasm)
            );
        }
        wasms
    })
}

fn seeded_fixture() -> (StandaloneCanisterFixture, RowProjectionOutput) {
    // Finish both builds before starting PocketIC, avoiding its idle timeout
    // during a cold compilation. OnceLock shares only immutable artifact bytes.
    let fixture = install_prebuilt_fixture_canister("sql", artifacts().0.clone());
    await_ready_query(&fixture, SOURCE_ROWS).expect("fresh source fixture should become ready");
    load_fixtures(&fixture);
    let adopted = migration_status(&fixture);
    assert_eq!(adopted.phase(), SchemaMigrationPhase::Adopted);
    assert!(adopted.plan_digest().is_none());
    let seed = projection(&fixture, SOURCE_ROWS);
    assert_eq!(seed.row_count, 3);
    (fixture, seed)
}

fn complete_migration(
    fixture: &StandaloneCanisterFixture,
    interrupt_at: Option<SchemaMigrationPhase>,
) -> SchemaMigrationStatusPage {
    let source_status = migration_status(fixture);
    upgrade(fixture, artifacts().1.clone());
    let mut status = migration_status(fixture);
    assert_eq!(status.phase(), SchemaMigrationPhase::Idle);
    assert!(status.plan_digest().is_some());
    assert_eq!(status.transitions().len(), 1);
    assert_eq!(status.transitions()[0].from_version(), Some(1));
    assert_eq!(status.transitions()[0].to_version(), 2);
    assert_eq!(
        query_sql(fixture, SOURCE_ROWS)
            .expect_err("a deployed successor must wait for explicit migration")
            .code(),
        icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
    );
    assert_waiting_without_background_work(fixture);

    let mut resumed_after_upgrade = false;
    let plan = status.plan_digest();
    for _ in 0..ADVANCE_LIMIT {
        if status.phase() == SchemaMigrationPhase::Applied {
            break;
        }

        eprintln!(
            "advancing migration from {:?}: validated={} rewritten={} indexes={}",
            status.phase(),
            status.rows_validated(),
            status.rows_rewritten(),
            status.indexes_rebuilt(),
        );
        status = advance(fixture, &status);
        assert_eq!(
            status.database_identity(),
            source_status.database_identity()
        );
        assert_eq!(status.plan_digest(), plan);
        assert!(status.findings().is_empty());
        assert!(status.next_cursor().is_none());
        if database_is_gated(status.phase()) {
            assert_eq!(status.accepted_head(), source_status.accepted_head());
            let unavailable = query_sql(fixture, "SELECT name FROM SqlTestUser ORDER BY name ASC")
                .expect_err("ordinary reads must be unavailable during physical migration");
            assert_eq!(
                unavailable.code(),
                icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
                "applications must branch on typed startup readiness, not generic conflict",
            );
        }
        if interrupt_at == Some(status.phase()) && !resumed_after_upgrade {
            let before_upgrade = status.clone();
            upgrade(fixture, artifacts().1.clone());
            status = migration_status(fixture);
            assert_eq!(status, before_upgrade, "upgrade must resume exact progress");
            assert_eq!(
                query_sql(fixture, SOURCE_ROWS)
                    .expect_err("restart must not admit the unpublished successor")
                    .code(),
                icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING,
            );
            assert_waiting_without_background_work(fixture);
            assert_eq!(migration_status(fixture), before_upgrade);
            resumed_after_upgrade = true;
        }
    }

    assert_eq!(resumed_after_upgrade, interrupt_at.is_some());
    assert_eq!(status.phase(), SchemaMigrationPhase::Applied);
    assert_eq!(status.rows_validated(), 3);
    assert_eq!(status.rows_rewritten(), 3);
    assert!(status.indexes_rebuilt() >= 1);
    let receipt = status
        .terminal_receipt()
        .expect("applied migration should expose its terminal receipt");
    assert_eq!(receipt.database_identity(), status.database_identity());
    assert_eq!(receipt.plan_digest(), status.plan_digest());
    assert_eq!(receipt.accepted_head(), status.accepted_head());
    assert_eq!(receipt.prior_head(), source_status.accepted_head());
    assert_ne!(status.accepted_head(), source_status.accepted_head());
    status
}

// No actor instrumentation is needed: this isolated subnet contains only our
// fixture, so an update during simulated idle rounds is unintended background work.
fn assert_waiting_without_background_work(fixture: &StandaloneCanisterFixture) {
    let pic = fixture.pocket_ic();
    let subnet = pic
        .get_subnet(fixture.canister_id())
        .expect("fixture subnet");
    // Drain an already queued shared timer delivery before measuring a second
    // idle window; stopping a registration does not undo a scheduled IC message.
    pic.advance_time(Duration::from_secs(2));
    for _ in 0..8 {
        pic.tick();
    }
    let before = pic.get_subnet_metrics(subnet).expect("subnet metrics");
    let cycles_before = pic.cycle_balance(fixture.canister_id());
    pic.advance_time(Duration::from_secs(2));
    for _ in 0..8 {
        pic.tick();
    }
    let after = pic.get_subnet_metrics(subnet).expect("subnet metrics");
    let cycles_after = pic.cycle_balance(fixture.canister_id());
    eprintln!(
        "migration idle window: phase={:?} updates={} cycles={}",
        migration_status(fixture).phase(),
        after.update_transactions_total - before.update_transactions_total,
        cycles_before - cycles_after,
    );
    assert_eq!(
        after.update_transactions_total, before.update_transactions_total,
        "waiting for controller migration commands must not busy-retry the watchdog",
    );
}

fn assert_migrated_rows(
    fixture: &StandaloneCanisterFixture,
    seed: &RowProjectionOutput,
) -> RowProjectionOutput {
    let rows = projection(fixture, SUCCESSOR_ROWS);
    assert_eq!(rows.columns, ["id", "name", "age", "score"]);
    assert_eq!(rows.row_count, 3);
    assert_eq!(rows.rows.len(), 3);
    for ((row, source), (name, age, score)) in rows.rows.iter().zip(&seed.rows).zip([
        ("alice", 31, 28),
        ("bob", 24, 25),
        ("charlie", 43, 43),
    ]) {
        assert_eq!(
            row,
            &vec![
                source[0].clone(),
                OutputValue::text(name.to_string()),
                OutputValue::nat64(age),
                OutputValue::int64(score)
            ]
        );
    }
    let indexed = projection(
        fixture,
        "SELECT id, name, age, score FROM SqlTestUser WHERE name = 'alice'",
    );
    assert_eq!(indexed.rows, rows.rows[..1]);
    rows
}

fn projection(fixture: &StandaloneCanisterFixture, sql: &str) -> RowProjectionOutput {
    let SqlQueryResult::Projection(rows) =
        await_ready_query(fixture, sql).expect("fixture projection should succeed")
    else {
        panic!("expected row projection");
    };
    rows
}

fn load_fixtures(fixture: &StandaloneCanisterFixture) {
    let loaded: Result<(), Error> = fixture
        .update_candid("icydb_fixtures_load", ())
        .expect("fixture load response should decode");
    loaded.expect("fixture load should succeed");
}

fn upgrade(fixture: &StandaloneCanisterFixture, wasm: Vec<u8>) {
    let args = candid::encode_args(()).expect("empty upgrade arguments should encode");
    fixture
        .pocket_ic()
        .upgrade_canister(fixture.canister_id(), wasm, args, None)
        .expect("fixture upgrade should succeed");
    deliver_fixture_startup_watchdog(fixture);
}

fn migration_status(fixture: &StandaloneCanisterFixture) -> SchemaMigrationStatusPage {
    let result: Result<SchemaMigrationStatusPage, Error> = fixture
        .query_candid(
            "icydb_schema_migration",
            (SchemaMigrationStatusRequest::default(),),
        )
        .expect("migration status response should decode");
    result.expect("migration status should succeed")
}

fn migration_command(
    fixture: &StandaloneCanisterFixture,
    command: SchemaMigrationCommand,
) -> SchemaMigrationStatusPage {
    // Host-only whole-call charge, including ingress and any work scheduled
    // during the call. This is not a measurement of the migration body alone.
    let before = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let result: Result<SchemaMigrationStatusPage, Error> = fixture
        .update_candid("icydb_schema_migrate", (command,))
        .expect("migration command response should decode");
    let after = fixture.pocket_ic().cycle_balance(fixture.canister_id());
    let status = result.expect("migration command should succeed");
    let cycles = before
        .checked_sub(after)
        .expect("fixture does not receive cycles during migration");
    eprintln!(
        "migration command: scenario={} phase={:?} call_envelope_cycles={cycles}",
        std::thread::current().name().unwrap_or("unnamed"),
        status.phase(),
    );
    status
}

fn advance(
    fixture: &StandaloneCanisterFixture,
    status: &SchemaMigrationStatusPage,
) -> SchemaMigrationStatusPage {
    migration_command(
        fixture,
        SchemaMigrationCommand::Advance {
            expected_database: status.database_identity(),
            expected_head: status.accepted_head().clone(),
            expected_plan: status
                .plan_digest()
                .expect("deployed v2 fixture should expose its migration plan"),
            acknowledged_finding_page: None,
        },
    )
}

fn query_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    let result: Result<SqlQueryResult, Error> = fixture
        .query_candid("icydb_query", (sql.to_string(),))
        .expect("SQL query response should decode");
    result
}

fn await_ready_query(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
) -> Result<SqlQueryResult, Error> {
    for _ in 0..8 {
        match query_sql(fixture, sql) {
            Ok(result) => return Ok(result),
            Err(error)
                if error.code()
                    == icydb::ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING =>
            {
                fixture.pocket_ic().advance_time(Duration::from_secs(1));
                for _ in 0..4 {
                    fixture.pocket_ic().tick();
                }
            }
            Err(error) => return Err(error),
        }
    }
    query_sql(fixture, sql)
}

const fn database_is_gated(phase: SchemaMigrationPhase) -> bool {
    matches!(
        phase,
        SchemaMigrationPhase::Validating
            | SchemaMigrationPhase::ReadyToRewrite
            | SchemaMigrationPhase::RewritingRows
            | SchemaMigrationPhase::RebuildingIndexes
            | SchemaMigrationPhase::FinalValidation
            | SchemaMigrationPhase::Publishing
    )
}
