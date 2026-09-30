//! Generated populated-canister creation and mixed-migration qualification.

use super::{migration_command, migration_status, projection, query_sql};
use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error, ErrorCode,
    db::{
        EntitySchemaDescription, RowProjectionOutput, SchemaMigrationCommand, SchemaMigrationPhase,
        SchemaMigrationStatusPage, sql::SqlQueryResult,
    },
    value::OutputValue,
};
use icydb_testing_integration::{
    build_entity_creation_fixture_wasms, install_prebuilt_fixture_canister,
};
use serde::Deserialize;
use std::sync::OnceLock;

#[derive(CandidType, Deserialize)]
struct Measurement<T> {
    result: Result<T, Error>,
    local_instructions: u64,
}

#[derive(CandidType, Deserialize)]
struct CatalogMeasurement {
    description: EntitySchemaDescription,
    local_instructions: u64,
}

#[derive(Clone, Copy, Debug)]
enum Successor {
    Additive,
    Metadata,
    Physical,
}

impl Successor {
    const fn artifact(self) -> usize {
        match self {
            Self::Additive => 1,
            Self::Metadata => 2,
            Self::Physical => 3,
        }
    }

    const fn item(self) -> &'static str {
        match self {
            Self::Metadata => "CatalogItem",
            Self::Additive | Self::Physical => "Item",
        }
    }
}

fn artifacts() -> &'static [Vec<u8>; 4] {
    static WASMS: OnceLock<[Vec<u8>; 4]> = OnceLock::new();
    WASMS.get_or_init(|| {
        let wasms = build_entity_creation_fixture_wasms().expect("creation actors should build");
        for (label, wasm) in ["source", "additive", "metadata", "physical"]
            .into_iter()
            .zip(&wasms)
        {
            eprintln!(
                "entity-creation artifact: label={label} raw_wasm_bytes={} blake3={}",
                wasm.len(),
                blake3::hash(wasm)
            );
        }
        wasms
    })
}

fn cycles(fixture: &StandaloneCanisterFixture) -> u128 {
    fixture.pocket_ic().cycle_balance(fixture.canister_id())
}

fn report(fixture: &StandaloneCanisterFixture, before: u128, label: &str, instructions: u64) {
    assert!(instructions > 0);
    eprintln!(
        "entity-creation request: label={label} local_instructions={instructions} delivery_cycles={}",
        before
            .checked_sub(cycles(fixture))
            .expect("fixture receives no cycles")
    );
}

fn measured_rows(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
    label: &str,
) -> RowProjectionOutput {
    let before = cycles(fixture);
    let measured: Measurement<SqlQueryResult> = fixture
        .query_candid("measure_sql_query_instructions", (sql.to_string(),))
        .expect("query measurement should decode");
    report(fixture, before, label, measured.local_instructions);
    let SqlQueryResult::Projection(rows) = measured.result.expect("accepted read should succeed")
    else {
        panic!("expected projection");
    };
    rows
}

fn measured_update(fixture: &StandaloneCanisterFixture, sql: &str, label: &str) {
    let before = cycles(fixture);
    let measured: Measurement<SqlQueryResult> = fixture
        .update_candid(
            "measure_trusted_sql_exact_update_instructions",
            (sql.to_string(),),
        )
        .expect("update measurement should decode");
    measured.result.expect("old entity update should succeed");
    report(fixture, before, label, measured.local_instructions);
}

fn catalog(fixture: &StandaloneCanisterFixture, entity: &str) -> EntitySchemaDescription {
    let measured: Result<CatalogMeasurement, Error> = fixture
        .query_candid(
            "measure_accepted_schema_read_instructions",
            (entity.to_string(),),
        )
        .expect("catalog measurement should decode");
    let measured = measured.expect("accepted catalog should be ready");
    assert!(measured.local_instructions > 0);
    measured.description
}

fn upgrade(fixture: &StandaloneCanisterFixture, successor: Successor, label: &str) {
    let before = cycles(fixture);
    super::upgrade(fixture, artifacts()[successor.artifact()].clone());
    eprintln!(
        "entity-creation lifecycle: successor={successor:?} label={label} upgrade_and_watchdog_cycles={}",
        before.checked_sub(cycles(fixture)).unwrap()
    );
}

fn seed() -> StandaloneCanisterFixture {
    let fixture = install_prebuilt_fixture_canister("sql", artifacts()[0].clone());
    projection(&fixture, "SELECT id FROM Item ORDER BY id");
    let seeded: Result<(), Error> = fixture
        .update_candid("seed_entity_rename", ())
        .expect("source seed should decode");
    seeded.expect("populated source should seed");
    let inserted: Result<(), Error> = fixture
        .update_candid("seed_entity_creation_target", ())
        .expect("source insert should decode");
    inserted.expect("unreferenced target row should insert");
    measured_update(
        &fixture,
        "UPDATE Item SET label = 304 WHERE id = 3",
        "source-old-update",
    );
    fixture
}

fn complete_mixed(
    fixture: &StandaloneCanisterFixture,
    successor: Successor,
) -> Option<(SchemaMigrationCommand, SchemaMigrationStatusPage)> {
    let pending = migration_status(fixture);
    if matches!(successor, Successor::Additive) {
        assert_eq!(pending.phase(), SchemaMigrationPhase::Adopted);
        assert!(pending.plan_digest().is_none());
        return None;
    }
    assert_eq!(pending.phase(), SchemaMigrationPhase::Idle);
    assert_eq!(
        query_sql(fixture, "SELECT id FROM Quest")
            .unwrap_err()
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
    );
    let command = SchemaMigrationCommand::Advance {
        expected_database: pending.database_identity(),
        expected_head: pending.accepted_head().clone(),
        expected_plan: pending.plan_digest().unwrap(),
        acknowledged_finding_page: None,
    };
    // Restart while metadata is Idle or physical work is Prepared. Startup
    // must preserve the explicit controller's ownership of progress.
    if matches!(successor, Successor::Physical) {
        assert_eq!(
            migration_command(fixture, command.clone()).phase(),
            SchemaMigrationPhase::Prepared
        );
    }
    let before_restart = migration_status(fixture);
    upgrade(fixture, successor, "pending-restart");
    assert_eq!(migration_status(fixture), before_restart);
    let mut status = migration_status(fixture);
    for _ in 0..32 {
        if status.phase() == SchemaMigrationPhase::Applied {
            break;
        }
        status = migration_command(fixture, command.clone());
        if status.phase() != SchemaMigrationPhase::Applied {
            assert_eq!(status.accepted_head(), pending.accepted_head());
            assert_eq!(
                query_sql(fixture, "SELECT id FROM Quest")
                    .unwrap_err()
                    .code(),
                ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
            );
        }
    }
    assert_eq!(status.phase(), SchemaMigrationPhase::Applied);
    assert_eq!(
        status.rows_rewritten(),
        if matches!(successor, Successor::Physical) {
            3
        } else {
            0
        }
    );
    assert_eq!(migration_command(fixture, command.clone()), status);
    Some((command, status))
}

fn assert_created(fixture: &StandaloneCanisterFixture, item: &str) {
    let before = cycles(fixture);
    let checked: Measurement<()> = fixture
        .query_candid("check_created_quest", ())
        .expect("generated named value read should decode");
    checked
        .result
        .expect("generated binding should decode added enum and record");
    report(
        fixture,
        before,
        "new-typed-read",
        checked.local_instructions,
    );
    assert_eq!(
        projection(
            fixture,
            "SELECT id, item_id, code FROM Quest WHERE code = 700"
        )
        .rows,
        vec![vec![
            OutputValue::nat64(7),
            OutputValue::nat64(3),
            OutputValue::nat64(700)
        ]]
    );
    let deleted: Result<Error, Error> = fixture
        .update_candid("check_created_quest_target_delete", ())
        .expect("delete response should decode");
    assert_eq!(
        deleted
            .expect("fixed delete control should catch constraint rejection")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
    );
    for (target, code) in [(3_u64, 700_u64), (999, 701)] {
        let rejected: Measurement<()> = fixture
            .update_candid("write_created_quest", (8_u64, target, code))
            .expect("rejected new write should decode");
        assert_eq!(
            rejected.result.unwrap_err().code(),
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
        );
    }
    assert_eq!(
        projection(fixture, "SELECT id FROM Quest ORDER BY id").rows,
        vec![vec![OutputValue::nat64(7)]]
    );
    assert_eq!(
        projection(fixture, &format!("SELECT id FROM {item} WHERE key = 102")).rows,
        vec![vec![OutputValue::nat64(2)]]
    );
    let checked: Result<(), Error> = fixture
        .query_candid("check_entity_rename_bindings", ())
        .expect("old typed bindings should decode");
    checked.expect("old generated bindings remain readable");
}

fn rehearse(successor: Successor) {
    let fixture = seed();
    let source_sql = "SELECT id, key, label, parent_id FROM Item ORDER BY id";
    let mut expected = measured_rows(&fixture, source_sql, "source-old-read");
    assert_eq!(expected.rows.len(), 3);
    let item_before = catalog(&fixture, "Item");
    let holder_before = catalog(&fixture, "Holder");
    let source_status = migration_status(&fixture);
    assert_eq!(source_status.phase(), SchemaMigrationPhase::Adopted);
    upgrade(&fixture, successor, "successor");
    if !matches!(successor, Successor::Additive) {
        assert_eq!(
            migration_status(&fixture).accepted_head(),
            source_status.accepted_head()
        );
    }
    let replay = complete_mixed(&fixture, successor);
    let target = migration_status(&fixture);
    assert_eq!(
        target.database_identity(),
        source_status.database_identity()
    );
    assert_ne!(target.accepted_head(), source_status.accepted_head());
    // Publication invalidates prepared runtime authority. Use the maintained
    // readiness driver before issuing measured queries against the new head.
    let before = cycles(&fixture);
    assert_eq!(projection(&fixture, "SELECT id FROM Quest").rows.len(), 0);
    eprintln!(
        "entity-creation readiness: successor={successor:?} delivery_cycles={}",
        before.checked_sub(cycles(&fixture)).unwrap()
    );
    let item_after = catalog(&fixture, successor.item());
    let holder_after = catalog(&fixture, "Holder");
    assert_eq!(item_after.entity_tag(), item_before.entity_tag());
    assert_eq!(holder_after.entity_tag(), holder_before.entity_tag());
    if matches!(successor, Successor::Additive) {
        assert_eq!(item_after, item_before);
        assert_eq!(holder_after, holder_before);
    }
    let quest = catalog(&fixture, "Quest");
    assert_ne!(quest.entity_tag(), item_after.entity_tag());
    assert_ne!(quest.entity_tag(), holder_after.entity_tag());
    let sql = format!(
        "SELECT id, key, label, parent_id FROM {} ORDER BY id",
        successor.item()
    );
    expected.entity = successor.item().into();
    assert_eq!(
        measured_rows(&fixture, &sql, "successor-old-read"),
        expected
    );
    if matches!(successor, Successor::Physical) {
        assert_eq!(
            projection(&fixture, "SELECT coins FROM Item ORDER BY id").rows,
            vec![vec![OutputValue::nat64(5)]; 3]
        );
    }
    let before = cycles(&fixture);
    let created: Measurement<()> = fixture
        .update_candid("write_created_quest", (7_u64, 3_u64, 700_u64))
        .expect("new write measurement should decode");
    created
        .result
        .expect("added indexed entity should accept named values");
    report(&fixture, before, "new-write", created.local_instructions);
    assert_created(&fixture, successor.item());
    measured_update(
        &fixture,
        &format!("UPDATE {} SET label = 305 WHERE id = 3", successor.item()),
        "successor-old-update",
    );
    expected.rows[2][2] = OutputValue::nat64(305);
    assert_eq!(projection(&fixture, &sql), expected);
    upgrade(&fixture, successor, "applied-restart");
    assert_eq!(catalog(&fixture, "Quest"), quest);
    assert_eq!(
        measured_rows(&fixture, &sql, "restarted-old-read"),
        expected
    );
    assert_created(&fixture, successor.item());
    if let Some((command, status)) = replay {
        assert_eq!(migration_command(&fixture, command), status);
    }
}

#[test]
fn populated_generated_entity_creation_reconciles_and_survives_restart() {
    rehearse(Successor::Additive);
}

#[test]
fn populated_generated_entity_creation_accompanies_metadata_rename() {
    rehearse(Successor::Metadata);
}

#[test]
fn populated_generated_entity_creation_accompanies_physical_migration() {
    rehearse(Successor::Physical);
}
