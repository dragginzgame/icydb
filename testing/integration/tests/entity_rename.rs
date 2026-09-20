//! Generated actor qualification for the E2 Item/Holder workload.
//! Reuses the migration closeout's build, controller and lifecycle owners.

use super::{migration_command, migration_status, projection, query_sql, upgrade};
use candid::CandidType;
use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error, ErrorCode,
    db::{
        EntitySchemaDescription, RowProjectionOutput, SchemaMigrationCommand, SchemaMigrationPhase,
        sql::SqlQueryResult,
    },
    value::OutputValue,
};
use icydb_testing_integration::{
    build_entity_rename_fixture_wasms, install_prebuilt_fixture_canister,
};
use serde::Deserialize;
use std::sync::OnceLock;

const SOURCE_ROWS: &str = "SELECT id, key, label, parent_id FROM Item ORDER BY id";
const SUCCESSOR_ROWS: &str = "SELECT id, key, label, parent_id FROM CatalogItem ORDER BY id";
const HOLDER_ROWS: &str = "SELECT id, item_id FROM Holder ORDER BY id";

#[derive(CandidType, Deserialize)]
struct QueryMeasurement {
    result: Result<SqlQueryResult, Error>,
    local_instructions: u64,
}

#[derive(CandidType, Deserialize)]
struct CatalogMeasurement {
    description: EntitySchemaDescription,
    local_instructions: u64,
}

fn artifacts() -> &'static (Vec<u8>, Vec<u8>) {
    static WASMS: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
    WASMS.get_or_init(|| {
        let wasms = build_entity_rename_fixture_wasms().expect("rename actors should build");
        for (label, wasm) in [("source", &wasms.0), ("successor", &wasms.1)] {
            eprintln!(
                "entity-rename {label}: raw_wasm_bytes={} blake3={}",
                wasm.len(),
                blake3::hash(wasm)
            );
        }
        wasms
    })
}

fn update_sql(fixture: &StandaloneCanisterFixture, sql: &str) -> Result<SqlQueryResult, Error> {
    fixture
        .update_candid("icydb_update", (sql.to_string(),))
        .expect("SQL update response should decode")
}

fn measured_rows(
    fixture: &StandaloneCanisterFixture,
    sql: &str,
    label: &str,
) -> RowProjectionOutput {
    // Readiness delivery is outside the existing query instruction interval.
    projection(fixture, sql);
    let measured: QueryMeasurement = fixture
        .query_candid("measure_sql_query_instructions", (sql.to_string(),))
        .expect("query instruction response should decode");
    assert!(measured.local_instructions > 0);
    let SqlQueryResult::Projection(rows) = measured.result.expect("measured read should succeed")
    else {
        panic!("expected a row projection");
    };
    let encoded = candid::encode_args((&rows,)).expect("projection should encode");
    eprintln!(
        "entity-rename read: label={label} local_instructions={} projection_blake3={}",
        measured.local_instructions,
        blake3::hash(&encoded)
    );
    rows
}

fn assert_rows(fixture: &StandaloneCanisterFixture) {
    let rows = projection(fixture, SUCCESSOR_ROWS);
    assert_eq!(rows.rows, expected_rows());
    assert_eq!(
        projection(fixture, HOLDER_ROWS).rows,
        vec![vec![OutputValue::nat64(10), OutputValue::nat64(2)]]
    );
    assert_eq!(
        projection(fixture, "SELECT id FROM CatalogItem WHERE key = 102").rows,
        vec![vec![OutputValue::nat64(2)]]
    );
    assert_bindings(fixture);
}

fn assert_bindings(fixture: &StandaloneCanisterFixture) {
    let result: Result<(), Error> = fixture
        .query_candid("check_entity_rename_bindings", ())
        .expect("typed binding check should decode");
    result.expect("generated bindings should decode the exact populated rows");
}

fn expected_rows() -> Vec<Vec<OutputValue>> {
    vec![
        vec![
            OutputValue::nat64(1),
            OutputValue::nat64(101),
            OutputValue::nat64(201),
            OutputValue::null(),
        ],
        vec![
            OutputValue::nat64(2),
            OutputValue::nat64(102),
            OutputValue::nat64(202),
            OutputValue::nat64(1),
        ],
    ]
}

fn assert_constraints(fixture: &StandaloneCanisterFixture) {
    assert_rows(fixture);
    let catalog_before = measured_catalog(fixture, "before-rejected-deletes");
    let deletes: Result<Vec<Error>, Error> = fixture
        .update_candid("check_entity_rename_deletes", ())
        .expect("delete checks should decode");
    let deletes = deletes.expect("both deletes should reject");
    assert_eq!(deletes.len(), 2);
    for error in deletes {
        assert_eq!(
            error.code(),
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
        );
    }
    // The update returns normally after catching both constraint errors. It
    // preserves rows and accepted authority while retaining heap preparation;
    // the earlier query-only calls cannot retain that preparation across calls.
    assert_eq!(
        measured_catalog(fixture, "after-rejected-deletes"),
        catalog_before
    );
    assert_eq!(
        measured_rows(fixture, SUCCESSOR_ROWS, "after-rejected-deletes").rows,
        expected_rows()
    );
    // Both reverse domains and both target checks remain enforced after rename.
    for sql in [
        "UPDATE Holder SET item_id = 999 WHERE id = 10",
        "UPDATE CatalogItem SET parent_id = 999 WHERE id = 2",
        "UPDATE CatalogItem SET key = 101 WHERE id = 2",
    ] {
        assert_eq!(
            update_sql(fixture, sql)
                .expect_err("maintained constraint must reject")
                .code(),
            ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
        );
    }
    assert_rows(fixture);
}

fn measured_catalog(fixture: &StandaloneCanisterFixture, label: &str) -> EntitySchemaDescription {
    let measured: Result<CatalogMeasurement, Error> = fixture
        .query_candid(
            "measure_accepted_schema_read_instructions",
            ("CatalogItem".to_string(),),
        )
        .expect("catalog instruction response should decode");
    let measured = measured.expect("accepted catalog should remain readable");
    assert!(measured.local_instructions > 0);
    assert_eq!(measured.description.entity_name(), "CatalogItem");
    eprintln!(
        "entity-rename catalog: label={label} local_instructions={}",
        measured.local_instructions,
    );
    measured.description
}

// A metadata rename publishes in one atomic command; there is no artificial
// Prepared/rewrite phase at which to interrupt. Restart on either side instead.
fn rehearse(restart_before: bool) {
    let fixture = install_prebuilt_fixture_canister("sql", artifacts().0.clone());
    projection(&fixture, SOURCE_ROWS);
    let seeded: Result<(), Error> = fixture
        .update_candid("seed_entity_rename", ())
        .expect("seed response should decode");
    seeded.expect("fixed rows should insert");
    assert_bindings(&fixture);
    let source_rows = measured_rows(&fixture, SOURCE_ROWS, "source");
    assert_eq!(source_rows.rows, expected_rows());
    let before = migration_status(&fixture);
    assert_eq!(before.phase(), SchemaMigrationPhase::Adopted);
    eprintln!(
        "entity-rename seed call: input_blake3={}",
        blake3::hash(&candid::encode_args(()).unwrap())
    );

    upgrade(&fixture, artifacts().1.clone());
    let pending = migration_status(&fixture);
    assert_eq!(pending.phase(), SchemaMigrationPhase::Idle);
    assert_eq!(pending.accepted_head(), before.accepted_head());
    assert_eq!(pending.database_identity(), before.database_identity());
    assert_eq!(pending.transitions().len(), 2);
    for transition in pending.transitions() {
        assert_eq!(transition.from_version(), Some(1));
        assert_eq!(transition.to_version(), 2);
    }
    assert_eq!(
        query_sql(&fixture, SUCCESSOR_ROWS)
            .expect_err("successor must wait for explicit migration")
            .code(),
        ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING
    );
    if restart_before {
        upgrade(&fixture, artifacts().1.clone());
        assert_eq!(migration_status(&fixture), pending);
    }
    let command = SchemaMigrationCommand::Advance {
        expected_database: pending.database_identity(),
        expected_head: pending.accepted_head().clone(),
        expected_plan: pending.plan_digest().expect("successor has a plan"),
        acknowledged_finding_page: None,
    };
    eprintln!(
        "entity-rename advance: input_blake3={}",
        blake3::hash(&candid::encode_args((&command,)).unwrap())
    );
    let applied = migration_command(&fixture, command.clone());
    assert_eq!(applied.phase(), SchemaMigrationPhase::Applied);
    assert_eq!(applied.rows_rewritten(), 0);
    assert_eq!(applied.indexes_rebuilt(), 0);
    let receipt = applied
        .terminal_receipt()
        .expect("metadata publication has a receipt");
    assert_eq!(receipt.prior_head(), before.accepted_head());
    assert_eq!(receipt.accepted_head(), applied.accepted_head());
    assert_ne!(applied.accepted_head(), before.accepted_head());
    assert_eq!(applied.database_identity(), before.database_identity());
    // Simulate a lost response by ignoring the first result for retry input:
    // repeat the exact predecessor-head command, not a newly derived command.
    assert_eq!(migration_command(&fixture, command.clone()), applied);
    let successor_rows = measured_rows(&fixture, SUCCESSOR_ROWS, "successor");
    // The projection's public entity label must change; values and columns must not.
    let mut expected_successor = source_rows;
    assert_eq!(expected_successor.entity, "Item");
    expected_successor.entity = "CatalogItem".to_string();
    assert_eq!(successor_rows, expected_successor);
    // Separate query messages do not retain heap-cache preparation. Keep the
    // repeated read before any ordinary update to expose that lifecycle cost.
    assert_eq!(
        measured_rows(&fixture, SUCCESSOR_ROWS, "successor-query-repeat"),
        expected_successor
    );
    assert_constraints(&fixture);
    assert_eq!(
        measured_rows(&fixture, SUCCESSOR_ROWS, "successor-after-updates"),
        expected_successor
    );
    upgrade(&fixture, artifacts().1.clone());
    assert_eq!(migration_command(&fixture, command), applied);
    assert_eq!(migration_status(&fixture), applied);
    assert_eq!(
        measured_rows(&fixture, SUCCESSOR_ROWS, "restarted-before-updates"),
        expected_successor
    );
    assert_constraints(&fixture);
    assert_eq!(
        measured_rows(&fixture, SUCCESSOR_ROWS, "restarted"),
        expected_successor
    );
}

#[test]
fn populated_entity_rename_preserves_relations_and_replays_after_upgrade() {
    rehearse(false);
}

#[test]
fn populated_entity_rename_waits_through_restart_then_replays_exact_command() {
    rehearse(true);
}
