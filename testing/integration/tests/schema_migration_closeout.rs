use std::{env, fs, path::PathBuf};

use ic_testkit::pic::StandaloneCanisterFixture;
use icydb::{
    Error,
    db::{
        SchemaMigrationCommand, SchemaMigrationPhase, SchemaMigrationStatusPage,
        SchemaMigrationStatusRequest,
        sql::{SqlQueryPerfResult, SqlQueryResult},
    },
};
use icydb_testing_integration::install_prebuilt_fixture_canister;

const V1_WASM: &str = "ICYDB_MIGRATION_V1_WASM";
const V2_WASM: &str = "ICYDB_MIGRATION_V2_WASM";

#[test]
#[ignore = "release closeout deploys the maintained v1 and v2 SQL fixture Wasms"]
fn maintained_sql_fixture_migrates_v1_to_v2_and_resumes_after_upgrade() {
    let v1_wasm = read_wasm(V1_WASM);
    let v2_wasm = read_wasm(V2_WASM);
    let fixture = install_prebuilt_fixture_canister("sql", v1_wasm);

    load_fixtures(&fixture);
    let adopted = migration_status(&fixture);
    assert_eq!(adopted.phase(), SchemaMigrationPhase::Adopted);
    assert!(adopted.plan_digest().is_none());

    upgrade(&fixture, v2_wasm.clone());
    let mut status = migration_status(&fixture);
    assert_eq!(status.phase(), SchemaMigrationPhase::Idle);
    assert!(status.plan_digest().is_some());
    assert_eq!(status.transitions().len(), 1);
    assert_eq!(status.transitions()[0].from_version(), Some(1));
    assert_eq!(status.transitions()[0].to_version(), 2);

    let mut resumed_after_upgrade = false;
    for _ in 0..32 {
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
        status = advance(&fixture, &status);
        if !resumed_after_upgrade && database_is_gated(status.phase()) {
            let unavailable = query_sql(&fixture, "SELECT name FROM SqlTestUser ORDER BY name ASC");
            assert!(
                unavailable.is_err(),
                "ordinary reads must be unavailable during physical migration"
            );

            let before_upgrade = status.clone();
            upgrade(&fixture, v2_wasm.clone());
            status = migration_status(&fixture);
            assert_eq!(status, before_upgrade, "upgrade must resume exact progress");
            resumed_after_upgrade = true;
        }
    }

    assert!(
        resumed_after_upgrade,
        "fixture must prove gated upgrade recovery"
    );
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

    let migrated = query_sql(
        &fixture,
        "SELECT name, age, score FROM SqlTestUser ORDER BY name ASC",
    )
    .expect("v2 query should succeed after publication");
    let SqlQueryResult::Projection(rows) = migrated else {
        panic!("expected migrated row projection");
    };
    assert_eq!(rows.columns, ["name", "age", "score"]);
    assert_eq!(
        rows.rendered_rows(),
        [
            ["alice".to_string(), "31".to_string(), "28".to_string()],
            ["bob".to_string(), "24".to_string(), "25".to_string()],
            ["charlie".to_string(), "43".to_string(), "43".to_string()],
        ]
    );

    assert!(
        query_sql(&fixture, "SELECT rank FROM SqlTestUser").is_err(),
        "the predecessor source field name must not remain an alias"
    );
}

fn read_wasm(variable: &str) -> Vec<u8> {
    let path = env::var_os(variable).map_or_else(
        || panic!("{variable} must name one raw test_sql Wasm"),
        PathBuf::from,
    );
    fs::read(&path).unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
}

fn load_fixtures(fixture: &StandaloneCanisterFixture) {
    let loaded: Result<(), Error> = fixture
        .update_call("icydb_fixtures_load", ())
        .expect("fixture load response should decode");
    loaded.expect("fixture load should succeed");
}

fn upgrade(fixture: &StandaloneCanisterFixture, wasm: Vec<u8>) {
    let args = candid::encode_args(()).expect("empty upgrade arguments should encode");
    fixture
        .pic()
        .upgrade_canister(fixture.canister_id(), wasm, args, None)
        .expect("fixture upgrade should succeed");
}

fn migration_status(fixture: &StandaloneCanisterFixture) -> SchemaMigrationStatusPage {
    let result: Result<SchemaMigrationStatusPage, Error> = fixture
        .query_call(
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
    let result: Result<SchemaMigrationStatusPage, Error> = fixture
        .update_call("icydb_schema_migrate", (command,))
        .expect("migration command response should decode");
    result.expect("migration command should succeed")
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
    let result: Result<SqlQueryPerfResult, Error> = fixture
        .query_call("icydb_query", (sql.to_string(),))
        .expect("SQL query response should decode");
    result.map(|response| response.result)
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
