//!
//! Small SQL canister used for lightweight generated-vs-typed smoke tests.
//!

extern crate canic_cdk as ic_cdk;

#[cfg(feature = "sql")]
use canic_cdk::query;
use canic_cdk::update;
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
use icydb_testing_test_sql_fixtures::sql::SqlTestUser;

icydb::start!();

/// Execute one reduced SQL statement against the lightweight SQL fixture.
#[cfg(feature = "sql")]
#[query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    sql_dispatch::query(sql.as_str())
}

/// Clear all lightweight SQL smoke-test fixture rows from this canister.
#[update]
fn fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<SqlTestUser>().execute()?;

    Ok(())
}

/// Load one deterministic baseline fixture dataset for SQL smoke tests.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;
    db().insert_many_atomic(sql_users())?;

    Ok(())
}

/// Build one deterministic baseline SQL user fixture batch.
fn sql_users() -> Vec<SqlTestUser> {
    vec![
        SqlTestUser {
            name: "alice".to_string(),
            age: 31,
            ..Default::default()
        },
        SqlTestUser {
            name: "bob".to_string(),
            age: 24,
            ..Default::default()
        },
        SqlTestUser {
            name: "charlie".to_string(),
            age: 43,
            ..Default::default()
        },
    ]
}

///
/// TESTS
///

#[cfg(all(test, feature = "sql"))]
mod tests {
    use super::{SqlQueryResult, SqlTestUser, db, fixtures_load_default, sql_dispatch};
    use icydb_testing_test_sql_fixtures::sql::SqlTestCanister;

    const SQL_TEST_MEMORY_MIN: u8 = 155;
    const SQL_TEST_MEMORY_MAX: u8 = 165;

    // Re-queue the sql-test application memory range before each host-side
    // bootstrap so generated `db()` initialization stays deterministic per
    // test thread after one earlier test drained the eager-init queue.
    fn ensure_sql_memory_range() {
        ::icydb::__reexports::canic_memory::ic_memory_range!(
            SQL_TEST_MEMORY_MIN,
            SQL_TEST_MEMORY_MAX
        );
    }

    // Reload the deterministic fixture rows before each smoke-test query so
    // generated and typed dispatch compare against one stable dataset.
    fn reload_default_fixtures() {
        ensure_sql_memory_range();
        fixtures_load_default().expect("fixture reload should succeed");
    }

    // Execute one generated reduced-SQL dispatch query against the smoke
    // fixture canister surface.
    fn dispatch_result_for_sql(sql: &str) -> SqlQueryResult {
        ensure_sql_memory_range();
        sql_dispatch::query(sql).expect("sql_dispatch query should succeed")
    }

    // Build one typed session for the sql-test canister fixture surface.
    fn test_db() -> icydb::db::DbSession<SqlTestCanister> {
        ensure_sql_memory_range();
        db()
    }

    // Execute one typed SQL dispatch query against the sql-test fixture
    // canister surface.
    fn typed_result_for_sql(sql: &str) -> SqlQueryResult {
        test_db()
            .execute_sql_dispatch::<SqlTestUser>(sql)
            .expect("typed execute_sql_dispatch should succeed")
    }

    // Compare one generated sql_dispatch payload against the typed SQL
    // dispatch path after reloading one deterministic fixture dataset.
    fn assert_dispatch_matches_typed(sql: &str, context: &str) {
        reload_default_fixtures();
        let dispatch = dispatch_result_for_sql(sql);
        let typed = typed_result_for_sql(sql);

        assert_eq!(dispatch, typed, "{context}");
    }

    #[test]
    fn generated_sql_dispatch_surface_is_stable() {
        icydb_testing_wasm_helpers::assert_generated_sql_dispatch_surface_from_out_dir!();
    }

    #[test]
    fn generated_sql_dispatch_show_entities_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "SHOW ENTITIES",
            "generated SHOW ENTITIES should match the typed sql-test surface",
        );
    }

    #[test]
    fn generated_sql_dispatch_projection_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "SELECT id, name FROM SqlTestUser ORDER BY name ASC LIMIT 2",
            "generated projection rows should match the typed sql-test surface",
        );
    }

    #[test]
    fn generated_sql_dispatch_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM SqlTestUser WHERE name = 'alice' ORDER BY id ASC LIMIT 1",
            "generated EXPLAIN EXECUTION should match the typed sql-test surface",
        );
    }
}

canic_cdk::export_candid!();
