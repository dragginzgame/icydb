//!
//! Test-only SQL canister used by local and integration test harnesses.
//!

#[cfg(feature = "sql")]
mod perf;
mod seed;

extern crate canic_cdk as ic_cdk;

#[cfg(feature = "sql")]
use crate::perf::{
    SqlPerfAttributionRequest, SqlPerfAttributionSample, SqlPerfRequest, SqlPerfSample,
};
#[cfg(feature = "sql")]
use canic_cdk::query;
use canic_cdk::update;
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
use icydb_testing_quickstart_fixtures::schema::{ActiveUser, Character, Order, User};

icydb::start!();

/// Return one list of fixture entity names accepted by the SQL endpoints.
#[cfg(feature = "sql")]
#[query]
fn sql_entities() -> Vec<String> {
    sql_dispatch::entities()
}

/// Execute one reduced SQL statement against fixture entities.
#[cfg(feature = "sql")]
#[query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    sql_dispatch::query(sql.as_str())
}

/// Measure one repeated SQL surface invocation inside wasm and return local
/// instruction totals plus one compact outcome summary.
#[cfg(feature = "sql")]
#[query]
fn sql_perf(request: SqlPerfRequest) -> Result<SqlPerfSample, icydb::Error> {
    perf::sample_sql_surface(request)
}

/// Attribute one representative SQL surface into fixed-cost wasm phases.
#[cfg(feature = "sql")]
#[query]
fn sql_perf_attribution(
    request: SqlPerfAttributionRequest,
) -> Result<SqlPerfAttributionSample, icydb::Error> {
    perf::attribute_sql_surface(request)
}

/// Clear all fixture rows from this canister.
#[update]
fn fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<Order>().execute()?;
    db().delete::<Character>().execute()?;
    db().delete::<ActiveUser>().execute()?;
    db().delete::<User>().execute()?;

    Ok(())
}

/// Load one deterministic baseline fixture dataset.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;

    db().insert_many_atomic(seed::base::users())?;
    db().insert_many_atomic(seed::base::orders())?;
    db().insert_many_atomic(seed::base::active_users())?;
    db().insert_many_atomic(seed::rpg::characters())?;

    Ok(())
}

///
/// TESTS
///

#[cfg(all(test, feature = "sql"))]
mod tests {
    use super::{
        ActiveUser, Character, SqlQueryResult, User, db, fixtures_load_default, sql_dispatch,
    };
    use candid::encode_one;
    use icydb::{db::PersistedRow, traits::EntityValue};
    use icydb_testing_quickstart_fixtures::schema::QuickstartCanister;

    const QUICKSTART_MEMORY_MIN: u8 = 104;
    const QUICKSTART_MEMORY_MAX: u8 = 154;

    // `MemoryRuntimeApi::bootstrap_registry()` drains one process-global
    // eager-init queue. In host-parallel unit tests, later test threads can
    // therefore observe the quickstart canister range as missing on the current
    // thread even though the queue was already consumed elsewhere. Re-queue the
    // quickstart application range before each bootstrap-dependent test path so
    // the generated `db()` bootstrap stays deterministic per test thread.
    fn ensure_sql_test_memory_range() {
        ::icydb::__reexports::canic_memory::ic_memory_range!(
            QUICKSTART_MEMORY_MIN,
            QUICKSTART_MEMORY_MAX
        );
    }

    fn dispatch_result_for_sql(sql: &str) -> SqlQueryResult {
        ensure_sql_test_memory_range();
        sql_dispatch::query(sql).expect("sql_dispatch query should succeed")
    }

    fn dispatch_result_for_sql_unchecked(sql: &str) -> Result<SqlQueryResult, icydb::Error> {
        ensure_sql_test_memory_range();
        sql_dispatch::query(sql)
    }

    fn test_db() -> icydb::db::DbSession<QuickstartCanister> {
        ensure_sql_test_memory_range();
        db()
    }

    fn reload_default_fixtures() {
        ensure_sql_test_memory_range();
        fixtures_load_default().expect("fixture reload should succeed");
    }

    fn typed_result_for_sql_as<E>(sql: &str) -> SqlQueryResult
    where
        E: PersistedRow<Canister = QuickstartCanister> + EntityValue,
    {
        test_db()
            .execute_sql_dispatch::<E>(sql)
            .expect("typed execute_sql_dispatch should succeed")
    }

    fn typed_result_for_sql(sql: &str) -> SqlQueryResult {
        typed_result_for_sql_as::<User>(sql)
    }

    fn typed_result_for_sql_unchecked_as<E>(sql: &str) -> Result<SqlQueryResult, icydb::Error>
    where
        E: PersistedRow<Canister = QuickstartCanister> + EntityValue,
    {
        test_db().execute_sql_dispatch::<E>(sql)
    }

    fn typed_result_for_sql_unchecked(sql: &str) -> Result<SqlQueryResult, icydb::Error> {
        typed_result_for_sql_unchecked_as::<User>(sql)
    }

    // Compare one sql_dispatch lane payload against the typed `execute_sql_dispatch` path.
    fn assert_dispatch_matches_typed(sql: &str, context: &str) {
        let dispatch = dispatch_result_for_sql(sql);
        let typed = typed_result_for_sql(sql);

        assert_eq!(dispatch, typed, "{context}");
    }

    // Compare one sql_dispatch lane payload against one typed dispatch entity
    // surface without re-hardcoding the entity type at each callsite.
    fn assert_dispatch_matches_typed_as<E>(sql: &str, context: &str)
    where
        E: PersistedRow<Canister = QuickstartCanister> + EntityValue,
    {
        let dispatch = dispatch_result_for_sql(sql);
        let typed = typed_result_for_sql_as::<E>(sql);

        assert_eq!(dispatch, typed, "{context}");
    }

    // Compare one fallible projection SQL path across dispatch and typed execution.
    fn assert_dispatch_result_matches_typed(sql: &str, context: &str) {
        assert_dispatch_result_matches_typed_as::<User>(sql, context);
    }

    // Compare one fallible projection SQL path across dispatch and one typed
    // entity-specific execution surface.
    fn assert_dispatch_result_matches_typed_as<E>(sql: &str, context: &str)
    where
        E: PersistedRow<Canister = QuickstartCanister> + EntityValue,
    {
        let dispatch = dispatch_result_for_sql_unchecked(sql);
        let typed = typed_result_for_sql_unchecked_as::<E>(sql);

        match (dispatch, typed) {
            (Ok(dispatch), Ok(typed)) => {
                assert_eq!(dispatch, typed, "{context}");
            }
            (Err(dispatch_err), Err(typed_err)) => {
                assert_eq!(
                    dispatch_err.kind(),
                    typed_err.kind(),
                    "{context}: error kind mismatch",
                );
                assert_eq!(
                    dispatch_err.origin(),
                    typed_err.origin(),
                    "{context}: error origin mismatch",
                );
            }
            (dispatch, typed) => {
                panic!("{context}: dispatch={dispatch:?} typed={typed:?}");
            }
        }
    }

    // Normalize one row-shaped SQL payload for comparisons across fixture
    // reloads that regenerate primary keys and audit timestamps.
    fn normalized_mutating_dispatch_payload(payload: SqlQueryResult) -> SqlQueryResult {
        match payload {
            SqlQueryResult::Projection(mut rows) => {
                for row in &mut rows.rows {
                    for (index, column) in rows.columns.iter().enumerate() {
                        if matches!(column.as_str(), "id" | "created_at" | "updated_at") {
                            row[index] = "<dynamic>".to_string();
                        }
                    }
                }

                SqlQueryResult::Projection(rows)
            }
            other => other,
        }
    }

    // Compare one mutating SQL path across generated and typed dispatch by
    // reloading the deterministic fixture dataset before each execution.
    fn assert_delete_dispatch_result_matches_typed(sql: &str, context: &str) {
        ensure_sql_test_memory_range();
        fixtures_load_default().expect("fixture reload before generated DELETE should succeed");
        let dispatch = sql_dispatch::query(sql);

        ensure_sql_test_memory_range();
        fixtures_load_default().expect("fixture reload before typed DELETE should succeed");
        let typed = test_db().execute_sql_dispatch::<User>(sql);

        match (dispatch, typed) {
            (Ok(dispatch), Ok(typed)) => {
                assert_eq!(
                    normalized_mutating_dispatch_payload(dispatch),
                    normalized_mutating_dispatch_payload(typed),
                    "{context}",
                );
            }
            (Err(dispatch_err), Err(typed_err)) => {
                assert_eq!(
                    dispatch_err.kind(),
                    typed_err.kind(),
                    "{context}: error kind mismatch",
                );
                assert_eq!(
                    dispatch_err.origin(),
                    typed_err.origin(),
                    "{context}: error origin mismatch",
                );
            }
            (dispatch, typed) => {
                panic!("{context}: dispatch={dispatch:?} typed={typed:?}");
            }
        }
    }

    fn dispatch_explain_for_sql(sql: &str) -> String {
        let payload = dispatch_result_for_sql(sql);
        match payload {
            SqlQueryResult::Explain { explain, .. } => explain,
            other => panic!(
                "sql_dispatch query should return explain payload for EXPLAIN SQL: {other:?}"
            ),
        }
    }

    fn explain_access_line(explain: &str) -> &str {
        explain
            .lines()
            .find(|line| line.starts_with("access="))
            .expect("explain payload should include an access line")
    }

    #[test]
    fn generated_sql_dispatch_surface_is_stable() {
        let actor =
            icydb_testing_wasm_fixtures::assert_generated_sql_dispatch_surface_from_out_dir!();

        assert!(
            !actor.contains("from_statement_sql"),
            "generated sql_dispatch must not include removed from_statement_sql resolver"
        );
        assert!(
            !actor.contains("pub fn query_rows ("),
            "generated sql_dispatch must not include removed query_rows convenience entrypoint"
        );
    }

    #[test]
    fn generated_sql_dispatch_explain_text_matches_typed_explain_surface() {
        let sql = "EXPLAIN SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 5";
        let typed_explain_payload = test_db()
            .execute_sql_dispatch::<User>(sql)
            .expect("typed execute_sql_dispatch should succeed");
        let typed_explain = match typed_explain_payload {
            SqlQueryResult::Explain { explain, .. } => explain,
            other => panic!(
                "typed execute_sql_dispatch should return explain payload for EXPLAIN SQL: {other:?}"
            ),
        };
        let dispatch_explain = dispatch_explain_for_sql(sql);

        assert_eq!(
            dispatch_explain, typed_explain,
            "typed execute_sql_dispatch and sql_dispatch explain should render identical canonical text",
        );
    }

    #[test]
    fn generated_sql_dispatch_access_line_matches_typed_query_access_plan() {
        let query_sql = "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 5";
        let explain_sql = format!("EXPLAIN {query_sql}");

        let typed_query = test_db()
            .query_from_sql::<User>(query_sql)
            .expect("typed query_from_sql should lower");
        let typed_access = format!(
            "access={:?}",
            typed_query
                .explain()
                .expect("typed query explain projection should succeed")
                .access(),
        );

        let dispatch_explain = dispatch_explain_for_sql(explain_sql.as_str());
        let dispatch_access = explain_access_line(dispatch_explain.as_str());

        assert_eq!(
            dispatch_access, typed_access,
            "typed query access plan and sql_dispatch explain access line should stay equivalent",
        );
    }

    #[test]
    fn typed_execute_sql_dispatch_supports_show_entities_lane() {
        let payload = test_db()
            .execute_sql_dispatch::<User>("SHOW ENTITIES")
            .expect("typed execute_sql_dispatch should support SHOW ENTITIES");

        match payload {
            SqlQueryResult::ShowEntities { entities } => {
                assert!(
                    entities.contains(&"User".to_string()),
                    "SHOW ENTITIES should include User fixture entity",
                );
                assert!(
                    entities.contains(&"ActiveUser".to_string()),
                    "SHOW ENTITIES should include ActiveUser fixture entity",
                );
                assert!(
                    entities.contains(&"Order".to_string()),
                    "SHOW ENTITIES should include Order fixture entity",
                );
                assert!(
                    entities.contains(&"Character".to_string()),
                    "SHOW ENTITIES should include Character fixture entity",
                );
            }
            other => panic!(
                "SHOW ENTITIES should return ShowEntities payload from execute_sql_dispatch: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_projection_matches_typed_projection_surface() {
        let sql = "SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 5";
        assert_dispatch_result_matches_typed(
            sql,
            "typed execute_sql_dispatch and sql_dispatch should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_computed_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT LOWER(name) FROM User ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep computed projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_user_expression_order_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep User expression-order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_user_expression_order_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep User expression-order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_user_expression_order_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending User expression-order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_user_expression_order_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending User expression-order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_user_expression_order_projection_matches_expected_rows() {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "User");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "alice");
                assert_eq!(rows.rows[1][1], "bob");
            }
            other => {
                panic!("expression-order projection should return a projection payload: {other:?}")
            }
        }
    }

    #[test]
    fn generated_sql_dispatch_user_expression_order_desc_projection_matches_expected_rows() {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "User");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "charlie");
                assert_eq!(rows.rows[1][1], "bob");
            }
            other => panic!(
                "descending expression-order projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_user_expression_order_explain_reports_materialized_route() {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM User ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("IndexRangeScan")
                && explain.contains("User|LOWER(name)")
                && explain.contains("OrderByAccessSatisfied"),
            "expression-order explain should preserve the shared index-range access contract: {explain}",
        );
        assert!(
            explain.contains("cov_read_route=Text(\"materialized\")")
                && explain.contains("cov_scan_reason=Text(\"order_mat\")"),
            "expression-order explain should report the non-covering materialized projection route: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_user_expression_order_desc_explain_reports_materialized_route() {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM User ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("IndexRangeScan")
                && explain.contains("User|LOWER(name)")
                && explain.contains("OrderByAccessSatisfied"),
            "descending expression-order explain should preserve the shared index-range access contract: {explain}",
        );
        assert!(
            explain.contains("cov_read_route=Text(\"materialized\")")
                && explain.contains("cov_scan_reason=Text(\"order_mat\")")
                && explain.contains("scan_dir=Text(\"desc\")"),
            "descending expression-order explain should report the non-covering materialized projection route: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_covering_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, name FROM Character WHERE name = 'Alex Ander' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep Character covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_covering_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name = 'Alex Ander' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep Character covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_strict_like_prefix_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Character strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_strict_like_prefix_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Character strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_strict_like_prefix_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Character strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_strict_like_prefix_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Character strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_direct_starts_with_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Character direct STARTS_WITH covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_direct_starts_with_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Character direct STARTS_WITH covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_direct_starts_with_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Character direct STARTS_WITH covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_direct_starts_with_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Character direct STARTS_WITH covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_strict_text_range_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Character strict text-range covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_strict_text_range_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Character strict text-range covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_strict_text_range_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Character strict text-range covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_strict_text_range_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Character strict text-range covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_equivalent_strict_prefix_forms_match_projection_rows() {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated Character STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated Character text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_equivalent_desc_strict_prefix_forms_match_projection_rows()
    {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM Character WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM Character WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM Character WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending Character STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending Character text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_order_only_composite_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Character order-only composite covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_order_only_composite_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, level, class_name FROM Character ORDER BY level ASC, class_name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Character order-only composite covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_order_only_composite_desc_projection_matches_typed_surface()
    {
        assert_dispatch_result_matches_typed_as::<Character>(
            "SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Character order-only composite covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_order_only_composite_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<Character>(
            "EXPLAIN EXECUTION SELECT id, level, class_name FROM Character ORDER BY level DESC, class_name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Character order-only composite covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_order_only_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered order-only covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_order_only_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered order-only covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_order_only_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_strict_like_prefix_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_strict_like_prefix_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_strict_like_prefix_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_strict_like_prefix_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_equivalent_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 1",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 1",
        );

        assert_eq!(
            starts_with, like,
            "generated ActiveUser STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated ActiveUser text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_equivalent_desc_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 1",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 1",
        );

        assert_eq!(
            starts_with, like,
            "generated descending ActiveUser STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending ActiveUser text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_strict_like_prefix_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_strict_like_prefix_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_strict_like_prefix_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_strict_like_prefix_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_order_only_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite order-only covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_order_only_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite order-only covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_order_only_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_order_only_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered expression-order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_order_only_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered expression-order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered expression-order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_order_only_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered expression-order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_strict_like_prefix_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered expression strict LIKE prefix projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_strict_like_prefix_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered expression strict LIKE prefix EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_strict_like_prefix_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered expression strict LIKE prefix projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_strict_like_prefix_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered expression strict LIKE prefix EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_equivalent_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated ActiveUser filtered expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_equivalent_desc_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending ActiveUser filtered expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_order_only_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite expression order-only projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_order_only_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite expression order-only EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite expression order-only projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_order_only_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite expression order-only EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_strict_like_prefix_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite expression strict LIKE prefix projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_strict_like_prefix_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite expression strict LIKE prefix EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_strict_like_prefix_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite expression strict LIKE prefix projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_strict_like_prefix_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<ActiveUser>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite expression strict LIKE prefix EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_equivalent_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated ActiveUser filtered composite expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_equivalent_desc_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending ActiveUser filtered composite expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_equivalent_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated ActiveUser filtered composite STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated ActiveUser filtered composite text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_equivalent_desc_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending ActiveUser filtered composite STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending ActiveUser filtered composite text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_strict_like_prefix_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 1);
                assert_eq!(rows.rows.len(), 1);
                assert_eq!(rows.rows[0][1], "bravo");
            }
            other => panic!(
                "filtered strict LIKE prefix ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_strict_like_prefix_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 1);
                assert_eq!(rows.rows.len(), 1);
                assert_eq!(rows.rows[0][1], "bravo");
            }
            other => panic!(
                "descending filtered strict LIKE prefix ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_strict_like_prefix_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(
                    rows.columns,
                    vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
                );
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "gold");
                assert_eq!(rows.rows[0][2], "bravo");
                assert_eq!(rows.rows[1][1], "gold");
                assert_eq!(rows.rows[1][2], "bristle");
            }
            other => panic!(
                "filtered composite strict LIKE prefix ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_strict_like_prefix_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(
                    rows.columns,
                    vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
                );
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "gold");
                assert_eq!(rows.rows[0][2], "bristle");
                assert_eq!(rows.rows[1][1], "gold");
                assert_eq!(rows.rows[1][2], "bravo");
            }
            other => panic!(
                "descending filtered composite strict LIKE prefix ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_order_only_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(
                    rows.columns,
                    vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
                );
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "gold");
                assert_eq!(rows.rows[0][2], "bravo");
                assert_eq!(rows.rows[1][1], "gold");
                assert_eq!(rows.rows[1][2], "bristle");
            }
            other => panic!(
                "filtered composite order-only ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_order_only_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(
                    rows.columns,
                    vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
                );
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "gold");
                assert_eq!(rows.rows[0][2], "bristle");
                assert_eq!(rows.rows[1][1], "gold");
                assert_eq!(rows.rows[1][2], "bravo");
            }
            other => panic!(
                "descending filtered composite order-only ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_order_only_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(rows.columns, vec!["id".to_string(), "handle".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bravo");
                assert_eq!(rows.rows[1][1], "Brisk");
            }
            other => panic!(
                "filtered expression order-only ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_order_only_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(rows.columns, vec!["id".to_string(), "handle".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bristle");
                assert_eq!(rows.rows[1][1], "Brisk");
            }
            other => panic!(
                "descending filtered expression order-only ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_strict_like_prefix_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(rows.columns, vec!["id".to_string(), "handle".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bravo");
                assert_eq!(rows.rows[1][1], "Brisk");
            }
            other => panic!(
                "filtered expression strict LIKE prefix ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_strict_like_prefix_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(rows.columns, vec!["id".to_string(), "handle".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bristle");
                assert_eq!(rows.rows[1][1], "Brisk");
            }
            other => panic!(
                "descending filtered expression strict LIKE prefix ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_strict_text_range_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered expression text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_strict_text_range_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered expression text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_equivalent_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated ActiveUser filtered expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated ActiveUser filtered expression text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_expression_equivalent_desc_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, handle FROM ActiveUser WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending ActiveUser filtered expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending ActiveUser filtered expression text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_order_only_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(
                    rows.columns,
                    vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
                );
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "gold");
                assert_eq!(rows.rows[0][2], "bravo");
                assert_eq!(rows.rows[1][1], "gold");
                assert_eq!(rows.rows[1][2], "bristle");
            }
            other => panic!(
                "filtered composite expression order-only ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_order_only_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(
                    rows.columns,
                    vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
                );
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "gold");
                assert_eq!(rows.rows[0][2], "bristle");
                assert_eq!(rows.rows[1][1], "gold");
                assert_eq!(rows.rows[1][2], "bravo");
            }
            other => panic!(
                "descending filtered composite expression order-only ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_strict_like_prefix_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(
                    rows.columns,
                    vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
                );
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "gold");
                assert_eq!(rows.rows[0][2], "bravo");
                assert_eq!(rows.rows[1][1], "gold");
                assert_eq!(rows.rows[1][2], "bristle");
            }
            other => panic!(
                "filtered composite expression strict LIKE prefix ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_strict_like_prefix_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(
                    rows.columns,
                    vec!["id".to_string(), "tier".to_string(), "handle".to_string()]
                );
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "gold");
                assert_eq!(rows.rows[0][2], "bristle");
                assert_eq!(rows.rows[1][1], "gold");
                assert_eq!(rows.rows[1][2], "bravo");
            }
            other => panic!(
                "descending filtered composite expression strict LIKE prefix ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_strict_text_range_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep ActiveUser filtered composite expression text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_strict_text_range_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<ActiveUser>(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending ActiveUser filtered composite expression text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_equivalent_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated ActiveUser filtered composite expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated ActiveUser filtered composite expression text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_composite_expression_equivalent_desc_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM ActiveUser WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending ActiveUser filtered composite expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending ActiveUser filtered composite expression text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_order_only_projection_matches_expected_rows() {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bravo");
                assert_eq!(rows.rows[1][1], "charlie");
            }
            other => panic!(
                "filtered order-only ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_active_user_filtered_order_only_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM ActiveUser WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "ActiveUser");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "echo");
                assert_eq!(rows.rows[1][1], "charlie");
            }
            other => panic!(
                "descending filtered order-only ActiveUser projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_global_aggregate_execution_stays_fail_closed() {
        let sql = "SELECT COUNT(*) FROM User";
        let dispatch_err = dispatch_result_for_sql_unchecked(sql)
            .expect_err("sql_dispatch should reject global aggregate execution");
        let typed_err = typed_result_for_sql_unchecked(sql)
            .expect_err("typed execute_sql_dispatch should reject global aggregate execution");

        assert_eq!(
            dispatch_err.kind(),
            typed_err.kind(),
            "typed execute_sql_dispatch and sql_dispatch should keep global aggregate error kind parity",
        );
        assert_eq!(
            dispatch_err.origin(),
            typed_err.origin(),
            "typed execute_sql_dispatch and sql_dispatch should keep global aggregate error origin parity",
        );
        assert!(
            dispatch_err.to_string().contains("global aggregate SELECT")
                && dispatch_err
                    .to_string()
                    .contains("execute_sql_aggregate(...)"),
            "sql_dispatch should preserve explicit aggregate-lane guidance",
        );
        assert!(
            typed_err.to_string().contains("global aggregate SELECT")
                && typed_err.to_string().contains("execute_sql_aggregate(...)"),
            "typed execute_sql_dispatch should preserve explicit aggregate-lane guidance",
        );
    }

    #[test]
    fn generated_sql_dispatch_grouped_execution_stays_fail_closed() {
        let sql = "SELECT age, COUNT(*) FROM User GROUP BY age";
        let dispatch_err = dispatch_result_for_sql_unchecked(sql)
            .expect_err("sql_dispatch should reject grouped SQL execution");
        let typed_err = typed_result_for_sql_unchecked(sql)
            .expect_err("typed execute_sql_dispatch should reject grouped SQL execution");

        assert_eq!(
            dispatch_err.kind(),
            typed_err.kind(),
            "typed execute_sql_dispatch and sql_dispatch should keep grouped SQL error kind parity",
        );
        assert_eq!(
            dispatch_err.origin(),
            typed_err.origin(),
            "typed execute_sql_dispatch and sql_dispatch should keep grouped SQL error origin parity",
        );
        assert!(
            dispatch_err
                .to_string()
                .contains("generated SQL query surface rejects grouped SELECT execution")
                && dispatch_err
                    .to_string()
                    .contains("execute_sql_grouped(...)"),
            "sql_dispatch should preserve explicit grouped-entrypoint guidance",
        );
        assert!(
            typed_err
                .to_string()
                .contains("execute_sql_dispatch rejects grouped SELECT execution")
                && typed_err.to_string().contains("execute_sql_grouped(...)"),
            "typed execute_sql_dispatch should preserve explicit grouped-entrypoint guidance",
        );
    }

    #[test]
    fn generated_sql_dispatch_grouped_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT age, COUNT(*) FROM User GROUP BY age",
            "typed execute_sql_dispatch and sql_dispatch should keep grouped EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_starts_with_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM User WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct STARTS_WITH parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_starts_with_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM User WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct STARTS_WITH EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_starts_with_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) STARTS_WITH parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_starts_with_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM User WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) STARTS_WITH EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_starts_with_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) STARTS_WITH parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_starts_with_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM User WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) STARTS_WITH EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_non_casefold_wrapped_direct_starts_with_stays_fail_closed() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM User WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep non-casefold wrapped direct STARTS_WITH fail-closed parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_non_casefold_wrapped_direct_starts_with_explain_stays_fail_closed() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM User WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep non-casefold wrapped direct STARTS_WITH EXPLAIN fail-closed parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_computed_projection_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT LOWER(name) FROM User ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep computed projection EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_delete_matches_typed_delete_surface() {
        assert_delete_dispatch_result_matches_typed(
            "DELETE FROM User ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_explain_delete_matches_typed_explain_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN DELETE FROM User ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep EXPLAIN DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_describe_matches_typed_describe_surface() {
        assert_dispatch_matches_typed(
            "DESCRIBE public.User",
            "typed execute_sql_dispatch and sql_dispatch should return identical DESCRIBE payloads",
        );
    }

    #[test]
    fn generated_sql_dispatch_show_indexes_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "SHOW INDEXES public.User",
            "typed execute_sql_dispatch and sql_dispatch should return identical SHOW INDEXES payloads",
        );
    }

    #[test]
    fn generated_sql_dispatch_show_columns_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "SHOW COLUMNS public.User",
            "typed execute_sql_dispatch and sql_dispatch should return identical SHOW COLUMNS payloads",
        );
    }

    #[test]
    fn generated_sql_dispatch_show_entities_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "SHOW ENTITIES",
            "typed execute_sql_dispatch and sql_dispatch should return identical SHOW ENTITIES payloads",
        );
    }

    #[test]
    fn generated_sql_dispatch_character_metadata_surfaces_encode_cleanly() {
        ensure_sql_test_memory_range();

        for sql in [
            "DESCRIBE Character",
            "DESCRIBE public.Character",
            "SHOW INDEXES Character",
            "SHOW INDEXES public.Character",
            "SHOW COLUMNS Character",
            "SHOW COLUMNS public.Character",
        ] {
            let payload = sql_dispatch::query(sql).unwrap_or_else(|err| {
                panic!("sql_dispatch query should succeed for {sql}: {err:?}")
            });
            let encoded = encode_one(&payload).unwrap_or_else(|err| {
                panic!("Candid encoding should succeed for {sql} payload {payload:?}: {err}")
            });
            let decoded: SqlQueryResult = candid::decode_one(&encoded).unwrap_or_else(|err| {
                panic!("Candid decoding should succeed for {sql} payload {payload:?}: {err}")
            });

            assert_eq!(
                decoded, payload,
                "Character metadata payload should survive canister-style Candid roundtrip for {sql}",
            );
        }
    }
}

canic_cdk::export_candid!();
