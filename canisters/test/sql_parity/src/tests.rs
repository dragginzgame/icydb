mod tests {
    use super::{
        Customer, CustomerAccount, CustomerOrder, SqlQueryResult, db, fixtures_load_default,
        fixtures_mark_customer_index_building,
        fixtures_make_customer_name_order_stale,
        fixtures_make_customer_order_numeric_equality_desc_stale,
        fixtures_make_customer_order_numeric_equality_stale,
        fixtures_make_customer_order_order_only_composite_desc_stale,
        fixtures_make_customer_order_order_only_composite_stale,
        perf::{SqlPerfRequest, SqlPerfSurface, sample_sql_surface},
        sql_dispatch,
    };
    use candid::encode_one;
    use icydb::{
        db::PersistedRow,
        error::{ErrorKind, ErrorOrigin, RuntimeErrorKind},
        traits::EntityValue,
        types::Decimal,
        value::Value,
    };
    use icydb_testing_test_sql_parity_fixtures::{fixtures, schema::SqlParityCanister};
    use std::collections::BTreeSet;

    const DEMO_RPG_MEMORY_MIN: u8 = 104;
    const DEMO_RPG_MEMORY_MAX: u8 = 154;

    // The generated `db()` bootstrap now flushes pending eager-init state
    // without introducing a new owner range at call time. In host-parallel
    // unit tests, later test threads can therefore observe the sql-parity
    // range as missing on the current thread once an earlier thread already
    // drained that process-global eager-init queue. Re-queue the sql-parity
    // application range before each bootstrap-dependent test path so the
    // generated `db()` bootstrap stays deterministic per test thread.
    fn ensure_sql_test_memory_range() {
        ::icydb::__reexports::canic_memory::ic_memory_range!(
            DEMO_RPG_MEMORY_MIN,
            DEMO_RPG_MEMORY_MAX
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

    fn test_db() -> icydb::db::DbSession<SqlParityCanister> {
        ensure_sql_test_memory_range();
        db()
    }

    fn reload_default_fixtures() {
        ensure_sql_test_memory_range();
        fixtures_load_default().expect("fixture reload should succeed");
    }

    fn reload_default_fixtures_with_customer_name_stale() {
        reload_default_fixtures();
        ensure_sql_test_memory_range();
        fixtures_make_customer_name_order_stale()
            .expect("stale Customer name-order fixture mutation should succeed");
    }

    fn reload_default_fixtures_with_customer_index_building() {
        reload_default_fixtures();
        ensure_sql_test_memory_range();
        fixtures_mark_customer_index_building()
            .expect("Customer index-building fixture mutation should succeed");
    }

    fn reload_default_fixtures_with_customer_order_order_only_composite_stale() {
        reload_default_fixtures();
        ensure_sql_test_memory_range();
        fixtures_make_customer_order_order_only_composite_stale()
            .expect("stale CustomerOrder order-only composite fixture mutation should succeed");
    }

    fn reload_default_fixtures_with_customer_order_numeric_equality_stale() {
        reload_default_fixtures();
        ensure_sql_test_memory_range();
        fixtures_make_customer_order_numeric_equality_stale()
            .expect("stale CustomerOrder numeric-equality fixture mutation should succeed");
    }

    fn reload_default_fixtures_with_customer_order_numeric_equality_desc_stale() {
        reload_default_fixtures();
        ensure_sql_test_memory_range();
        fixtures_make_customer_order_numeric_equality_desc_stale()
            .expect("stale descending CustomerOrder numeric-equality fixture mutation should succeed");
    }

    fn reload_default_fixtures_with_customer_order_order_only_composite_desc_stale() {
        reload_default_fixtures();
        ensure_sql_test_memory_range();
        fixtures_make_customer_order_order_only_composite_desc_stale().expect(
            "stale descending CustomerOrder order-only composite fixture mutation should succeed",
        );
    }

    fn typed_result_for_sql_as<E>(sql: &str) -> SqlQueryResult
    where
        E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
    {
        test_db()
            .execute_sql_dispatch::<E>(sql)
            .expect("typed execute_sql_dispatch should succeed")
    }

    fn typed_result_for_sql(sql: &str) -> SqlQueryResult {
        typed_result_for_sql_as::<Customer>(sql)
    }

    fn typed_result_for_sql_unchecked_as<E>(sql: &str) -> Result<SqlQueryResult, icydb::Error>
    where
        E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
    {
        test_db().execute_sql_dispatch::<E>(sql)
    }

    fn typed_result_for_sql_unchecked(sql: &str) -> Result<SqlQueryResult, icydb::Error> {
        typed_result_for_sql_unchecked_as::<Customer>(sql)
    }

    // Execute one constrained global aggregate SQL statement through the typed
    // aggregate lane so parity tests can lock the dedicated scalar surface
    // directly instead of inferring it through dispatch rejection.
    fn typed_aggregate_value_for_sql_as<E>(sql: &str) -> Value
    where
        E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
    {
        test_db()
            .execute_sql_aggregate::<E>(sql)
            .expect("typed execute_sql_aggregate should succeed")
    }

    fn typed_aggregate_value_for_sql(sql: &str) -> Value {
        typed_aggregate_value_for_sql_as::<Customer>(sql)
    }

    fn typed_aggregate_value_for_sql_unchecked_as<E>(sql: &str) -> Result<Value, icydb::Error>
    where
        E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
    {
        test_db().execute_sql_aggregate::<E>(sql)
    }

    fn typed_aggregate_value_for_sql_unchecked(sql: &str) -> Result<Value, icydb::Error> {
        typed_aggregate_value_for_sql_unchecked_as::<Customer>(sql)
    }

    fn perf_sample(surface: SqlPerfSurface, sql: &str) -> super::perf::SqlPerfSample {
        reload_default_fixtures();
        sample_sql_surface(SqlPerfRequest {
            surface,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("sql perf sample should succeed")
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
        E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
    {
        let dispatch = dispatch_result_for_sql(sql);
        let typed = typed_result_for_sql_as::<E>(sql);

        assert_eq!(dispatch, typed, "{context}");
    }

    // Compare one fallible projection SQL path across dispatch and typed execution.
    fn assert_dispatch_result_matches_typed(sql: &str, context: &str) {
        assert_dispatch_result_matches_typed_as::<Customer>(sql, context);
    }

    // Compare one fallible projection SQL path across dispatch and one typed
    // entity-specific execution surface.
    fn assert_dispatch_result_matches_typed_as<E>(sql: &str, context: &str)
    where
        E: PersistedRow<Canister = SqlParityCanister> + EntityValue,
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
        let typed = test_db().execute_sql_dispatch::<Customer>(sql);

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
            icydb_testing_wasm_helpers::assert_generated_sql_dispatch_surface_from_out_dir!();

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
        let sql = "EXPLAIN SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 5";
        let typed_explain_payload = test_db()
            .execute_sql_dispatch::<Customer>(sql)
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
        let query_sql = "SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 5";
        let explain_sql = format!("EXPLAIN {query_sql}");

        let typed_query = test_db()
            .query_from_sql::<Customer>(query_sql)
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
            .execute_sql_dispatch::<Customer>("SHOW ENTITIES")
            .expect("typed execute_sql_dispatch should support SHOW ENTITIES");

        match payload {
            SqlQueryResult::ShowEntities { entities } => {
                assert!(
                    entities.contains(&"Customer".to_string()),
                    "SHOW ENTITIES should include Customer fixture entity",
                );
                assert!(
                    entities.contains(&"CustomerAccount".to_string()),
                    "SHOW ENTITIES should include CustomerAccount fixture entity",
                );
                assert!(
                    entities.contains(&"CustomerOrder".to_string()),
                    "SHOW ENTITIES should include CustomerOrder fixture entity",
                );
            }
            other => panic!(
                "SHOW ENTITIES should return ShowEntities payload from execute_sql_dispatch: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_projection_matches_typed_projection_surface() {
        let sql = "SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 5";
        assert_dispatch_result_matches_typed(
            sql,
            "typed execute_sql_dispatch and sql_dispatch should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_computed_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT LOWER(name) FROM Customer ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep computed projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_order_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Customer expression-order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_order_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Customer expression-order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_order_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Customer expression-order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_order_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Customer expression-order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_order_projection_matches_expected_rows() {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "Customer");
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
    fn generated_sql_dispatch_customer_expression_order_desc_projection_matches_expected_rows() {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "Customer");
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
    fn generated_sql_dispatch_customer_expression_order_explain_reports_materialized_route() {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("IndexRangeScan")
                && explain.contains("Customer|LOWER(name)")
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
    fn generated_sql_dispatch_customer_expression_order_desc_explain_reports_materialized_route() {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("IndexRangeScan")
                && explain.contains("Customer|LOWER(name)")
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
    fn generated_sql_dispatch_customer_expression_key_only_order_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Customer expression key-only order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_order_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Customer expression key-only order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_order_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain =
            dispatch_explain_for_sql("EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY LOWER(name) ASC, id ASC LIMIT 2");

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\")])"),
            "Customer expression key-only order explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "Customer expression key-only order explain should report the witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_order_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed(
            "SELECT id FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Customer expression key-only order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_order_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Customer expression key-only order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_order_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain =
            dispatch_explain_for_sql("EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY LOWER(name) DESC, id DESC LIMIT 2");

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\")])"),
            "descending Customer expression key-only order explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "descending Customer expression key-only order explain should report the witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_strict_text_range_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed(
            "SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Customer expression key-only strict text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_strict_text_range_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep Customer expression key-only strict text-range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_strict_text_range_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\")])"),
            "Customer expression key-only strict text-range explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "Customer expression key-only strict text-range explain should report the witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_strict_text_range_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed(
            "SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Customer expression key-only strict text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_strict_text_range_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending Customer expression key-only strict text-range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_expression_key_only_strict_text_range_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY LOWER(name) DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\")])"),
            "descending Customer expression key-only strict text-range explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "descending Customer expression key-only strict text-range explain should report the witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_primary_key_covering_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id FROM Customer ORDER BY id ASC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep PK-only Customer covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_primary_key_covering_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY id ASC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep PK-only Customer covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_primary_key_covering_projection_matches_expected_shape() {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql("SELECT id FROM Customer ORDER BY id ASC LIMIT 1");

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "Customer");
                assert_eq!(rows.columns, vec!["id".to_string()]);
                assert_eq!(rows.row_count, 1);
                assert_eq!(rows.rows.len(), 1);
                assert_eq!(rows.rows[0].len(), 1);
            }
            other => {
                panic!("PK-only covering projection should return a projection payload: {other:?}")
            }
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_primary_key_covering_explain_reports_planner_proven_route() {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id FROM Customer ORDER BY id ASC LIMIT 1",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\")])")
                && explain.contains("covering_sources=List([Text(\"primary_key\")])"),
            "PK-only covering explain should expose the explicit covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"planner_proven\")"),
            "PK-only covering explain should report the planner-proven row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep witness-backed Customer secondary covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_explain_reports_witness_validated_route() {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "secondary covering explain should expose the explicit covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "secondary covering explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_name_only_stale_explain_matches_typed_surface()
     {
        reload_default_fixtures_with_customer_name_stale();

        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT name FROM Customer ORDER BY name ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale Customer name-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_name_only_stale_explain_reports_storage_existence_witness_route()
     {
        reload_default_fixtures_with_customer_name_stale();

        let explain =
            dispatch_explain_for_sql("EXPLAIN EXECUTION SELECT name FROM Customer ORDER BY name ASC LIMIT 2");

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"name\")])"),
            "stale Customer name-only covering explain should stay on the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")")
                && explain.contains("authority_decision=Text(\"storage_existence_witness\")")
                && explain.contains("authority_reason=Text(\"stale_storage_existence_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "stale Customer name-only covering explain should report the storage-owned existence witness mode: {explain}",
        );
        assert!(
            !explain.contains("row_check_required"),
            "stale Customer name-only covering explain should not fall back to row_check_required once the storage witness is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_building_explain_matches_typed_surface() {
        reload_default_fixtures_with_customer_index_building();

        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep building-index Customer covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_building_explain_reports_index_not_valid()
    {
        reload_default_fixtures_with_customer_index_building();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "building-index Customer covering explain should stay on the covering-read route contract while failing closed on authority: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"row_check_required\")")
                && explain.contains("authority_decision=Text(\"row_check_required\")")
                && explain.contains("authority_reason=Text(\"index_not_valid\")")
                && explain.contains("index_state=Text(\"building\")"),
            "building-index Customer covering explain should expose the explicit index_not_valid downgrade: {explain}",
        );
        assert!(
            !explain.contains("witness_validated"),
            "building-index Customer covering explain should not expose witness_validated authority: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_non_covering_explain_matches_typed_surface() {
        reload_default_fixtures();

        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT age FROM Customer ORDER BY name ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep non-covering Customer authority EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_non_covering_explain_reports_probe_required_reason()
    {
        reload_default_fixtures();

        let explain =
            dispatch_explain_for_sql("EXPLAIN EXECUTION SELECT age FROM Customer ORDER BY name ASC LIMIT 2");

        assert!(
            explain.contains("cov_read_route=Text(\"materialized\")"),
            "non-covering Customer explain should stay on the materialized route contract: {explain}",
        );
        assert!(
            explain.contains("authority_decision=Text(\"row_check_required\")")
                && explain.contains("authority_reason=Text(\"probe_required\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "non-covering Customer explain should expose the centralized probe_required authority classification: {explain}",
        );
        assert!(
            !explain.contains("witness_validated")
                && !explain.contains("storage_existence_witness"),
            "non-covering Customer explain must not surface probe-free authority labels: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_pk_plus_name_stale_explain_matches_typed_surface()
     {
        reload_default_fixtures_with_customer_name_stale();

        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale Customer PK-plus-name covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_pk_plus_name_stale_explain_reports_storage_existence_witness_route()
    {
        reload_default_fixtures_with_customer_name_stale();

        let explain =
            dispatch_explain_for_sql("EXPLAIN EXECUTION SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2");

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "stale Customer PK-plus-name covering explain should stay on the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")")
                && explain.contains("authority_decision=Text(\"storage_existence_witness\")")
                && explain.contains("authority_reason=Text(\"stale_storage_existence_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "stale Customer PK-plus-name covering explain should report the storage-owned existence witness authority classification: {explain}",
        );
        assert!(
            !explain.contains("row_check_required"),
            "stale Customer PK-plus-name covering explain should not fall back to row_check_required once the storage witness is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_stale_explain_matches_typed_surface()
    {
        reload_default_fixtures_with_customer_order_order_only_composite_stale();
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale CustomerOrder composite order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_stale_explain_reports_storage_existence_witness_route()
    {
        reload_default_fixtures_with_customer_order_order_only_composite_stale();
        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"),
            "stale CustomerOrder composite order-only explain should stay on the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")"),
            "stale CustomerOrder composite order-only explain should report the storage-owned existence witness mode: {explain}",
        );
        assert!(
            !explain.contains("authority_decision")
                && !explain.contains("authority_reason")
                && !explain.contains("index_state"),
            "stale CustomerOrder composite order-only explain should stay on the richer profile-owned surface instead of inheriting flat classifier labels: {explain}",
        );
        assert!(
            !explain.contains("row_check_required"),
            "stale CustomerOrder composite order-only explain should not fall back to row_check_required once the storage witness is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_leading_component_stale_explain_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_order_only_composite_stale();
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale CustomerOrder composite leading-component covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_leading_component_stale_explain_reports_storage_existence_witness_route(
    ) {
        reload_default_fixtures_with_customer_order_order_only_composite_stale();
        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"priority\")])"),
            "stale CustomerOrder composite leading-component explain should stay on the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")"),
            "stale CustomerOrder composite leading-component explain should report the storage-owned existence witness mode: {explain}",
        );
        assert!(
            !explain.contains("row_check_required"),
            "stale CustomerOrder composite leading-component explain should not fall back to row_check_required once the storage witness is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_leading_component_desc_stale_explain_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_order_only_composite_desc_stale();
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale descending CustomerOrder composite leading-component covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_leading_component_desc_stale_explain_reports_storage_existence_witness_route(
    ) {
        reload_default_fixtures_with_customer_order_order_only_composite_desc_stale();
        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"priority\")])"),
            "stale descending CustomerOrder composite leading-component explain should stay on the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")"),
            "stale descending CustomerOrder composite leading-component explain should report the storage-owned existence witness mode: {explain}",
        );
        assert!(
            !explain.contains("row_check_required"),
            "stale descending CustomerOrder composite leading-component explain should not fall back to row_check_required once the storage witness is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_desc_stale_explain_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_order_only_composite_desc_stale();
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale descending CustomerOrder composite order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_desc_stale_explain_reports_storage_existence_witness_route(
    ) {
        reload_default_fixtures_with_customer_order_order_only_composite_desc_stale();
        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"),
            "stale descending CustomerOrder composite order-only explain should stay on the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")"),
            "stale descending CustomerOrder composite order-only explain should report the storage-owned existence witness mode: {explain}",
        );
        assert!(
            !explain.contains("row_check_required"),
            "stale descending CustomerOrder composite order-only explain should not fall back to row_check_required once the storage witness is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_stale_explain_matches_typed_surface()
    {
        reload_default_fixtures_with_customer_order_numeric_equality_stale();
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale CustomerOrder numeric-equality covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_stale_explain_reports_storage_existence_witness_route(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_stale();
        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"
                ),
            "stale CustomerOrder numeric-equality explain should stay on the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")"),
            "stale CustomerOrder numeric-equality explain should report the storage-owned existence witness mode: {explain}",
        );
        assert!(
            !explain.contains("row_check_required"),
            "stale CustomerOrder numeric-equality explain should not fall back to row_check_required once the storage witness is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_desc_stale_explain_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_desc_stale();
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale descending CustomerOrder numeric-equality covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_desc_stale_projection_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_desc_stale();
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale descending CustomerOrder numeric-equality projection parity on the storage-witness route",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_desc_stale_explain_reports_storage_existence_witness_route(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_desc_stale();
        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"
                ),
            "stale descending CustomerOrder numeric-equality explain should stay on the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")"),
            "stale descending CustomerOrder numeric-equality explain should report the storage-owned existence witness mode: {explain}",
        );
        assert!(
            !explain.contains("row_check_required") && !explain.contains("witness_validated"),
            "stale descending CustomerOrder numeric-equality explain should not fall back once the shared covering-membership kernel keeps reverse traversal honest: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_leading_component_stale_explain_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_stale();
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale CustomerOrder numeric-equality leading-component EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_leading_component_stale_explain_reports_storage_existence_witness_route(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_stale();
        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain
                    .contains("covering_fields=List([Text(\"id\"), Text(\"priority\")])"),
            "stale CustomerOrder numeric-equality leading-component explain should stay on the covering-read route contract: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")"),
            "stale CustomerOrder numeric-equality leading-component explain should report the storage-owned existence witness mode once the shared covering-membership kernel can carry that authority shape: {explain}",
        );
        assert!(
            !explain.contains("row_check_required") && !explain.contains("witness_validated"),
            "stale CustomerOrder numeric-equality leading-component explain should not fall back once the covering-membership witness path is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_leading_component_stale_projection_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_stale();
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale CustomerOrder numeric-equality leading-component projection parity on the storage-witness route",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_leading_component_desc_stale_explain_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_desc_stale();
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale descending CustomerOrder numeric-equality leading-component EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_leading_component_desc_stale_explain_reports_storage_existence_witness_route(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_desc_stale();
        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain
                    .contains("covering_fields=List([Text(\"id\"), Text(\"priority\")])"),
            "stale descending CustomerOrder numeric-equality leading-component explain should stay on the covering-read route contract: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"storage_existence_witness\")"),
            "stale descending CustomerOrder numeric-equality leading-component explain should report the storage-owned existence witness mode once the reverse covering-membership kernel can carry that authority shape: {explain}",
        );
        assert!(
            !explain.contains("row_check_required") && !explain.contains("witness_validated"),
            "stale descending CustomerOrder numeric-equality leading-component explain should not fall back once the reverse covering-membership witness path is authoritative: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_leading_component_desc_stale_projection_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_order_numeric_equality_desc_stale();
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale descending CustomerOrder numeric-equality leading-component projection parity on the storage-witness route",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_equality_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep witness-backed Customer secondary equality covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_equality_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "secondary covering equality explain should expose the explicit covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "secondary covering equality explain should report the witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_equality_desc_explain_matches_typed_surface()
    {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id DESC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep witness-backed Customer secondary equality desc covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_equality_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id DESC LIMIT 1",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "secondary covering equality desc explain should expose the explicit covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "secondary covering equality desc explain should report the witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_strict_range_explain_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep witness-backed Customer secondary covering range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_strict_range_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "secondary covering range explain should expose the explicit covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "secondary covering range explain should report the witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_strict_range_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep witness-backed Customer secondary covering desc range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_strict_range_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "secondary covering desc range explain should expose the explicit covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")")
                && explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "secondary covering desc range explain should report the witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_equality_stale_explain_matches_typed_surface()
     {
        reload_default_fixtures_with_customer_name_stale();

        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep stale Customer secondary equality covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_equality_stale_explain_reports_authoritative_witness_unavailable(
    ) {
        reload_default_fixtures_with_customer_name_stale();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name = 'alice' ORDER BY id LIMIT 1",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "stale secondary covering equality explain should stay on the explicit covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"row_check_required\")")
                && explain.contains("authority_decision=Text(\"row_check_required\")")
                && explain.contains("authority_reason=Text(\"authoritative_witness_unavailable\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "stale secondary covering equality explain should expose the centralized authoritative_witness_unavailable downgrade: {explain}",
        );
        assert!(
            !explain.contains("witness_validated")
                && !explain.contains("storage_existence_witness"),
            "stale secondary covering equality explain should not expose probe-free authority labels: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_strict_range_stale_explain_matches_typed_surface(
    ) {
        reload_default_fixtures_with_customer_name_stale();

        assert_dispatch_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep stale Customer secondary covering range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_secondary_covering_strict_range_stale_explain_reports_authoritative_witness_unavailable(
    ) {
        reload_default_fixtures_with_customer_name_stale();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE name >= 'a' AND name < 'c' ORDER BY name ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"name\")])"),
            "stale secondary covering range explain should stay on the explicit covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"row_check_required\")")
                && explain.contains("authority_decision=Text(\"row_check_required\")")
                && explain.contains("authority_reason=Text(\"authoritative_witness_unavailable\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "stale secondary covering range explain should expose the centralized authoritative_witness_unavailable downgrade: {explain}",
        );
        assert!(
            !explain.contains("witness_validated")
                && !explain.contains("storage_existence_witness"),
            "stale secondary covering range explain should not expose probe-free authority labels: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_covering_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, name FROM CustomerOrder WHERE name = 'A-101' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_covering_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name = 'A-101' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_strict_like_prefix_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_strict_like_prefix_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_strict_like_prefix_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_strict_like_prefix_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_direct_starts_with_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder direct STARTS_WITH covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_direct_starts_with_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder direct STARTS_WITH covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_direct_starts_with_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder direct STARTS_WITH covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_direct_starts_with_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder direct STARTS_WITH covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_strict_text_range_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder strict text-range covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_strict_text_range_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder strict text-range covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_strict_text_range_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder strict text-range covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_strict_text_range_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder strict text-range covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_equivalent_strict_prefix_forms_match_projection_rows() {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name ASC, id ASC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated CustomerOrder STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated CustomerOrder text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_equivalent_desc_strict_prefix_forms_match_projection_rows()
    {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerOrder WHERE name LIKE 'A%' ORDER BY name DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerOrder WHERE STARTS_WITH(name, 'A') ORDER BY name DESC, id DESC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerOrder WHERE name >= 'A' AND name < 'B' ORDER BY name DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending CustomerOrder STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending CustomerOrder text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder order-only composite covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder order-only composite covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"
                ),
            "CustomerOrder order-only composite explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "CustomerOrder order-only composite explain should report the witness-backed row mode: {explain}",
        );
        assert!(
            explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "CustomerOrder order-only composite explain should expose the centralized witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_desc_projection_matches_typed_surface()
    {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder order-only composite covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder order-only composite covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_order_only_composite_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"
                ),
            "descending CustomerOrder order-only composite explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "descending CustomerOrder order-only composite explain should report the witness-backed row mode: {explain}",
        );
        assert!(
            explain.contains("authority_decision=Text(\"witness_validated\")")
                && explain.contains("authority_reason=Text(\"synchronized_pair_witness\")")
                && explain.contains("index_state=Text(\"valid\")"),
            "descending CustomerOrder order-only composite explain should expose the centralized witness-backed authority classification: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder numeric-equality projection parity on uint-backed fields",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder numeric-equality EXPLAIN parity on uint-backed fields",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_explain_reports_witness_validated_route() {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"
                ),
            "CustomerOrder numeric-equality explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "CustomerOrder numeric-equality explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_desc_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder numeric-equality projection parity on uint-backed fields",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder numeric-equality EXPLAIN parity on uint-backed fields",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"
                ),
            "descending CustomerOrder numeric-equality explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "descending CustomerOrder numeric-equality explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_status_strict_text_range_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder numeric-equality bounded status projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_status_strict_text_range_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerOrder numeric-equality bounded status EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_status_strict_text_range_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"
                ),
            "CustomerOrder numeric-equality bounded status explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "CustomerOrder numeric-equality bounded status explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_status_strict_text_range_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerOrder>(
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder numeric-equality bounded status projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_status_strict_text_range_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerOrder>(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerOrder numeric-equality bounded status EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_order_numeric_equality_status_strict_text_range_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 AND status >= 'B' AND status < 'D' ORDER BY status DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"priority\"), Text(\"status\")])"
                ),
            "descending CustomerOrder numeric-equality bounded status explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "descending CustomerOrder numeric-equality bounded status explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_order_only_projection_matches_typed_surface() {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered order-only covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_order_only_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered order-only covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_order_only_desc_explain_matches_typed_surface() {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_strict_like_prefix_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_strict_like_prefix_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_strict_like_prefix_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_strict_like_prefix_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_equivalent_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name ASC, id ASC LIMIT 1",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name ASC, id ASC LIMIT 1",
        );

        assert_eq!(
            starts_with, like,
            "generated CustomerAccount STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated CustomerAccount text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_equivalent_desc_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND STARTS_WITH(name, 'br') ORDER BY name DESC, id DESC LIMIT 1",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name >= 'br' AND name < 'bs' ORDER BY name DESC, id DESC LIMIT 1",
        );

        assert_eq!(
            starts_with, like,
            "generated descending CustomerAccount STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending CustomerAccount text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_strict_like_prefix_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_strict_like_prefix_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_strict_like_prefix_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite strict LIKE prefix covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_strict_like_prefix_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite strict LIKE prefix covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite order-only covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"tier\"), Text(\"handle\")])"
                ),
            "CustomerAccount filtered composite order-only explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "CustomerAccount filtered composite order-only explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_strict_like_prefix_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"tier\"), Text(\"handle\")])"
                ),
            "CustomerAccount filtered composite strict LIKE prefix explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "CustomerAccount filtered composite strict LIKE prefix explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_strict_like_prefix_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"tier\"), Text(\"handle\")])"
                ),
            "descending CustomerAccount filtered composite strict LIKE prefix explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "descending CustomerAccount filtered composite strict LIKE prefix explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite order-only covering projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite order-only covering EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"tier\"), Text(\"handle\")])"
                ),
            "descending CustomerAccount filtered composite order-only explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "descending CustomerAccount filtered composite order-only explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_desc_offset_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2 OFFSET 1",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains(
                    "covering_fields=List([Text(\"id\"), Text(\"tier\"), Text(\"handle\")])"
                ),
            "descending CustomerAccount filtered composite order-only offset explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "descending CustomerAccount filtered composite order-only offset explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_order_only_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered expression-order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_order_only_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered expression-order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered expression-order projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_order_only_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered expression-order EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_strict_like_prefix_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered expression strict LIKE prefix projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_strict_like_prefix_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered expression strict LIKE prefix EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_strict_like_prefix_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered expression strict LIKE prefix projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_strict_like_prefix_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered expression strict LIKE prefix EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_equivalent_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated CustomerAccount filtered expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_equivalent_desc_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending CustomerAccount filtered expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_order_only_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression order-only projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_order_only_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression order-only EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_order_only_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression key-only order-only projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_order_only_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression key-only order-only EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_order_only_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"tier\")])"),
            "CustomerAccount filtered composite expression key-only order-only explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "CustomerAccount filtered composite expression key-only order-only explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression key-only order-only projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_order_only_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression key-only order-only EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_order_only_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"tier\")])"),
            "descending CustomerAccount filtered composite expression key-only order-only explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "descending CustomerAccount filtered composite expression key-only order-only explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_strict_text_range_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression key-only strict text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_strict_text_range_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression key-only strict text-range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_strict_text_range_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"tier\")])"),
            "CustomerAccount filtered composite expression key-only strict text-range explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "CustomerAccount filtered composite expression key-only strict text-range explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_strict_text_range_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression key-only strict text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_strict_text_range_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression key-only strict text-range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_strict_text_range_desc_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"tier\")])"),
            "descending CustomerAccount filtered composite expression key-only strict text-range explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "descending CustomerAccount filtered composite expression key-only strict text-range explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_direct_starts_with_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression key-only direct STARTS_WITH projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_direct_starts_with_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression key-only direct STARTS_WITH EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_key_only_direct_starts_with_explain_reports_witness_validated_route()
     {
        reload_default_fixtures();

        let explain = dispatch_explain_for_sql(
            "EXPLAIN EXECUTION SELECT id, tier FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'br') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert!(
            explain.contains("cov_read_route=Text(\"covering_read\")")
                && explain.contains("covering_fields=List([Text(\"id\"), Text(\"tier\")])"),
            "CustomerAccount filtered composite expression key-only direct STARTS_WITH explain should expose the covering-read route: {explain}",
        );
        assert!(
            explain.contains("existing_row_mode=Text(\"witness_validated\")"),
            "CustomerAccount filtered composite expression key-only direct STARTS_WITH explain should report the witness-backed row mode: {explain}",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_order_only_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression order-only projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_order_only_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression order-only EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_strict_like_prefix_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression strict LIKE prefix projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_strict_like_prefix_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression strict LIKE prefix EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_strict_like_prefix_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression strict LIKE prefix projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_strict_like_prefix_desc_explain_matches_typed_surface()
     {
        assert_dispatch_matches_typed_as::<CustomerAccount>(
            "EXPLAIN EXECUTION SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression strict LIKE prefix EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_equivalent_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated CustomerAccount filtered composite expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_equivalent_desc_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending CustomerAccount filtered composite expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_equivalent_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle ASC, id ASC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated CustomerAccount filtered composite STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated CustomerAccount filtered composite text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_equivalent_desc_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(handle, 'br') ORDER BY handle DESC, id DESC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle >= 'br' AND handle < 'bs' ORDER BY handle DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending CustomerAccount filtered composite STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending CustomerAccount filtered composite text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_strict_like_prefix_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name ASC, id ASC LIMIT 1",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 1);
                assert_eq!(rows.rows.len(), 1);
                assert_eq!(rows.rows[0][1], "bravo");
            }
            other => panic!(
                "filtered strict LIKE prefix CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_strict_like_prefix_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true AND name LIKE 'br%' ORDER BY name DESC, id DESC LIMIT 1",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 1);
                assert_eq!(rows.rows.len(), 1);
                assert_eq!(rows.rows[0][1], "bravo");
            }
            other => panic!(
                "descending filtered strict LIKE prefix CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_strict_like_prefix_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
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
                "filtered composite strict LIKE prefix CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_strict_like_prefix_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND handle LIKE 'br%' ORDER BY handle DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
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
                "descending filtered composite strict LIKE prefix CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
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
                "filtered composite order-only CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_order_only_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY handle DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
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
                "descending filtered composite order-only CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_order_only_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
                assert_eq!(rows.columns, vec!["id".to_string(), "handle".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bravo");
                assert_eq!(rows.rows[1][1], "Brisk");
            }
            other => panic!(
                "filtered expression order-only CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_order_only_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
                assert_eq!(rows.columns, vec!["id".to_string(), "handle".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bristle");
                assert_eq!(rows.rows[1][1], "Brisk");
            }
            other => panic!(
                "descending filtered expression order-only CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_strict_like_prefix_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
                assert_eq!(rows.columns, vec!["id".to_string(), "handle".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bravo");
                assert_eq!(rows.rows[1][1], "Brisk");
            }
            other => panic!(
                "filtered expression strict LIKE prefix CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_strict_like_prefix_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
                assert_eq!(rows.columns, vec!["id".to_string(), "handle".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bristle");
                assert_eq!(rows.rows[1][1], "Brisk");
            }
            other => panic!(
                "descending filtered expression strict LIKE prefix CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_strict_text_range_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered expression text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_strict_text_range_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered expression text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_equivalent_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated CustomerAccount filtered expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated CustomerAccount filtered expression text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_expression_equivalent_desc_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, handle FROM CustomerAccount WHERE active = true AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending CustomerAccount filtered expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending CustomerAccount filtered expression text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_order_only_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
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
                "filtered composite expression order-only CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_order_only_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
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
                "descending filtered composite expression order-only CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_strict_like_prefix_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
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
                "filtered composite expression strict LIKE prefix CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_strict_like_prefix_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
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
                "descending filtered composite expression strict LIKE prefix CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_strict_text_range_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep CustomerAccount filtered composite expression text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_strict_text_range_desc_projection_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed_as::<CustomerAccount>(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep descending CustomerAccount filtered composite expression text-range projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_equivalent_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) ASC, id ASC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated CustomerAccount filtered composite expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated CustomerAccount filtered composite expression text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_composite_expression_equivalent_desc_strict_prefix_forms_match_projection_rows()
     {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) LIKE 'br%' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND STARTS_WITH(LOWER(handle), 'BR') ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, tier, handle FROM CustomerAccount WHERE active = true AND tier = 'gold' AND LOWER(handle) >= 'br' AND LOWER(handle) < 'bs' ORDER BY LOWER(handle) DESC, id DESC LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated descending CustomerAccount filtered composite expression STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated descending CustomerAccount filtered composite expression text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_order_only_projection_matches_expected_rows() {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name ASC, id ASC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "bravo");
                assert_eq!(rows.rows[1][1], "charlie");
            }
            other => panic!(
                "filtered order-only CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_customer_account_filtered_order_only_desc_projection_matches_expected_rows()
     {
        reload_default_fixtures();

        let payload = dispatch_result_for_sql(
            "SELECT id, name FROM CustomerAccount WHERE active = true ORDER BY name DESC, id DESC LIMIT 2",
        );

        match payload {
            SqlQueryResult::Projection(rows) => {
                assert_eq!(rows.entity, "CustomerAccount");
                assert_eq!(rows.columns, vec!["id".to_string(), "name".to_string()]);
                assert_eq!(rows.row_count, 2);
                assert_eq!(rows.rows.len(), 2);
                assert_eq!(rows.rows[0][1], "echo");
                assert_eq!(rows.rows[1][1], "charlie");
            }
            other => panic!(
                "descending filtered order-only CustomerAccount projection should return a projection payload: {other:?}"
            ),
        }
    }

    #[test]
    fn generated_sql_dispatch_global_aggregate_execution_stays_fail_closed() {
        let sql = "SELECT COUNT(*) FROM Customer";
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
    fn typed_execute_sql_aggregate_customer_count_queries_return_expected_values() {
        reload_default_fixtures();

        let count_rows = typed_aggregate_value_for_sql("SELECT COUNT(*) FROM Customer");
        let count_age = typed_aggregate_value_for_sql("SELECT COUNT(age) FROM Customer");

        assert_eq!(
            count_rows,
            Value::Uint(3),
            "typed execute_sql_aggregate COUNT(*) should return the default Customer fixture cardinality",
        );
        assert_eq!(
            count_age,
            Value::Uint(3),
            "typed execute_sql_aggregate COUNT(age) should count all non-null Customer ages in the default fixture set",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_count_perf_surface_reports_expected_values() {
        for (sql, expected_rendered_value) in [
            ("SELECT COUNT(*) FROM Customer", "Uint(3)"),
            ("SELECT COUNT(age) FROM Customer", "Uint(3)"),
        ] {
            let sample = perf_sample(SqlPerfSurface::TypedExecuteSqlAggregateCustomer, sql);

            assert!(
                sample.outcome.success,
                "typed execute_sql_aggregate perf sample should succeed for `{sql}`: {sample:?}",
            );
            assert_eq!(
                sample.outcome.result_kind,
                "aggregate_value",
                "typed execute_sql_aggregate perf sample should keep the aggregate outcome kind for `{sql}`",
            );
            assert_eq!(
                sample.outcome.entity.as_deref(),
                Some("Customer"),
                "typed execute_sql_aggregate perf sample should stay on the Customer aggregate lane for `{sql}`",
            );
            assert_eq!(
                sample.outcome.rendered_value.as_deref(),
                Some(expected_rendered_value),
                "typed execute_sql_aggregate perf sample should render the expected scalar value for `{sql}`",
            );
            assert_eq!(
                sample.outcome.row_count,
                None,
                "typed execute_sql_aggregate perf sample should stay scalar for `{sql}`",
            );
            assert_eq!(
                sample.outcome.has_cursor,
                None,
                "typed execute_sql_aggregate perf sample should not expose cursor state for `{sql}`",
            );
        }
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_numeric_queries_return_expected_values() {
        reload_default_fixtures();

        let min_age = typed_aggregate_value_for_sql("SELECT MIN(age) FROM Customer");
        let max_age = typed_aggregate_value_for_sql("SELECT MAX(age) FROM Customer");
        let sum_age = typed_aggregate_value_for_sql("SELECT SUM(age) FROM Customer");
        let avg_age = typed_aggregate_value_for_sql("SELECT AVG(age) FROM Customer");

        assert_eq!(
            min_age,
            Value::Int(24),
            "typed execute_sql_aggregate MIN(age) should return the smallest default Customer age",
        );
        assert_eq!(
            max_age,
            Value::Int(43),
            "typed execute_sql_aggregate MAX(age) should return the largest default Customer age",
        );
        assert_eq!(
            sum_age,
            Value::Decimal(Decimal::from(98u64)),
            "typed execute_sql_aggregate SUM(age) should return the default Customer age total",
        );
        assert_eq!(
            avg_age,
            Value::Decimal(Decimal::from_i128_with_scale(32_666_666_666_666_666_667, 18)),
            "typed execute_sql_aggregate AVG(age) should preserve the decimal average across the default Customer fixture set",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_numeric_perf_surface_reports_expected_values() {
        for (sql, expected_rendered_value) in [
            ("SELECT MIN(age) FROM Customer", "Int(24)"),
            ("SELECT MAX(age) FROM Customer", "Int(43)"),
            (
                "SELECT SUM(age) FROM Customer",
                "Decimal(Decimal { mantissa: 98, scale: 0 })",
            ),
            (
                "SELECT AVG(age) FROM Customer",
                "Decimal(Decimal { mantissa: 32666666666666666667, scale: 18 })",
            ),
        ] {
            let sample = perf_sample(SqlPerfSurface::TypedExecuteSqlAggregateCustomer, sql);

            assert!(
                sample.outcome.success,
                "typed execute_sql_aggregate perf sample should succeed for `{sql}`: {sample:?}",
            );
            assert_eq!(
                sample.outcome.result_kind,
                "aggregate_value",
                "typed execute_sql_aggregate perf sample should keep the aggregate outcome kind for `{sql}`",
            );
            assert_eq!(
                sample.outcome.entity.as_deref(),
                Some("Customer"),
                "typed execute_sql_aggregate perf sample should stay on the Customer aggregate lane for `{sql}`",
            );
            assert_eq!(
                sample.outcome.rendered_value.as_deref(),
                Some(expected_rendered_value),
                "typed execute_sql_aggregate perf sample should render the expected scalar value for `{sql}`",
            );
            assert_eq!(
                sample.outcome.row_count,
                None,
                "typed execute_sql_aggregate perf sample should stay scalar for `{sql}`",
            );
            assert_eq!(
                sample.outcome.has_cursor,
                None,
                "typed execute_sql_aggregate perf sample should not expose cursor state for `{sql}`",
            );
        }
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_filtered_queries_return_expected_values() {
        reload_default_fixtures();

        let count_rows =
            typed_aggregate_value_for_sql("SELECT COUNT(*) FROM Customer WHERE age >= 30");
        let count_age =
            typed_aggregate_value_for_sql("SELECT COUNT(age) FROM Customer WHERE age >= 30");
        let min_age = typed_aggregate_value_for_sql("SELECT MIN(age) FROM Customer WHERE age >= 30");
        let max_age = typed_aggregate_value_for_sql("SELECT MAX(age) FROM Customer WHERE age >= 30");
        let sum_age = typed_aggregate_value_for_sql("SELECT SUM(age) FROM Customer WHERE age >= 30");
        let avg_age = typed_aggregate_value_for_sql("SELECT AVG(age) FROM Customer WHERE age >= 30");

        assert_eq!(
            count_rows,
            Value::Uint(2),
            "typed execute_sql_aggregate COUNT(*) should respect filtered Customer windows",
        );
        assert_eq!(
            count_age,
            Value::Uint(2),
            "typed execute_sql_aggregate COUNT(age) should respect filtered Customer windows",
        );
        assert_eq!(
            min_age,
            Value::Int(31),
            "typed execute_sql_aggregate MIN(age) should keep filtered Customer scalar typing",
        );
        assert_eq!(
            max_age,
            Value::Int(43),
            "typed execute_sql_aggregate MAX(age) should keep filtered Customer scalar typing",
        );
        assert_eq!(
            sum_age,
            Value::Decimal(Decimal::from(74u64)),
            "typed execute_sql_aggregate SUM(age) should total the filtered Customer ages",
        );
        assert_eq!(
            avg_age,
            Value::Decimal(Decimal::from(37u64)),
            "typed execute_sql_aggregate AVG(age) should preserve the filtered Customer average",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_filtered_perf_surface_reports_expected_values() {
        for (sql, expected_rendered_value) in [
            ("SELECT COUNT(*) FROM Customer WHERE age >= 30", "Uint(2)"),
            ("SELECT COUNT(age) FROM Customer WHERE age >= 30", "Uint(2)"),
            ("SELECT MIN(age) FROM Customer WHERE age >= 30", "Int(31)"),
            ("SELECT MAX(age) FROM Customer WHERE age >= 30", "Int(43)"),
            (
                "SELECT SUM(age) FROM Customer WHERE age >= 30",
                "Decimal(Decimal { mantissa: 74, scale: 0 })",
            ),
            (
                "SELECT AVG(age) FROM Customer WHERE age >= 30",
                "Decimal(Decimal { mantissa: 37, scale: 0 })",
            ),
        ] {
            let sample = perf_sample(SqlPerfSurface::TypedExecuteSqlAggregateCustomer, sql);

            assert!(
                sample.outcome.success,
                "typed execute_sql_aggregate perf sample should succeed for `{sql}`: {sample:?}",
            );
            assert_eq!(
                sample.outcome.result_kind,
                "aggregate_value",
                "typed execute_sql_aggregate perf sample should keep the aggregate outcome kind for `{sql}`",
            );
            assert_eq!(
                sample.outcome.entity.as_deref(),
                Some("Customer"),
                "typed execute_sql_aggregate perf sample should stay on the Customer aggregate lane for `{sql}`",
            );
            assert_eq!(
                sample.outcome.rendered_value.as_deref(),
                Some(expected_rendered_value),
                "typed execute_sql_aggregate perf sample should render the expected filtered scalar value for `{sql}`",
            );
            assert_eq!(
                sample.outcome.row_count,
                None,
                "typed execute_sql_aggregate perf sample should stay scalar for `{sql}`",
            );
            assert_eq!(
                sample.outcome.has_cursor,
                None,
                "typed execute_sql_aggregate perf sample should not expose cursor state for `{sql}`",
            );
        }
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_empty_window_queries_return_expected_values() {
        reload_default_fixtures();

        let count_rows = typed_aggregate_value_for_sql("SELECT COUNT(*) FROM Customer WHERE age < 0");
        let sum_age = typed_aggregate_value_for_sql("SELECT SUM(age) FROM Customer WHERE age < 0");
        let avg_age = typed_aggregate_value_for_sql("SELECT AVG(age) FROM Customer WHERE age < 0");
        let min_age = typed_aggregate_value_for_sql("SELECT MIN(age) FROM Customer WHERE age < 0");
        let max_age = typed_aggregate_value_for_sql("SELECT MAX(age) FROM Customer WHERE age < 0");

        assert_eq!(
            count_rows,
            Value::Uint(0),
            "typed execute_sql_aggregate COUNT(*) should return zero on an empty filtered Customer window",
        );
        assert_eq!(
            sum_age,
            Value::Null,
            "typed execute_sql_aggregate SUM(age) should stay null on an empty filtered Customer window",
        );
        assert_eq!(
            avg_age,
            Value::Null,
            "typed execute_sql_aggregate AVG(age) should stay null on an empty filtered Customer window",
        );
        assert_eq!(
            min_age,
            Value::Null,
            "typed execute_sql_aggregate MIN(age) should stay null on an empty filtered Customer window",
        );
        assert_eq!(
            max_age,
            Value::Null,
            "typed execute_sql_aggregate MAX(age) should stay null on an empty filtered Customer window",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_empty_window_perf_surface_reports_expected_values() {
        for (sql, expected_rendered_value) in [
            ("SELECT COUNT(*) FROM Customer WHERE age < 0", "Uint(0)"),
            ("SELECT SUM(age) FROM Customer WHERE age < 0", "Null"),
            ("SELECT AVG(age) FROM Customer WHERE age < 0", "Null"),
            ("SELECT MIN(age) FROM Customer WHERE age < 0", "Null"),
            ("SELECT MAX(age) FROM Customer WHERE age < 0", "Null"),
        ] {
            let sample = perf_sample(SqlPerfSurface::TypedExecuteSqlAggregateCustomer, sql);

            assert!(
                sample.outcome.success,
                "typed execute_sql_aggregate perf sample should succeed for `{sql}`: {sample:?}",
            );
            assert_eq!(
                sample.outcome.result_kind,
                "aggregate_value",
                "typed execute_sql_aggregate perf sample should keep the aggregate outcome kind for `{sql}`",
            );
            assert_eq!(
                sample.outcome.entity.as_deref(),
                Some("Customer"),
                "typed execute_sql_aggregate perf sample should stay on the Customer aggregate lane for `{sql}`",
            );
            assert_eq!(
                sample.outcome.rendered_value.as_deref(),
                Some(expected_rendered_value),
                "typed execute_sql_aggregate perf sample should render the expected empty-window scalar value for `{sql}`",
            );
            assert_eq!(
                sample.outcome.row_count,
                None,
                "typed execute_sql_aggregate perf sample should stay scalar for `{sql}`",
            );
            assert_eq!(
                sample.outcome.has_cursor,
                None,
                "typed execute_sql_aggregate perf sample should not expose cursor state for `{sql}`",
            );
        }
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_window_queries_return_expected_values() {
        reload_default_fixtures();

        let count_rows = typed_aggregate_value_for_sql(
            "SELECT COUNT(*) FROM Customer ORDER BY age DESC LIMIT 2 OFFSET 1",
        );
        let sum_age = typed_aggregate_value_for_sql(
            "SELECT SUM(age) FROM Customer ORDER BY age DESC LIMIT 1 OFFSET 1",
        );
        let avg_age = typed_aggregate_value_for_sql(
            "SELECT AVG(age) FROM Customer ORDER BY age ASC LIMIT 2 OFFSET 1",
        );

        assert_eq!(
            count_rows,
            Value::Uint(2),
            "typed execute_sql_aggregate COUNT(*) should respect Customer order/limit/offset windows",
        );
        assert_eq!(
            sum_age,
            Value::Decimal(Decimal::from(31u64)),
            "typed execute_sql_aggregate SUM(age) should respect Customer order/limit/offset windows",
        );
        assert_eq!(
            avg_age,
            Value::Decimal(Decimal::from(37u64)),
            "typed execute_sql_aggregate AVG(age) should respect Customer order/limit/offset windows",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_window_perf_surface_reports_expected_values() {
        for (sql, expected_rendered_value) in [
            (
                "SELECT COUNT(*) FROM Customer ORDER BY age DESC LIMIT 2 OFFSET 1",
                "Uint(2)",
            ),
            (
                "SELECT SUM(age) FROM Customer ORDER BY age DESC LIMIT 1 OFFSET 1",
                "Decimal(Decimal { mantissa: 31, scale: 0 })",
            ),
            (
                "SELECT AVG(age) FROM Customer ORDER BY age ASC LIMIT 2 OFFSET 1",
                "Decimal(Decimal { mantissa: 37, scale: 0 })",
            ),
        ] {
            let sample = perf_sample(SqlPerfSurface::TypedExecuteSqlAggregateCustomer, sql);

            assert!(
                sample.outcome.success,
                "typed execute_sql_aggregate perf sample should succeed for `{sql}`: {sample:?}",
            );
            assert_eq!(
                sample.outcome.result_kind,
                "aggregate_value",
                "typed execute_sql_aggregate perf sample should keep the aggregate outcome kind for `{sql}`",
            );
            assert_eq!(
                sample.outcome.entity.as_deref(),
                Some("Customer"),
                "typed execute_sql_aggregate perf sample should stay on the Customer aggregate lane for `{sql}`",
            );
            assert_eq!(
                sample.outcome.rendered_value.as_deref(),
                Some(expected_rendered_value),
                "typed execute_sql_aggregate perf sample should render the expected windowed scalar value for `{sql}`",
            );
            assert_eq!(
                sample.outcome.row_count,
                None,
                "typed execute_sql_aggregate perf sample should stay scalar for `{sql}`",
            );
            assert_eq!(
                sample.outcome.has_cursor,
                None,
                "typed execute_sql_aggregate perf sample should not expose cursor state for `{sql}`",
            );
        }
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_offset_beyond_window_queries_return_expected_values() {
        reload_default_fixtures();

        let count_rows = typed_aggregate_value_for_sql(
            "SELECT COUNT(*) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        );
        let sum_age = typed_aggregate_value_for_sql(
            "SELECT SUM(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        );
        let avg_age = typed_aggregate_value_for_sql(
            "SELECT AVG(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        );
        let min_age = typed_aggregate_value_for_sql(
            "SELECT MIN(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        );
        let max_age = typed_aggregate_value_for_sql(
            "SELECT MAX(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
        );

        assert_eq!(
            count_rows,
            Value::Uint(0),
            "typed execute_sql_aggregate COUNT(*) should return zero when offset removes the full Customer window",
        );
        assert_eq!(
            sum_age,
            Value::Null,
            "typed execute_sql_aggregate SUM(age) should stay null when offset removes the full Customer window",
        );
        assert_eq!(
            avg_age,
            Value::Null,
            "typed execute_sql_aggregate AVG(age) should stay null when offset removes the full Customer window",
        );
        assert_eq!(
            min_age,
            Value::Null,
            "typed execute_sql_aggregate MIN(age) should stay null when offset removes the full Customer window",
        );
        assert_eq!(
            max_age,
            Value::Null,
            "typed execute_sql_aggregate MAX(age) should stay null when offset removes the full Customer window",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_offset_beyond_window_perf_surface_reports_expected_values(
    ) {
        for (sql, expected_rendered_value) in [
            (
                "SELECT COUNT(*) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
                "Uint(0)",
            ),
            (
                "SELECT SUM(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
                "Null",
            ),
            (
                "SELECT AVG(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
                "Null",
            ),
            (
                "SELECT MIN(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
                "Null",
            ),
            (
                "SELECT MAX(age) FROM Customer ORDER BY age ASC LIMIT 1 OFFSET 10",
                "Null",
            ),
        ] {
            let sample = perf_sample(SqlPerfSurface::TypedExecuteSqlAggregateCustomer, sql);

            assert!(
                sample.outcome.success,
                "typed execute_sql_aggregate perf sample should succeed for `{sql}`: {sample:?}",
            );
            assert_eq!(
                sample.outcome.result_kind,
                "aggregate_value",
                "typed execute_sql_aggregate perf sample should keep the aggregate outcome kind for `{sql}`",
            );
            assert_eq!(
                sample.outcome.entity.as_deref(),
                Some("Customer"),
                "typed execute_sql_aggregate perf sample should stay on the Customer aggregate lane for `{sql}`",
            );
            assert_eq!(
                sample.outcome.rendered_value.as_deref(),
                Some(expected_rendered_value),
                "typed execute_sql_aggregate perf sample should render the expected offset-beyond-window scalar value for `{sql}`",
            );
            assert_eq!(
                sample.outcome.row_count,
                None,
                "typed execute_sql_aggregate perf sample should stay scalar for `{sql}`",
            );
            assert_eq!(
                sample.outcome.has_cursor,
                None,
                "typed execute_sql_aggregate perf sample should not expose cursor state for `{sql}`",
            );
        }
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_rejects_non_aggregate_select_in_current_lane() {
        reload_default_fixtures();

        let err = typed_aggregate_value_for_sql_unchecked("SELECT age FROM Customer")
            .expect_err("non-aggregate SELECT should stay fail-closed for execute_sql_aggregate");

        assert_eq!(
            err.kind(),
            &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            "typed execute_sql_aggregate should map non-aggregate SELECT rejection onto Runtime::Unsupported",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "typed execute_sql_aggregate should keep non-aggregate SELECT rejection on the query origin",
        );
        assert!(
            err.to_string()
                .contains("execute_sql_aggregate requires constrained global aggregate SELECT"),
            "typed execute_sql_aggregate should preserve constrained aggregate-surface guidance for non-aggregate SELECT",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_rejects_grouped_select_in_current_lane() {
        reload_default_fixtures();

        let err = typed_aggregate_value_for_sql_unchecked(
            "SELECT age, COUNT(*) FROM Customer GROUP BY age",
        )
        .expect_err("grouped SELECT should stay fail-closed for execute_sql_aggregate");

        assert_eq!(
            err.kind(),
            &ErrorKind::Runtime(RuntimeErrorKind::Unsupported),
            "typed execute_sql_aggregate should map grouped SELECT rejection onto Runtime::Unsupported",
        );
        assert_eq!(
            err.origin(),
            ErrorOrigin::Query,
            "typed execute_sql_aggregate should keep grouped SELECT rejection on the query origin",
        );
        assert!(
            err.to_string()
                .contains("execute_sql_aggregate rejects grouped SELECT"),
            "typed execute_sql_aggregate should preserve grouped-entrypoint guidance for grouped SELECT rejection",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_reject_path_perf_surface_reports_non_aggregate_error() {
        let sample = perf_sample(
            SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
            "SELECT age FROM Customer",
        );

        assert!(
            !sample.outcome.success,
            "typed execute_sql_aggregate perf sample should fail for non-aggregate SELECT: {sample:?}",
        );
        assert_eq!(
            sample.outcome.result_kind,
            "error",
            "typed execute_sql_aggregate perf sample should classify non-aggregate SELECT as an error",
        );
        assert_eq!(
            sample.outcome.error_kind.as_deref(),
            Some("Runtime(Unsupported)"),
            "typed execute_sql_aggregate perf sample should preserve Runtime::Unsupported for non-aggregate SELECT",
        );
        assert_eq!(
            sample.outcome.error_origin.as_deref(),
            Some("Query"),
            "typed execute_sql_aggregate perf sample should preserve Query origin for non-aggregate SELECT",
        );
        assert!(
            sample
                .outcome
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("execute_sql_aggregate requires constrained global aggregate SELECT")),
            "typed execute_sql_aggregate perf sample should preserve constrained aggregate-surface guidance for non-aggregate SELECT",
        );
    }

    #[test]
    fn typed_execute_sql_aggregate_customer_reject_path_perf_surface_reports_grouped_error() {
        let sample = perf_sample(
            SqlPerfSurface::TypedExecuteSqlAggregateCustomer,
            "SELECT age, COUNT(*) FROM Customer GROUP BY age",
        );

        assert!(
            !sample.outcome.success,
            "typed execute_sql_aggregate perf sample should fail for grouped SELECT: {sample:?}",
        );
        assert_eq!(
            sample.outcome.result_kind,
            "error",
            "typed execute_sql_aggregate perf sample should classify grouped SELECT as an error",
        );
        assert_eq!(
            sample.outcome.error_kind.as_deref(),
            Some("Runtime(Unsupported)"),
            "typed execute_sql_aggregate perf sample should preserve Runtime::Unsupported for grouped SELECT",
        );
        assert_eq!(
            sample.outcome.error_origin.as_deref(),
            Some("Query"),
            "typed execute_sql_aggregate perf sample should preserve Query origin for grouped SELECT",
        );
        assert!(
            sample
                .outcome
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("execute_sql_aggregate rejects grouped SELECT")),
            "typed execute_sql_aggregate perf sample should preserve grouped-entrypoint guidance for grouped SELECT",
        );
    }

    #[test]
    fn generated_sql_dispatch_grouped_execution_stays_fail_closed() {
        let sql = "SELECT age, COUNT(*) FROM Customer GROUP BY age";
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
            "EXPLAIN SELECT age, COUNT(*) FROM Customer GROUP BY age",
            "typed execute_sql_dispatch and sql_dispatch should keep grouped EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_global_aggregate_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT COUNT(*) FROM Customer",
            "typed execute_sql_dispatch and sql_dispatch should keep global aggregate EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_global_aggregate_explain_execution_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN EXECUTION SELECT COUNT(*) FROM Customer",
            "typed execute_sql_dispatch and sql_dispatch should keep global aggregate EXPLAIN EXECUTION parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_starts_with_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM Customer WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct STARTS_WITH parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_starts_with_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM Customer WHERE STARTS_WITH(name, 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct STARTS_WITH EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_starts_with_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) STARTS_WITH parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_starts_with_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) STARTS_WITH EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_starts_with_explain_json_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN JSON SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) STARTS_WITH EXPLAIN JSON parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_strict_text_range_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) ordered text-range parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_strict_text_range_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) ordered text-range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_strict_text_range_explain_json_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) ordered text-range EXPLAIN JSON parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_strict_text_range_explain_execution_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) ordered text-range EXPLAIN EXECUTION parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_equivalent_prefix_forms_match_explain_execution_route() {
        reload_default_fixtures();

        let explains = [
            dispatch_explain_for_sql(
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            ),
        ];

        for explain in explains {
            assert!(
                explain.contains("IndexRangeScan")
                    && explain.contains("OrderByMaterializedSort")
                    && explain.contains("proj_fields=List([Text(\"id\"), Text(\"name\")])"),
                "direct LOWER(field) equivalent prefix-form explains should preserve the shared expression index-range materialized route: {explain}",
            );
            assert!(
                !explain.contains("FullScan"),
                "direct LOWER(field) equivalent prefix-form explains must not fall back to full scan: {explain}",
            );
        }
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_equivalent_prefix_forms_match_explain_json_route() {
        reload_default_fixtures();

        let explains = [
            dispatch_explain_for_sql(
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
            ),
        ];

        for explain in explains {
            assert!(
                explain.contains("\"mode\":{\"type\":\"Load\"")
                    && explain.contains("\"access\":{\"type\":\"IndexRange\""),
                "direct LOWER(field) equivalent prefix-form JSON explains should preserve the shared expression index-range route: {explain}",
            );
            assert!(
                !explain.contains("\"type\":\"FullScan\""),
                "direct LOWER(field) equivalent prefix-form JSON explains must not fall back to full scan: {explain}",
            );
        }
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_equivalent_prefix_forms_match_projection_rows() {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated direct LOWER(field) STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated direct LOWER(field) ordered text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_strict_text_range_delete_matches_typed_surface() {
        assert_delete_dispatch_result_matches_typed(
            "DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) ordered text-range DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_strict_text_range_explain_delete_matches_typed_surface()
    {
        assert_dispatch_result_matches_typed(
            "EXPLAIN DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) ordered text-range EXPLAIN DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_strict_text_range_explain_json_delete_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed(
            "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep direct LOWER(field) ordered text-range EXPLAIN JSON DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_lower_delete_equivalent_prefix_forms_match_explain_json_route()
    {
        reload_default_fixtures();

        let explains = [
            dispatch_explain_for_sql(
                "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) LIKE 'a%' ORDER BY id LIMIT 1",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN JSON DELETE FROM Customer WHERE STARTS_WITH(LOWER(name), 'a') ORDER BY id LIMIT 1",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN JSON DELETE FROM Customer WHERE LOWER(name) >= 'a' AND LOWER(name) < 'b' ORDER BY id LIMIT 1",
            ),
        ];

        for explain in explains {
            assert!(
                explain.contains("\"mode\":{\"type\":\"Delete\"")
                    && explain.contains("\"access\":{\"type\":\"IndexRange\""),
                "direct LOWER(field) equivalent delete prefix-form JSON explains should preserve the shared expression index-range route: {explain}",
            );
            assert!(
                !explain.contains("\"type\":\"FullScan\""),
                "direct LOWER(field) equivalent delete prefix-form JSON explains must not fall back to full scan: {explain}",
            );
        }
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_starts_with_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) STARTS_WITH parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_starts_with_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) STARTS_WITH EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_starts_with_explain_json_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN JSON SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) STARTS_WITH EXPLAIN JSON parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_strict_text_range_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) ordered text-range parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_strict_text_range_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) ordered text-range EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_strict_text_range_explain_json_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) ordered text-range EXPLAIN JSON parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_strict_text_range_explain_execution_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed(
            "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) ordered text-range EXPLAIN EXECUTION parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_equivalent_prefix_forms_match_explain_execution_route() {
        reload_default_fixtures();

        let explains = [
            dispatch_explain_for_sql(
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN EXECUTION SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            ),
        ];

        for explain in explains {
            assert!(
                explain.contains("IndexRangeScan")
                    && explain.contains("OrderByMaterializedSort")
                    && explain.contains("proj_fields=List([Text(\"id\"), Text(\"name\")])"),
                "direct UPPER(field) equivalent prefix-form explains should preserve the shared expression index-range materialized route: {explain}",
            );
            assert!(
                !explain.contains("FullScan"),
                "direct UPPER(field) equivalent prefix-form explains must not fall back to full scan: {explain}",
            );
        }
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_equivalent_prefix_forms_match_explain_json_route() {
        reload_default_fixtures();

        let explains = [
            dispatch_explain_for_sql(
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN JSON SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
            ),
        ];

        for explain in explains {
            assert!(
                explain.contains("\"mode\":{\"type\":\"Load\"")
                    && explain.contains("\"access\":{\"type\":\"IndexRange\""),
                "direct UPPER(field) equivalent prefix-form JSON explains should preserve the shared expression index-range route: {explain}",
            );
            assert!(
                !explain.contains("\"type\":\"FullScan\""),
                "direct UPPER(field) equivalent prefix-form JSON explains must not fall back to full scan: {explain}",
            );
        }
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_equivalent_prefix_forms_match_projection_rows() {
        reload_default_fixtures();

        let like = dispatch_result_for_sql(
            "SELECT id, name FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 2",
        );
        let starts_with = dispatch_result_for_sql(
            "SELECT id, name FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 2",
        );
        let range = dispatch_result_for_sql(
            "SELECT id, name FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 2",
        );

        assert_eq!(
            starts_with, like,
            "generated direct UPPER(field) STARTS_WITH and LIKE prefix queries should keep projection parity",
        );
        assert_eq!(
            range, like,
            "generated direct UPPER(field) ordered text-range and LIKE prefix queries should keep projection parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_strict_text_range_delete_matches_typed_surface() {
        assert_delete_dispatch_result_matches_typed(
            "DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) ordered text-range DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_strict_text_range_explain_delete_matches_typed_surface()
    {
        assert_dispatch_result_matches_typed(
            "EXPLAIN DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) ordered text-range EXPLAIN DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_strict_text_range_explain_json_delete_matches_typed_surface()
     {
        assert_dispatch_result_matches_typed(
            "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep direct UPPER(field) ordered text-range EXPLAIN JSON DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_direct_upper_delete_equivalent_prefix_forms_match_explain_json_route()
    {
        reload_default_fixtures();

        let explains = [
            dispatch_explain_for_sql(
                "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) LIKE 'A%' ORDER BY id LIMIT 1",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN JSON DELETE FROM Customer WHERE STARTS_WITH(UPPER(name), 'A') ORDER BY id LIMIT 1",
            ),
            dispatch_explain_for_sql(
                "EXPLAIN JSON DELETE FROM Customer WHERE UPPER(name) >= 'A' AND UPPER(name) < 'B' ORDER BY id LIMIT 1",
            ),
        ];

        for explain in explains {
            assert!(
                explain.contains("\"mode\":{\"type\":\"Delete\"")
                    && explain.contains("\"access\":{\"type\":\"IndexRange\""),
                "direct UPPER(field) equivalent delete prefix-form JSON explains should preserve the shared expression index-range route: {explain}",
            );
            assert!(
                !explain.contains("\"type\":\"FullScan\""),
                "direct UPPER(field) equivalent delete prefix-form JSON explains must not fall back to full scan: {explain}",
            );
        }
    }

    #[test]
    fn generated_sql_dispatch_non_casefold_wrapped_direct_starts_with_stays_fail_closed() {
        assert_dispatch_result_matches_typed(
            "SELECT id, name FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep non-casefold wrapped direct STARTS_WITH fail-closed parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_non_casefold_wrapped_direct_starts_with_explain_stays_fail_closed() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT id, name FROM Customer WHERE STARTS_WITH(TRIM(name), 'a') ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep non-casefold wrapped direct STARTS_WITH EXPLAIN fail-closed parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_computed_projection_explain_matches_typed_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN SELECT LOWER(name) FROM Customer ORDER BY id LIMIT 2",
            "typed execute_sql_dispatch and sql_dispatch should keep computed projection EXPLAIN parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_delete_matches_typed_delete_surface() {
        assert_delete_dispatch_result_matches_typed(
            "DELETE FROM Customer ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_explain_delete_matches_typed_explain_surface() {
        assert_dispatch_result_matches_typed(
            "EXPLAIN DELETE FROM Customer ORDER BY id LIMIT 1",
            "typed execute_sql_dispatch and sql_dispatch should keep EXPLAIN DELETE parity",
        );
    }

    #[test]
    fn generated_sql_dispatch_describe_matches_typed_describe_surface() {
        assert_dispatch_matches_typed(
            "DESCRIBE public.Customer",
            "typed execute_sql_dispatch and sql_dispatch should return identical DESCRIBE payloads",
        );
    }

    #[test]
    fn generated_sql_dispatch_show_indexes_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "SHOW INDEXES public.Customer",
            "typed execute_sql_dispatch and sql_dispatch should return identical SHOW INDEXES payloads",
        );
    }

    #[test]
    fn generated_sql_dispatch_show_columns_matches_typed_surface() {
        assert_dispatch_matches_typed(
            "SHOW COLUMNS public.Customer",
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
    fn generated_sql_dispatch_order_metadata_surfaces_encode_cleanly() {
        ensure_sql_test_memory_range();

        for sql in [
            "DESCRIBE CustomerOrder",
            "DESCRIBE public.CustomerOrder",
            "SHOW INDEXES CustomerOrder",
            "SHOW INDEXES public.CustomerOrder",
            "SHOW COLUMNS CustomerOrder",
            "SHOW COLUMNS public.CustomerOrder",
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
                "CustomerOrder metadata payload should survive canister-style Candid roundtrip for {sql}",
            );
        }
    }

    #[test]
    fn generated_sql_parity_order_fixtures_have_expected_count() {
        let rows = fixtures::customer_orders();

        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn generated_sql_parity_order_fixtures_keep_unique_names() {
        let rows = fixtures::customer_orders();
        let names: BTreeSet<String> = rows.iter().map(|row| row.name.clone()).collect();

        assert_eq!(names.len(), rows.len());
        assert!(names.contains("A-101"));
        assert!(names.contains("Z-900"));
    }

    #[test]
    fn generated_sql_parity_customer_fixtures_keep_expected_gold_handles() {
        let rows = fixtures::customer_accounts();
        let gold_handles: BTreeSet<String> = rows
            .iter()
            .filter(|row| row.active && row.tier == "gold")
            .map(|row| row.handle.clone())
            .collect();

        assert!(
            gold_handles == BTreeSet::from(["bravo".to_string(), "bristle".to_string()])
        );
    }

    #[test]
    fn customer_name_order_perf_surface_keeps_row_check_metrics_zero_in_parity() {
        let sql = "SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2";
        let generated = perf_sample(SqlPerfSurface::GeneratedDispatch, sql);
        let typed = perf_sample(SqlPerfSurface::TypedDispatchCustomer, sql);

        assert!(
            generated.outcome.success,
            "generated Customer name-order perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed Customer name-order perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(2),
            "generated Customer name-order perf sample should return the requested window",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(2),
            "typed Customer name-order perf sample should return the requested window",
        );

        let generated_metrics = generated
            .outcome
            .row_check_metrics
            .expect("generated Customer name-order perf sample should attach row_check metrics");
        let typed_metrics = typed
            .outcome
            .row_check_metrics
            .expect("typed Customer name-order perf sample should attach row_check metrics");

        assert_eq!(
            generated_metrics.row_check_covering_candidates_seen,
            0,
            "generated Customer name-order perf sample should not enter the row_check covering candidate lane on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count,
            0,
            "generated Customer name-order perf sample should not execute row-presence probes on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits,
            0,
            "generated Customer name-order perf sample should not execute row-presence probes on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses,
            0,
            "generated Customer name-order perf sample should not hit stale-row misses on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count,
            0,
            "generated Customer name-order perf sample should not route through the borrowed data-store row-check helper on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count,
            0,
            "generated Customer name-order perf sample should not bounce through the store-handle row-presence helper on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes,
            0,
            "generated Customer name-order perf sample should not encode row-check primary keys on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted,
            0,
            "generated Customer name-order perf sample should not report row_check-emitted rows on the witness-backed default fixture set",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed Customer name-order perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_name_order_stale_perf_surface_reports_row_check_metrics_in_parity() {
        let sql = "SELECT name FROM Customer ORDER BY name ASC LIMIT 2";
        reload_default_fixtures_with_customer_name_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("generated stale sql perf sample should succeed");
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomer,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("typed stale sql perf sample should succeed");

        assert!(
            generated.outcome.success,
            "generated stale Customer name-order perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale Customer name-order perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale Customer name-order perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale Customer name-order perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );

        let generated_metrics = generated
            .outcome
            .row_check_metrics
            .expect("generated stale Customer name-order perf sample should attach row_check metrics");
        let typed_metrics = typed
            .outcome
            .row_check_metrics
            .expect("typed stale Customer name-order perf sample should attach row_check metrics");

        assert_eq!(
            generated_metrics.row_check_covering_candidates_seen,
            2,
            "generated stale Customer name-order perf sample should inspect two secondary candidates before exhausting the requested window",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count,
            0,
            "generated stale Customer name-order perf sample should not execute borrowed row-presence probes once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits,
            0,
            "generated stale Customer name-order perf sample should not report borrowed row-presence hits once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses,
            0,
            "generated stale Customer name-order perf sample should not report borrowed row-presence misses once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count,
            0,
            "generated stale Customer name-order perf sample should no longer route stale-row checks through the borrowed data-store helper",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count,
            0,
            "generated stale Customer name-order perf sample should not bounce stale-row checks through the store-handle helper",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes,
            0,
            "generated stale Customer name-order perf sample should not encode authoritative row keys once the storage witness is attached to the index membership entry",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted,
            1,
            "generated stale Customer name-order perf sample should emit exactly one live row after stale-row filtering",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale Customer name-order perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_name_order_pk_projection_stale_perf_surface_reports_row_check_metrics_in_parity() {
        let sql = "SELECT id, name FROM Customer ORDER BY name ASC, id ASC LIMIT 2";
        reload_default_fixtures_with_customer_name_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("generated stale PK-plus-name sql perf sample should succeed");
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomer,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("typed stale PK-plus-name sql perf sample should succeed");

        assert!(
            generated.outcome.success,
            "generated stale Customer PK-plus-name order perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale Customer PK-plus-name order perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale Customer PK-plus-name order perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale Customer PK-plus-name order perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale Customer PK-plus-name order perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale Customer PK-plus-name order perf sample should attach row_check metrics",
        );

        assert_eq!(
            generated_metrics.row_check_covering_candidates_seen,
            2,
            "generated stale Customer PK-plus-name order perf sample should inspect two secondary candidates before exhausting the requested window",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count,
            0,
            "generated stale Customer PK-plus-name order perf sample should not execute borrowed row-presence probes once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits,
            0,
            "generated stale Customer PK-plus-name order perf sample should not report borrowed row-presence hits once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses,
            0,
            "generated stale Customer PK-plus-name order perf sample should not report borrowed row-presence misses once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count,
            0,
            "generated stale Customer PK-plus-name order perf sample should no longer route stale-row checks through the borrowed data-store helper",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count,
            0,
            "generated stale Customer PK-plus-name order perf sample should not bounce stale-row checks through the store-handle helper",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes,
            0,
            "generated stale Customer PK-plus-name order perf sample should not encode authoritative row keys once the storage witness is attached to the index membership entry",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted,
            1,
            "generated stale Customer PK-plus-name order perf sample should emit exactly one live row after stale-row filtering",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale Customer PK-plus-name order perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_order_order_only_composite_stale_perf_surface_reports_row_check_metrics_in_parity()
    {
        let sql =
            "SELECT id, priority, status FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2";
        reload_default_fixtures_with_customer_order_order_only_composite_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("generated stale CustomerOrder composite sql perf sample should succeed");
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomerOrder,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("typed stale CustomerOrder composite sql perf sample should succeed");

        assert!(
            generated.outcome.success,
            "generated stale CustomerOrder composite order-only perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale CustomerOrder composite order-only perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale CustomerOrder composite order-only perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale CustomerOrder composite order-only perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale CustomerOrder composite order-only perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale CustomerOrder composite order-only perf sample should attach row_check metrics",
        );

        assert_eq!(
            generated_metrics.row_check_covering_candidates_seen,
            2,
            "generated stale CustomerOrder composite order-only perf sample should inspect two secondary candidates before exhausting the requested window",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count,
            0,
            "generated stale CustomerOrder composite order-only perf sample should not execute borrowed row-presence probes once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits,
            0,
            "generated stale CustomerOrder composite order-only perf sample should not report borrowed row-presence hits once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses,
            0,
            "generated stale CustomerOrder composite order-only perf sample should not report borrowed row-presence misses once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count,
            0,
            "generated stale CustomerOrder composite order-only perf sample should no longer route stale-row checks through the borrowed data-store helper",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count,
            0,
            "generated stale CustomerOrder composite order-only perf sample should not bounce stale-row checks through the store-handle helper",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes,
            0,
            "generated stale CustomerOrder composite order-only perf sample should not encode authoritative row keys once the storage witness is attached to the index membership entry",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted,
            1,
            "generated stale CustomerOrder composite order-only perf sample should emit exactly one live row after stale-row filtering",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale CustomerOrder composite order-only perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_order_order_only_composite_leading_component_stale_perf_surface_reports_row_check_metrics_in_parity(
    ) {
        let sql =
            "SELECT id, priority FROM CustomerOrder ORDER BY priority ASC, status ASC, id ASC LIMIT 2";
        reload_default_fixtures_with_customer_order_order_only_composite_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "generated stale CustomerOrder composite leading-component sql perf sample should succeed",
        );
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomerOrder,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "typed stale CustomerOrder composite leading-component sql perf sample should succeed",
        );

        assert!(
            generated.outcome.success,
            "generated stale CustomerOrder composite leading-component perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale CustomerOrder composite leading-component perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale CustomerOrder composite leading-component perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale CustomerOrder composite leading-component perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale CustomerOrder composite leading-component perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale CustomerOrder composite leading-component perf sample should attach row_check metrics",
        );

        assert_eq!(
            generated_metrics.row_check_covering_candidates_seen,
            2,
            "generated stale CustomerOrder composite leading-component perf sample should inspect two secondary candidates before exhausting the requested window",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count,
            0,
            "generated stale CustomerOrder composite leading-component perf sample should not execute borrowed row-presence probes once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits,
            0,
            "generated stale CustomerOrder composite leading-component perf sample should not report borrowed row-presence hits once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses,
            0,
            "generated stale CustomerOrder composite leading-component perf sample should not report borrowed row-presence misses once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count,
            0,
            "generated stale CustomerOrder composite leading-component perf sample should no longer route stale-row checks through the borrowed data-store helper",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count,
            0,
            "generated stale CustomerOrder composite leading-component perf sample should not bounce stale-row checks through the store-handle helper",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes,
            0,
            "generated stale CustomerOrder composite leading-component perf sample should not encode authoritative row keys once the storage witness is attached to the index membership entry",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted,
            1,
            "generated stale CustomerOrder composite leading-component perf sample should emit exactly one live row after stale-row filtering",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale CustomerOrder composite leading-component perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_order_order_only_composite_leading_component_desc_stale_perf_surface_reports_row_check_metrics_in_parity(
    ) {
        let sql =
            "SELECT id, priority FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2";
        reload_default_fixtures_with_customer_order_order_only_composite_desc_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "generated stale descending CustomerOrder composite leading-component sql perf sample should succeed",
        );
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomerOrder,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "typed stale descending CustomerOrder composite leading-component sql perf sample should succeed",
        );

        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale descending CustomerOrder composite leading-component perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale descending CustomerOrder composite leading-component perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale descending CustomerOrder composite leading-component perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale descending CustomerOrder composite leading-component perf sample should attach row_check metrics",
        );

        assert_eq!(generated_metrics.row_check_covering_candidates_seen, 2);
        assert_eq!(generated_metrics.row_presence_probe_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_hits, 0);
        assert_eq!(generated_metrics.row_presence_probe_misses, 0);
        assert_eq!(generated_metrics.row_presence_probe_borrowed_data_store_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_store_handle_count, 0);
        assert_eq!(generated_metrics.row_presence_key_to_raw_encodes, 0);
        assert_eq!(generated_metrics.row_check_rows_emitted, 1);
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale descending CustomerOrder composite leading-component perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_order_order_only_composite_desc_stale_perf_surface_reports_row_check_metrics_in_parity(
    ) {
        let sql =
            "SELECT id, priority, status FROM CustomerOrder ORDER BY priority DESC, status DESC, id DESC LIMIT 2";
        reload_default_fixtures_with_customer_order_order_only_composite_desc_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("generated stale descending CustomerOrder composite sql perf sample should succeed");
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomerOrder,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("typed stale descending CustomerOrder composite sql perf sample should succeed");

        assert!(
            generated.outcome.success,
            "generated stale descending CustomerOrder composite order-only perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale descending CustomerOrder composite order-only perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale descending CustomerOrder composite order-only perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale descending CustomerOrder composite order-only perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale descending CustomerOrder composite order-only perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale descending CustomerOrder composite order-only perf sample should attach row_check metrics",
        );

        assert_eq!(
            generated_metrics.row_check_covering_candidates_seen,
            2,
            "generated stale descending CustomerOrder composite order-only perf sample should inspect two secondary candidates before exhausting the requested window",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_count,
            0,
            "generated stale descending CustomerOrder composite order-only perf sample should not execute borrowed row-presence probes once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_hits,
            0,
            "generated stale descending CustomerOrder composite order-only perf sample should not report borrowed row-presence hits once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_misses,
            0,
            "generated stale descending CustomerOrder composite order-only perf sample should not report borrowed row-presence misses once the storage witness is authoritative",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_borrowed_data_store_count,
            0,
            "generated stale descending CustomerOrder composite order-only perf sample should no longer route stale-row checks through the borrowed data-store helper",
        );
        assert_eq!(
            generated_metrics.row_presence_probe_store_handle_count,
            0,
            "generated stale descending CustomerOrder composite order-only perf sample should not bounce stale-row checks through the store-handle helper",
        );
        assert_eq!(
            generated_metrics.row_presence_key_to_raw_encodes,
            0,
            "generated stale descending CustomerOrder composite order-only perf sample should not encode authoritative row keys once the storage witness is attached to the index membership entry",
        );
        assert_eq!(
            generated_metrics.row_check_rows_emitted,
            1,
            "generated stale descending CustomerOrder composite order-only perf sample should emit exactly one live row after stale-row filtering",
        );
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale descending CustomerOrder composite order-only perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_order_numeric_equality_stale_perf_surface_reports_row_check_metrics_in_parity() {
        let sql =
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2";
        reload_default_fixtures_with_customer_order_numeric_equality_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("generated stale CustomerOrder numeric-equality sql perf sample should succeed");
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomerOrder,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect("typed stale CustomerOrder numeric-equality sql perf sample should succeed");

        assert!(
            generated.outcome.success,
            "generated stale CustomerOrder numeric-equality perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale CustomerOrder numeric-equality perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale CustomerOrder numeric-equality perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale CustomerOrder numeric-equality perf sample should consume scan budget on the missing leading row before emitting the first live row",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale CustomerOrder numeric-equality perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale CustomerOrder numeric-equality perf sample should attach row_check metrics",
        );

        assert_eq!(generated_metrics.row_check_covering_candidates_seen, 2);
        assert_eq!(generated_metrics.row_presence_probe_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_hits, 0);
        assert_eq!(generated_metrics.row_presence_probe_misses, 0);
        assert_eq!(generated_metrics.row_presence_probe_borrowed_data_store_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_store_handle_count, 0);
        assert_eq!(generated_metrics.row_presence_key_to_raw_encodes, 0);
        assert_eq!(generated_metrics.row_check_rows_emitted, 1);
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale CustomerOrder numeric-equality perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_order_numeric_equality_leading_component_stale_perf_surface_reports_row_check_metrics_in_parity(
    ) {
        let sql =
            "SELECT id, priority FROM CustomerOrder WHERE priority = 20 ORDER BY status ASC, id ASC LIMIT 2";
        reload_default_fixtures_with_customer_order_numeric_equality_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "generated stale CustomerOrder numeric-equality leading-component sql perf sample should succeed",
        );
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomerOrder,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "typed stale CustomerOrder numeric-equality leading-component sql perf sample should succeed",
        );

        assert!(
            generated.outcome.success,
            "generated stale CustomerOrder numeric-equality leading-component perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale CustomerOrder numeric-equality leading-component perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(1),
            "generated stale CustomerOrder numeric-equality leading-component perf sample should still emit one live row after the missing leading membership consumes part of the window",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(1),
            "typed stale CustomerOrder numeric-equality leading-component perf sample should still emit one live row after the missing leading membership consumes part of the window",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale CustomerOrder numeric-equality leading-component perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale CustomerOrder numeric-equality leading-component perf sample should attach row_check metrics",
        );

        assert_eq!(generated_metrics.row_check_covering_candidates_seen, 2);
        assert_eq!(generated_metrics.row_presence_probe_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_hits, 0);
        assert_eq!(generated_metrics.row_presence_probe_misses, 0);
        assert_eq!(generated_metrics.row_presence_probe_borrowed_data_store_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_store_handle_count, 0);
        assert_eq!(generated_metrics.row_presence_key_to_raw_encodes, 0);
        assert_eq!(generated_metrics.row_check_rows_emitted, 1);
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale CustomerOrder numeric-equality leading-component perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_order_numeric_equality_desc_stale_perf_surface_reports_row_check_metrics_in_parity()
    {
        let sql =
            "SELECT id, priority, status FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2";
        reload_default_fixtures_with_customer_order_numeric_equality_desc_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "generated stale descending CustomerOrder numeric-equality sql perf sample should succeed",
        );
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomerOrder,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "typed stale descending CustomerOrder numeric-equality sql perf sample should succeed",
        );

        assert!(
            generated.outcome.success,
            "generated stale descending CustomerOrder numeric-equality perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale descending CustomerOrder numeric-equality perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(2),
            "generated stale descending CustomerOrder numeric-equality perf sample should preserve the canonical two-row window after the missing descending-leading row is filtered out",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(2),
            "typed stale descending CustomerOrder numeric-equality perf sample should preserve the canonical two-row window after the missing descending-leading row is filtered out",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale descending CustomerOrder numeric-equality perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale descending CustomerOrder numeric-equality perf sample should attach row_check metrics",
        );

        assert_eq!(generated_metrics.row_check_covering_candidates_seen, 4);
        assert_eq!(generated_metrics.row_presence_probe_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_hits, 0);
        assert_eq!(generated_metrics.row_presence_probe_misses, 0);
        assert_eq!(generated_metrics.row_presence_probe_borrowed_data_store_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_store_handle_count, 0);
        assert_eq!(generated_metrics.row_presence_key_to_raw_encodes, 0);
        assert_eq!(generated_metrics.row_check_rows_emitted, 3);
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale descending CustomerOrder numeric-equality perf samples should keep row_check metrics in parity",
        );
    }

    #[test]
    fn customer_order_numeric_equality_leading_component_desc_stale_perf_surface_reports_row_check_metrics_in_parity(
    ) {
        let sql =
            "SELECT id, priority FROM CustomerOrder WHERE priority = 20 ORDER BY status DESC, id DESC LIMIT 2";
        reload_default_fixtures_with_customer_order_numeric_equality_desc_stale();
        let generated = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::GeneratedDispatch,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "generated stale descending CustomerOrder numeric-equality leading-component sql perf sample should succeed",
        );
        let typed = sample_sql_surface(SqlPerfRequest {
            surface: SqlPerfSurface::TypedDispatchCustomerOrder,
            sql: sql.to_string(),
            cursor_token: None,
            repeat_count: 1,
        })
        .expect(
            "typed stale descending CustomerOrder numeric-equality leading-component sql perf sample should succeed",
        );

        assert!(
            generated.outcome.success,
            "generated stale descending CustomerOrder numeric-equality leading-component perf sample should succeed: {generated:?}",
        );
        assert!(
            typed.outcome.success,
            "typed stale descending CustomerOrder numeric-equality leading-component perf sample should succeed: {typed:?}",
        );
        assert_eq!(
            generated.outcome.row_count,
            Some(2),
            "generated stale descending CustomerOrder numeric-equality leading-component perf sample should preserve the canonical two-row window after the missing descending-leading row is filtered out",
        );
        assert_eq!(
            typed.outcome.row_count,
            Some(2),
            "typed stale descending CustomerOrder numeric-equality leading-component perf sample should preserve the canonical two-row window after the missing descending-leading row is filtered out",
        );

        let generated_metrics = generated.outcome.row_check_metrics.expect(
            "generated stale descending CustomerOrder numeric-equality leading-component perf sample should attach row_check metrics",
        );
        let typed_metrics = typed.outcome.row_check_metrics.expect(
            "typed stale descending CustomerOrder numeric-equality leading-component perf sample should attach row_check metrics",
        );

        assert_eq!(generated_metrics.row_check_covering_candidates_seen, 4);
        assert_eq!(generated_metrics.row_presence_probe_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_hits, 0);
        assert_eq!(generated_metrics.row_presence_probe_misses, 0);
        assert_eq!(generated_metrics.row_presence_probe_borrowed_data_store_count, 0);
        assert_eq!(generated_metrics.row_presence_probe_store_handle_count, 0);
        assert_eq!(generated_metrics.row_presence_key_to_raw_encodes, 0);
        assert_eq!(generated_metrics.row_check_rows_emitted, 3);
        assert_eq!(
            generated_metrics, typed_metrics,
            "generated and typed stale descending CustomerOrder numeric-equality leading-component perf samples should keep row_check metrics in parity",
        );
    }

}
