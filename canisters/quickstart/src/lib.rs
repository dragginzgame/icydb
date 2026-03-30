//!
//! Test-only SQL canister used by local and integration test harnesses.
//!

mod seed;

#[cfg(debug_assertions)]
use canic::export_candid;
#[cfg(feature = "sql")]
use ic_cdk::query;
use ic_cdk::update;
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
use icydb_testing_quickstart_fixtures::schema::{Character, Order, User};

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

/// Clear all fixture rows from this canister.
#[update]
fn fixtures_reset() -> Result<(), icydb::Error> {
    db().delete::<Order>().execute()?;
    db().delete::<Character>().execute()?;
    db().delete::<User>().execute()?;

    Ok(())
}

/// Load one deterministic baseline fixture dataset.
#[update]
fn fixtures_load_default() -> Result<(), icydb::Error> {
    fixtures_reset()?;

    db().insert_many_atomic(seed::base::users())?;
    db().insert_many_atomic(seed::base::orders())?;
    db().insert_many_atomic(seed::rpg::characters())?;

    Ok(())
}

///
/// TESTS
///

#[cfg(all(test, feature = "sql"))]
mod tests {
    use super::{SqlQueryResult, User, db, sql_dispatch};
    use candid::encode_one;
    use icydb::error::{ErrorKind, RuntimeErrorKind};
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
        ::canic::ic_memory_range!(QUICKSTART_MEMORY_MIN, QUICKSTART_MEMORY_MAX);
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

    fn typed_result_for_sql(sql: &str) -> SqlQueryResult {
        test_db()
            .execute_sql_dispatch::<User>(sql)
            .expect("typed execute_sql_dispatch should succeed")
    }

    fn typed_result_for_sql_unchecked(sql: &str) -> Result<SqlQueryResult, icydb::Error> {
        test_db().execute_sql_dispatch::<User>(sql)
    }

    // Compare one sql_dispatch lane payload against the typed `execute_sql_dispatch` path.
    fn assert_dispatch_matches_typed(sql: &str, context: &str) {
        let dispatch = dispatch_result_for_sql(sql);
        let typed = typed_result_for_sql(sql);

        assert_eq!(dispatch, typed, "{context}");
    }

    // Compare one fallible projection SQL path across dispatch and typed execution.
    fn assert_dispatch_result_matches_typed(sql: &str, context: &str) {
        let dispatch = dispatch_result_for_sql_unchecked(sql);
        let typed = typed_result_for_sql_unchecked(sql);

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
    fn generated_sql_dispatch_query_rejects_delete_lane() {
        let err = dispatch_result_for_sql_unchecked("DELETE FROM User ORDER BY id LIMIT 1")
            .expect_err("query lane should reject DELETE");

        assert_eq!(
            err.kind(),
            &ErrorKind::Runtime(RuntimeErrorKind::Unsupported)
        );
    }

    #[test]
    fn generated_sql_dispatch_query_rejects_explain_delete_lane() {
        let err = dispatch_result_for_sql_unchecked("EXPLAIN DELETE FROM User ORDER BY id LIMIT 1")
            .expect_err("query lane should reject EXPLAIN DELETE");

        assert_eq!(
            err.kind(),
            &ErrorKind::Runtime(RuntimeErrorKind::Unsupported)
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

#[cfg(debug_assertions)]
export_candid!();
