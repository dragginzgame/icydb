//!
//! Test-only SQL canister used by local and integration test harnesses.
//!

mod seed;

#[cfg(feature = "sql")]
use ic_cdk::query as ic_query;
use ic_cdk::{export_candid, update};
#[cfg(feature = "sql")]
use icydb::db::sql::SqlQueryResult;
use icydb_testing_quickstart_fixtures::schema::{Character, Order, User};

icydb::start!();

/// Return one list of fixture entity names accepted by the SQL endpoints.
#[cfg(feature = "sql")]
#[ic_query]
fn sql_entities() -> Vec<String> {
    sql_dispatch::entities()
}

/// Execute one reduced SQL statement against fixture entities.
#[cfg(feature = "sql")]
#[ic_query]
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

    fn dispatch_result_for_sql(sql: &str) -> SqlQueryResult {
        sql_dispatch::query(sql).expect("sql_dispatch query should succeed")
    }

    fn dispatch_result_for_sql_unchecked(sql: &str) -> Result<SqlQueryResult, icydb::Error> {
        sql_dispatch::query(sql)
    }

    fn typed_result_for_sql(sql: &str) -> SqlQueryResult {
        db().execute_sql_dispatch::<User>(sql)
            .expect("typed execute_sql_dispatch should succeed")
    }

    fn typed_result_for_sql_unchecked(sql: &str) -> Result<SqlQueryResult, icydb::Error> {
        db().execute_sql_dispatch::<User>(sql)
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
        let actor = include_str!(concat!(env!("OUT_DIR"), "/actor.rs"));

        assert!(
            actor.contains("pub mod sql_dispatch"),
            "generated actor surface must include sql_dispatch module"
        );
        assert!(
            actor.contains("from_statement_route"),
            "generated sql_dispatch must include from_statement_route resolver"
        );
        assert!(
            !actor.contains("from_statement_sql"),
            "generated sql_dispatch must not include removed from_statement_sql resolver"
        );
        assert!(
            actor.contains("from_entity_name"),
            "generated sql_dispatch must include from_entity_name resolver"
        );
        assert!(
            actor.contains("pub struct SqlLaneTable"),
            "generated sql_dispatch must include one SqlLaneTable function-pointer descriptor"
        );
        assert!(
            actor.contains("pub struct SqlEntityDescriptor"),
            "generated sql_dispatch must include one SqlEntityDescriptor runtime descriptor"
        );
        assert!(
            actor.contains("SQL_ENTITY_DESCRIPTORS"),
            "generated sql_dispatch must include one static descriptor table"
        );
        assert!(
            !actor.contains("enum SqlEntityRoute"),
            "generated sql_dispatch must not regress to enum-based per-entity routing"
        );
        assert!(
            actor.contains("pub fn query ("),
            "generated sql_dispatch must include query convenience entrypoint"
        );
        assert!(
            !actor.contains("pub fn query_rows ("),
            "generated sql_dispatch must not include removed query_rows convenience entrypoint"
        );
        assert!(
            !actor.contains("pub fn describe_schema ("),
            "generated sql_dispatch must not include removed describe_schema helper"
        );
        assert!(
            !actor.contains("pub fn describe ("),
            "generated sql_dispatch must not include removed describe helper"
        );
        assert!(
            !actor.contains("pub fn show_indexes ("),
            "generated sql_dispatch must not include removed show_indexes helper"
        );
    }

    #[test]
    fn generated_sql_dispatch_explain_text_matches_typed_explain_surface() {
        let sql = "EXPLAIN SELECT id, name FROM User WHERE name = 'alice' ORDER BY id LIMIT 5";
        let typed_explain_payload = db()
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

        let typed_query = db()
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
        let payload = db()
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
        let dispatch = dispatch_result_for_sql_unchecked(sql);
        let typed = typed_result_for_sql_unchecked(sql);

        match (dispatch, typed) {
            (Ok(dispatch), Ok(typed)) => {
                assert_eq!(
                    dispatch, typed,
                    "typed execute_sql_dispatch and sql_dispatch should return identical projection payloads",
                );
            }
            (Err(dispatch_err), Err(typed_err)) => {
                assert_eq!(
                    dispatch_err.kind(),
                    typed_err.kind(),
                    "typed execute_sql_dispatch and sql_dispatch should fail with the same error kind for projection SQL",
                );
                assert_eq!(
                    dispatch_err.origin(),
                    typed_err.origin(),
                    "typed execute_sql_dispatch and sql_dispatch should fail with the same error origin for projection SQL",
                );
            }
            (dispatch, typed) => {
                panic!(
                    "typed execute_sql_dispatch and sql_dispatch projection outcomes diverged: dispatch={dispatch:?} typed={typed:?}"
                );
            }
        }
    }

    #[test]
    fn generated_sql_dispatch_describe_matches_typed_describe_surface() {
        let sql = "DESCRIBE public.User";
        let dispatch = dispatch_result_for_sql(sql);
        let typed = typed_result_for_sql(sql);

        assert_eq!(
            dispatch, typed,
            "typed execute_sql_dispatch and sql_dispatch should return identical DESCRIBE payloads",
        );
    }

    #[test]
    fn generated_sql_dispatch_show_indexes_matches_typed_surface() {
        let sql = "SHOW INDEXES public.User";
        let dispatch = dispatch_result_for_sql(sql);
        let typed = typed_result_for_sql(sql);

        assert_eq!(
            dispatch, typed,
            "typed execute_sql_dispatch and sql_dispatch should return identical SHOW INDEXES payloads",
        );
    }

    #[test]
    fn generated_sql_dispatch_show_columns_matches_typed_surface() {
        let sql = "SHOW COLUMNS public.User";
        let dispatch = dispatch_result_for_sql(sql);
        let typed = typed_result_for_sql(sql);

        assert_eq!(
            dispatch, typed,
            "typed execute_sql_dispatch and sql_dispatch should return identical SHOW COLUMNS payloads",
        );
    }
}

export_candid!();
