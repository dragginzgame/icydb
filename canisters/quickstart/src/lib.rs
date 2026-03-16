//!
//! Test-only SQL canister used by local and integration test harnesses.
//!

mod seed;

use ic_cdk::{export_candid, query as ic_query, update};
use icydb::db::sql::SqlQueryResult;
use icydb_testing_sql_test_fixtures::schema::{Character, Order, User};

icydb::start!();

/// Return one list of fixture entity names accepted by the SQL endpoints.
#[ic_query]
fn sql_entities() -> Vec<String> {
    sql_dispatch::entities()
}

/// Execute one reduced SQL statement against fixture entities.
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

#[cfg(test)]
mod tests {
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
}

export_candid!();
