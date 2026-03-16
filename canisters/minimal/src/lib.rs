//!
//! Minimal SQL canister used for wasm-footprint auditing.
//!

use ic_cdk::{export_candid, query as ic_query};
use icydb::db::sql::SqlQueryResult;

icydb::start!();

/// Execute one reduced SQL statement against the minimal entity set.
#[ic_query]
fn query(sql: String) -> Result<SqlQueryResult, icydb::Error> {
    sql_dispatch::query(sql.as_str())
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
            actor.contains("pub fn query ("),
            "generated sql_dispatch must include query convenience entrypoint"
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
