#![expect(clippy::missing_const_for_fn, clippy::unnecessary_wraps)]

use canic_icydb::__private::cdk as __icydb_host_sdk;
use icydb as __icydb_facade;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn sql_query<const INTROSPECTION: bool>(
            _: String,
        ) -> Result<icydb::db::sql::SqlQueryResult, icydb::Error> {
            let _ = INTROSPECTION;
            Ok(icydb::db::sql::SqlQueryResult::Count {
                entity: String::new(),
                row_count: 0,
            })
        }
    }
}

canic_icydb::endpoints! {
    icydb_sql_query(introspection = true);
}

#[test]
fn sql_query_introspection_endpoint_compile_contract() {}
