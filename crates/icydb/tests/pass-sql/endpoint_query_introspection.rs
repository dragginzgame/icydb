#![allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps)]

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

    pub(crate) mod endpoint_authorization {
        pub(crate) fn require_sql_controller() -> Result<(), icydb::Error> {
            Ok(())
        }
    }
}

icydb::endpoints! {
    icydb_sql_query(introspection = true);
}

#[test]
fn sql_query_introspection_endpoint_compile_contract() {}
