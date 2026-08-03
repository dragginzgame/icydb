mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn sql_query<const INTROSPECTION: bool>(
            _: String,
        ) -> Result<icydb::db::sql::SqlQueryPerfResult, icydb::Error> {
            let _ = INTROSPECTION;
            Ok(icydb::db::sql::SqlQueryPerfResult {
                result: icydb::db::sql::SqlQueryResult::Count {
                    entity: String::new(),
                    row_count: 0,
                },
                instructions: 0,
                planner_instructions: 0,
                store_instructions: 0,
                executor_instructions: 0,
                pure_covering_decode_instructions: 0,
                pure_covering_row_assembly_instructions: 0,
                decode_instructions: 0,
                compiler_instructions: 0,
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

fn main() {}
