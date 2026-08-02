mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_authorization {
        pub(crate) fn require_sql_controller() -> Result<(), icydb::Error> {
            Ok(())
        }
    }

    pub(crate) mod endpoint_handlers {
        pub(crate) fn sql_update_primary_key(
            _: String,
        ) -> Result<icydb::db::sql::SqlQueryResult, icydb::Error> {
            unreachable!()
        }
    }
}

icydb::endpoints! {
    icydb_update(admission = primary_key_only);
}

fn main() {}
