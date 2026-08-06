#![allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps)]

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_authorization {
        pub(crate) fn require_sql_controller() -> Result<(), icydb::Error> {
            Ok(())
        }
    }

    pub(crate) mod endpoint_handlers {
        pub(crate) fn sql_update_bounded(
            _: String,
        ) -> Result<icydb::db::sql::SqlQueryResult, icydb::Error> {
            unreachable!()
        }
    }
}

icydb::endpoints! {
    icydb_update(admission = bounded_deterministic);
}

#[test]
fn source_declared_bounded_update_policy_selects_exact_handler() {}
