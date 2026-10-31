use canic_icydb::__private::cdk as __icydb_host_sdk;
use icydb as __icydb_facade;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn sql_update_bounded(
            _: String,
        ) -> Result<icydb::db::sql::SqlQueryResult, icydb::Error> {
            unreachable!()
        }
    }
}

canic_icydb::mount! {
    icydb_update(admission = bounded_deterministic);
}

#[test]
fn source_declared_bounded_update_policy_selects_exact_handler() {}
