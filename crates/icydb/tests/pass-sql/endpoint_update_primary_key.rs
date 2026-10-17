use canic_icydb::__private::cdk as __icydb_host_sdk;
use icydb as __icydb_facade;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn sql_update_primary_key(
            _: String,
        ) -> Result<icydb::db::sql::SqlQueryResult, icydb::Error> {
            unreachable!()
        }
    }
}

canic_icydb::endpoints! {
    icydb_update(admission = primary_key_only);
}

#[test]
fn source_declared_primary_key_update_policy_selects_exact_handler() {}
