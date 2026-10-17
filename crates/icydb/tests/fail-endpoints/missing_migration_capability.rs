use icydb as __icydb_facade;
use canic_icydb::__private::cdk as __icydb_host_sdk;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();
}

canic_icydb::endpoints! {
    icydb_schema_migrate;
}

fn main() {}
