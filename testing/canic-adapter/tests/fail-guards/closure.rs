include!("common.rs");

use icydb as __icydb_facade;
use canic_icydb::__private::cdk as __icydb_host_sdk;

canic_icydb::mount! {
    icydb_sql_query(
        introspection = true,
        authorization = guard(|_| icydb::ReadAuthorizationDecision::Allow),
    );
}

fn main() {}
