use icydb as __icydb_facade;
use canic_icydb::__private::cdk as __icydb_host_sdk;

canic_icydb::mount! {
    icydb_schema(authorization = public);
}

fn main() {}
