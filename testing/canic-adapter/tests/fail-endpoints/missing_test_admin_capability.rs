#![allow(unexpected_cfgs, reason = "the capability-error fixture varies downstream check-cfg declarations; unexpected cfgs are not the tested failure")]

use icydb as __icydb_facade;
use canic_icydb::__private::cdk as __icydb_host_sdk;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();
}

canic_icydb::mount! {
    icydb_fixtures_reset;
}

fn main() {}
