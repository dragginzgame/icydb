#![allow(dead_code)]

mod __icydb_host {
    pub(crate) fn __icydb_startup_init() {}

    pub(crate) fn __icydb_startup_post_upgrade() {}
}

canic_icydb::__icydb_start_participant_lifecycle!();
canic_icydb::__icydb_start_participant_lifecycle!();

fn main() {}
