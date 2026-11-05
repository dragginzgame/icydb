#![allow(dead_code, reason = "the expected macro failure can prevent dead-code linting, so an expectation is not reliable")]

mod __icydb_host {
    pub(crate) fn __icydb_startup_init() {}

    pub(crate) fn __icydb_startup_post_upgrade() {}
}

canic_icydb::__icydb_start_participant_lifecycle!();
canic_icydb::__icydb_start_participant_lifecycle!();

fn main() {}
