#![allow(dead_code)]

mod __icydb_generated {
    pub(crate) fn __icydb_startup_init() {}

    pub(crate) fn __icydb_startup_post_upgrade() {}
}

mod __icydb_lifecycle_participant {}

icydb::__icydb_start_participant_lifecycle!();

fn main() {}
