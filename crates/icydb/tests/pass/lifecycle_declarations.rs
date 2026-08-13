#![allow(clippy::missing_const_for_fn, dead_code)]

use candid::CandidType;
use serde::Deserialize;

mod __icydb_generated {
    pub(crate) fn __icydb_startup_init() {}

    pub(crate) fn __icydb_startup_post_upgrade() {}
}

#[derive(CandidType, Deserialize)]
struct InitArgs {
    seed: u64,
}

#[derive(CandidType, Deserialize)]
struct UpgradeArgs {
    resume: bool,
}

fn application_init(args: InitArgs) {
    std::hint::black_box(args.seed);
}

fn application_post_upgrade(args: UpgradeArgs) {
    std::hint::black_box(args.resume);
}

icydb::__icydb_start_lifecycle! {
    init(args: InitArgs) => application_init;
    post_upgrade(args: UpgradeArgs) => application_post_upgrade;
}

fn main() {}
