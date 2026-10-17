#![expect(clippy::missing_const_for_fn, dead_code)]

use canic_icydb::__private::cdk as __icydb_host_sdk;
use icydb as __icydb_facade;
use serde::Deserialize;

mod __icydb_host {
    pub(crate) fn __icydb_startup_init() {}

    pub(crate) fn __icydb_startup_post_upgrade() {}
}

#[derive(Deserialize, serde::Serialize)]
struct InitArgs {
    seed: u64,
}

#[derive(Deserialize, serde::Serialize)]
struct UpgradeArgs {
    resume: bool,
}

fn application_init(args: InitArgs) {
    std::hint::black_box(args.seed);
}

fn application_post_upgrade(args: UpgradeArgs) {
    std::hint::black_box(args.resume);
}

canic_icydb::__icydb_start_lifecycle! {
    init(args: InitArgs) => application_init;
    post_upgrade(args: UpgradeArgs) => application_post_upgrade;
}

fn main() {}
