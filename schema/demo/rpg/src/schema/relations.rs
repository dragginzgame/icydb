use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

define_fixture_canister!(DemoRpgCanister = "DemoRpgCanister", namespace = "demo_rpg",);

define_fixture_store!(
    DemoRpgStore,
    canister = "DemoRpgCanister",
    storage(journaled(key = "main")),
);
