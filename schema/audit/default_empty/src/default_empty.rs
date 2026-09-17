use icydb_model::prelude::*;
use icydb_testing_wasm_helpers::{define_fixture_canister, define_fixture_store};

define_fixture_canister!(
    DefaultEmptyCanister = "DefaultEmptyCanister",
    namespace = "default_empty",
);

define_fixture_store!(
    DefaultEmptyStore,
    canister = "DefaultEmptyCanister",
    storage(journaled(key = "main")),
);
