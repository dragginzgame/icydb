use icydb::design::prelude::*;

#[canister(memory_namespace = "ui_test", memory_min = 100, memory_max = 110, commit_memory_id = 110)]
pub struct UiCanister {}

#[store(
    ident = "UI_DATA_STORE",
    store_name = "ui_data",
    canister = "UiCanister",
    storage(journaled(journal_memory_id = 103))
)]
pub struct UiDataStore {}

fn main() {}
