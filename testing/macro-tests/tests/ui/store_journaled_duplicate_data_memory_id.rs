use icydb::design::prelude::*;

#[canister(memory_namespace = "ui_test", memory_min = 100, memory_max = 110, commit_memory_id = 110)]
pub struct UiCanister {}

#[store(
    ident = "UI_DATA_STORE",
    store_name = "ui_data",
    canister = "UiCanister",
    storage(journaled(
        data_memory_id = 100,
        data_memory_id = 101,
        index_memory_id = 102,
        schema_memory_id = 103,
        journal_memory_id = 109,
    ))
)]
pub struct UiDataStore {}

fn main() {}
