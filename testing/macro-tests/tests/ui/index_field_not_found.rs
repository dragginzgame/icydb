use icydb::design::prelude::*;

#[canister(memory_min = 1, memory_max = 10, commit_memory_id = 10)]
pub struct UiCanister {}

#[store(
    ident = "UI_DATA_STORE",
    canister = "UiCanister",
    data_memory_id = 1,
    index_memory_id = 2
)]
pub struct UiDataStore {}

#[entity(
    store = "UiDataStore",
    pk(field = "id"),
    index(fields = "missing_field"),
    fields(field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"))
)]
pub struct InvalidIndexField;

fn main() {}
