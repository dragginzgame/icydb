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
    fields(
        field(ident = "id", value(item(prim = "Nat64"))),
        field(
            ident = "created_on_insert",
            value(item(prim = "Timestamp")),
            default = "Timestamp::EPOCH",
            generated(insert = "Timestamp::now")
        )
    )
)]
pub struct InvalidGeneratedDefaultConflictField;

fn main() {}
