use icydb::design::prelude::*;

#[canister(memory_namespace = "ui_test", memory_min = 100, memory_max = 110, commit_memory_id = 110)]
pub struct UiCanister {}

#[store(
    ident = "UI_DATA_STORE", store_name = "ui_data",
    canister = "UiCanister",
    data_memory_id = 100,
    index_memory_id = 101,
    schema_memory_id = 102
)]
pub struct UiDataStore {}

#[entity(
    store = "UiDataStore",
    pk(fields = ["id"]),
    index(fields = ["tags"]),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), generated(insert = "Ulid::generate")),
        field(ident = "tags", value(many, item(prim = "Text", unbounded)))
    )
)]
pub struct InvalidManyIndexField;

fn main() {}
