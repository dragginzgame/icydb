use icydb::design::prelude::*;

#[canister(memory_min = 1, memory_max = 10, commit_memory_id = 10)]
pub struct UiCanister {}

#[store(
    ident = "UI_DATA_STORE",
    canister = "UiCanister",
    data_memory_id = 1,
    index_memory_id = 2,
    schema_memory_id = 3
)]
pub struct UiDataStore {}

#[entity(
    store = "UiDataStore",
    pk(field = "id"),
    index(fields = "tags"),
    fields(
        field(ident = "id", value(item(prim = "Ulid")), default = "Ulid::generate"),
        field(ident = "tags", value(many, item(prim = "Text")))
    )
)]
pub struct InvalidManyIndexField;

fn main() {}
