use icydb::design::prelude::*;

#[canister(memory_namespace = "ui_test", memory_min = 100, memory_max = 110, commit_memory_id = 110)]
pub struct UiCanister {}

#[store(
    ident = "UI_DATA_STORE", store_name = "ui_data",
    canister = "UiCanister",
    storage(journaled(
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102,
        journal_memory_id = 109,
    ))
)]
pub struct UiDataStore {}

#[entity(source_key = "testing/macro-tests/tests/ui/generated_requires_ulid.rs::entity::1",
    store = "UiDataStore",
    version = 1,
    pk(fields = ["id"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "name", ident = "name",
            value(item(prim = "Text", unbounded)),
            generated(insert = "Ulid::generate")
        )
    )
)]
pub struct InvalidGeneratedField;

fn main() {}
