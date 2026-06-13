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

#[entity(
    store = "UiDataStore",
    version = 1,
    pk(fields = ["tenant_id", "singleton"]),
    fields(
        field(ident = "tenant_id", value(item(prim = "Nat64")), default = 1u64),
        field(ident = "singleton", value(item(prim = "Unit"))),
    )
)]
pub struct UnitCompositePkField;

fn main() {}
