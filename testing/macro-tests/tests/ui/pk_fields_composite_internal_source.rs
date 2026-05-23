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
    pk(fields = ["tenant_id", "local_id"], source = "internal"),
    fields(
        field(ident = "tenant_id", value(item(prim = "Nat64")), default = 1u64),
        field(ident = "local_id", value(item(prim = "Nat64")), default = 2u64),
    )
)]
pub struct CompositeInternalSourcePk;

fn main() {}
