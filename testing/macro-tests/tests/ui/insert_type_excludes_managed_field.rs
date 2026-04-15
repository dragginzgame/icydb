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
        field(ident = "name", value(item(prim = "Text")))
    )
)]
pub struct UiEntity {}

fn main() {
    let _ = icydb::Create::<UiEntity> {
        name: Some("Ada".to_string()),
        created_at: Some(Timestamp::now()),
        id: Some(1),
    };
}
