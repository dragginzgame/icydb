use model_api::prelude::*;

#[canister(
    memory_namespace = "model_typed_adapter",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 110,
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct TypedAdapterCanister {}

#[store(
    canister = "TypedAdapterCanister",
    storage(heap()),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct TypedAdapterStore {}

#[entity(
    store = "TypedAdapterStore",
    version = 1,
    pk(fields = ["id"]),
    typed_adapters,
    model_crate = "model_api",
    icydb_crate = "runtime_api",
    fields(
        field(
            name = "id",
            value(item(prim = "Nat64")),
            generated(insert = "Identity::next")
        ),
        field(
            name = "name",
            value(item(prim = "Text", max_len = 64))
        ),
        field(
            name = "nickname",
            value(opt, item(prim = "Text", max_len = 64))
        )
    )
)]
pub struct TypedAdapterEntity {}
