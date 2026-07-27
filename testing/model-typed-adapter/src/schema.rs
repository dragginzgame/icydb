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
    ident = "TYPED_ADAPTER_STORE",
    store_name = "typed_adapter",
    canister = "TypedAdapterCanister",
    storage(heap()),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct TypedAdapterStore {}

#[entity(
    source_key = "fixture/model-typed-adapter/entity",
    store = "TypedAdapterStore",
    version = 1,
    pk(fields = ["id"]),
    typed_adapters,
    model_crate = "model_api",
    icydb_crate = "runtime_api",
    fields(
        field(
            source_key = "id",
            ident = "id",
            value(item(prim = "Nat64"))
        ),
        field(
            source_key = "name",
            ident = "name",
            value(item(prim = "Text", max_len = 64))
        ),
        field(
            source_key = "nickname",
            ident = "nickname",
            value(opt, item(prim = "Text", max_len = 64))
        )
    )
)]
pub struct TypedAdapterEntity {}
