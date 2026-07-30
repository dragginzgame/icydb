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

#[newtype(
    primitive = "Nat64",
    item(prim = "Nat64"),
    typed_adapters,
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct X {}

#[newtype(
    primitive = "Nat64",
    item(prim = "Nat64"),
    typed_adapters,
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct XEntity {}

#[enum_(
    typed_adapters,
    variant(name = "Empty"),
    variant(name = "Count", value(item(is = "X"))),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct AdapterChoice {}

#[record(
    typed_adapters,
    fields(
        field(name = "label", value(item(prim = "Text", max_len = 64))),
        field(name = "choice", value(item(is = "AdapterChoice")))
    ),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct AdapterRecord {}

#[list(
    typed_adapters,
    item(is = "AdapterRecord"),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct AdapterList {}

#[set(
    typed_adapters,
    item(is = "X"),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct AdapterSet {}

#[map(
    typed_adapters,
    key(is = "X"),
    value(item(is = "AdapterChoice")),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct AdapterMap {}

#[tuple(
    typed_adapters,
    value(item(is = "XEntity")),
    value(opt, item(is = "AdapterRecord")),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct AdapterTuple {}

#[record(
    typed_adapters,
    fields(field(name = "next", value(opt, item(indirect, is = "RecursiveRecord")))),
    model_crate = "model_api",
    icydb_crate = "runtime_api"
)]
pub struct RecursiveRecord {}

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
        ),
        field(
            name = "profile",
            value(item(is = "AdapterRecord"))
        ),
        field(
            name = "list",
            value(item(is = "AdapterList"))
        ),
        field(
            name = "set",
            value(item(is = "AdapterSet"))
        ),
        field(
            name = "map",
            value(item(is = "AdapterMap"))
        ),
        field(
            name = "tuple",
            value(item(is = "AdapterTuple"))
        ),
        field(
            name = "recursive",
            value(opt, item(is = "RecursiveRecord"))
        )
    )
)]
pub struct TypedAdapterEntity {}
