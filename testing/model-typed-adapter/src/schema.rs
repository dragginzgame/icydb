use model_api::prelude::*;

#[canister(
    memory_namespace = "model_typed_adapter",
    memory_min = 100,
    memory_max = 110,
    commit_memory_id = 110,
    startup_memory_id = 109
)]
pub struct TypedAdapterCanister {}

#[store(canister = "TypedAdapterCanister", storage(heap()))]
pub struct TypedAdapterStore {}

#[newtype(item(prim = "Nat64"))]
pub struct X {}

#[newtype(item(prim = "Nat64"))]
pub struct XEntity {}

#[enum_(
    variant(name = "Empty"),
    variant(name = "Count", value(item(is = "X")))
)]
pub struct AdapterChoice {}

#[record(fields(
    field(name = "label", value(item(prim = "Text", max_len = 64))),
    field(name = "choice", value(item(is = "AdapterChoice")))
))]
pub struct AdapterRecord {}

#[list(item(is = "AdapterRecord"))]
pub struct AdapterList {}

#[set(item(is = "X"))]
pub struct AdapterSet {}

#[map(key(is = "X"), value(item(is = "AdapterChoice")))]
pub struct AdapterMap {}

#[tuple(value(item(is = "XEntity")), value(opt, item(is = "AdapterRecord")))]
pub struct AdapterTuple {}

#[record(fields(field(name = "next", value(opt, item(indirect, is = "RecursiveRecord")))))]
pub struct RecursiveRecord {}

#[entity(
    store = "TypedAdapterStore",
    version = 1,
    pk(fields = ["id"]),
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

#[entity(
    store = "TypedAdapterStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "label", value(item(prim = "Text", max_len = 64)))
    )
)]
pub struct TypedIdentityUser {}

#[entity(
    store = "TypedAdapterStore",
    version = 1,
    pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))))
)]
pub struct TypedIdentityCounter {}

#[entity(
    store = "TypedAdapterStore",
    version = 1,
    pk(fields = ["tenant_id", "local_id"]),
    fields(
        field(name = "tenant_id", value(item(prim = "Nat64"))),
        field(name = "local_id", value(item(prim = "Nat32")))
    )
)]
pub struct TypedCompositeTarget {}

#[entity(
    store = "TypedAdapterStore",
    version = 1,
    pk(field = "id"),
    relation(
        name = "composite_target",
        rel = "TypedCompositeTarget",
        fields = ["tenant_id", "local_id"]
    ),
    fields(
        field(
            name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(
            name = "user_id",
            value(item(rel = "TypedIdentityUser", prim = "Ulid"))
        ),
        field(
            name = "optional_user_id",
            value(opt, item(rel = "TypedIdentityUser", prim = "Ulid"))
        ),
        field(
            name = "user_ids",
            value(many, item(rel = "TypedIdentityUser", prim = "Ulid"))
        ),
        field(
            name = "counter_id",
            value(item(rel = "TypedIdentityCounter", prim = "Nat64"))
        ),
        field(name = "correlation_ulid", value(item(prim = "Ulid"))),
        field(name = "tenant_id", value(item(prim = "Nat64"))),
        field(name = "local_id", value(item(prim = "Nat32")))
    )
)]
pub struct TypedIdentityOwner {}
