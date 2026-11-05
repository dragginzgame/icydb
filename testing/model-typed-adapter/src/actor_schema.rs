#[model_api::canister(memory_namespace = "actor_hygiene")]
pub struct ActorHygieneCanister {}

#[model_api::store(canister = "ActorHygieneCanister", storage(heap()))]
pub struct HeapStore {}

#[model_api::store(
    canister = "ActorHygieneCanister",
    storage(journaled(key = "journaled"))
)]
pub struct JournaledStore {}

#[runtime_api::model::entity(
    store = "HeapStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(
            name = "id",
            value(item(prim = "Nat64")),
            generated(insert = "Identity::next")
        ),
        field(name = "label", value(item(prim = "Text", max_len = 32))),
        field(name = "nickname", value(opt, item(prim = "Text", max_len = 32)))
    )
)]
pub struct ActorHygieneEntity {}

#[runtime_api::model::entity(
    store = "HeapStore",
    version = 1,
    pk(fields = ["tenant", "local"]),
    fields(
        field(name = "tenant", value(item(prim = "Nat64"))),
        field(name = "local", value(item(prim = "Nat32"))),
        field(name = "label", value(opt, item(prim = "Text", max_len = 32)))
    )
)]
pub struct CompositeRow {}
