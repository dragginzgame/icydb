#[model_api::canister(memory_namespace = "actor_hygiene")]
pub struct ActorHygieneCanister {}

#[model_api::store(canister = "ActorHygieneCanister", storage(heap()))]
pub struct HeapStore {}

#[model_api::store(
    canister = "ActorHygieneCanister",
    storage(journaled(key = "journaled"))
)]
pub struct JournaledStore {}

#[model_api::entity(
    store = "HeapStore",
    version = 1,
    pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))))
)]
pub struct ActorHygieneEntity {}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    runtime_api::build::build_canister!(ActorHygieneCanister)
}
