//! Downstream compile contract with one direct IcyDB package dependency.

use icydb::model::prelude::*;
use runtime_api as icydb;

#[canister(
    memory_namespace = "facade_only",
    memory_min = 220,
    memory_max = 224,
    commit_memory_id = 224,
    startup_memory_id = 223
)]
pub struct FacadeCanister {}

#[store(canister = "FacadeCanister", storage(heap()))]
pub struct FacadeStore {}

#[record(fields(
    field(name = "rank", value(item(prim = "Nat64"))),
    field(name = "label", value(item(prim = "Text", max_len = 64)))
))]
pub struct FacadeProfile {}

#[entity(
    store = "FacadeStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "profile", value(item(is = "FacadeProfile")))
    )
)]
pub struct FacadePlayer {}

#[cfg(test)]
mod tests {
    use runtime_api::model::{TypedInputValue, TypedNamedType, TypedOutputValue};

    use super::{FacadePlayer, FacadeProfile};

    fn assert_named_value<T: TypedInputValue + TypedNamedType + TypedOutputValue>() {}

    #[test]
    fn facade_owns_model_authoring_and_generated_runtime_paths() {
        assert_named_value::<FacadeProfile>();
        let _ = FacadePlayer::PROFILE;
    }
}
