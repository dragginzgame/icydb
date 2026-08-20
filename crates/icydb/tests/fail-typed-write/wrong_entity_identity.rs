use icydb::{db::WriteCell, types::Id};
use icydb_model::prelude::*;

#[canister(
    memory_namespace = "typed_identity_wrong_entity",
    memory_min = 230,
    memory_max = 232,
    commit_memory_id = 232,
    startup_memory_id = 231
)]
pub struct TypedIdentityCanister {}

#[store(canister = "TypedIdentityCanister", storage(heap()))]
pub struct TypedIdentityStore {}

#[entity(
    store = "TypedIdentityStore",
    version = 1,
    pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Ulid"))))
)]
pub struct User {}

#[entity(
    store = "TypedIdentityStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(
            name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        )
    )
)]
pub struct Robot {}

fn main() {
    let robot_id = Id::<Robot>::from_key(icydb::types::Ulid::nil());
    let _ = UserInsert {
        id: WriteCell::Value(robot_id),
    };
}
