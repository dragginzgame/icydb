use icydb::{db::WriteCell, types::Ulid};
use icydb_model::prelude::*;

#[canister(
    memory_namespace = "typed_identity_raw_ulid",
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

fn main() {
    let _ = UserInsert {
        id: WriteCell::Value(Ulid::nil()),
    };
}
