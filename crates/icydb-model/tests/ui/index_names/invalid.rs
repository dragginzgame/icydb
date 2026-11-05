use icydb_model::prelude::*;
#[canister(memory_namespace = "names")]
pub struct NameCanister {}
#[store(canister = "NameCanister", storage(heap()))]
pub struct NameStore {}
#[entity(store = "NameStore", version = 1, pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))),
           field(name = "email", value(item(prim = "Text", max_len = 32)))),
    index(name = "bad name", field = "email")
)]
pub struct InvalidName {}
#[entity(store = "NameStore", version = 1, pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))),
           field(name = "email", value(item(prim = "Text", max_len = 32)))),
    index(name = "__icydb_private", field = "email")
)]
pub struct ReservedName {}
#[entity(store = "NameStore", version = 1, pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))),
           field(name = "email", value(item(prim = "Text", max_len = 32)))),
    index(name = "one", name = "two", field = "email")
)]
pub struct RepeatedName {}
fn main() {}
