use icydb_model::prelude::*;
#[canister(memory_namespace = "names")]
pub struct NameCanister {}
#[store(canister = "NameCanister", storage(heap()))]
pub struct NameStore {}
#[entity(store = "NameStore", version = 1, pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))),
           field(name = "email", value(item(prim = "Text", max_len = 32)))),
    index(name = "first", field = "email"),
    index(name = "second", field = "email")
)]
pub struct DuplicateDefinition {}
#[entity(store = "NameStore", version = 1, pk(field = "id"),
    fields(field(name = "id", value(item(prim = "Nat64"))),
           field(name = "email", value(item(prim = "Text", max_len = 32)))),
    index(field = "email"),
    index(field = "email")
)]
pub struct AutomaticDuplicate {}

fn main() {}
