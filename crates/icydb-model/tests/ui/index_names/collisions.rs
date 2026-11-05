use icydb_model::prelude::*;

#[canister(memory_namespace = "index_names")]
pub struct IndexNamesCanister {}

#[store(canister = "IndexNamesCanister", storage(heap()))]
pub struct IndexNamesStore {}

#[entity(
    store = "IndexNamesStore", version = 1, pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "a_b", value(item(prim = "Nat64"))),
        field(name = "a", value(item(prim = "Nat64"))),
        field(name = "b", value(item(prim = "Nat64")))
    ),
    index(name = "chosen_name", field = "a_b"),
    index(name = "chosen_name", fields = ["a", "b"])
)]
pub struct CompoundNameCollision {}

#[entity(
    store = "IndexNamesStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "email", value(item(prim = "Text", max_len = 32))),
        field(name = "lower_email", value(item(prim = "Text", max_len = 32)))
    ),
    index(name = "chosen_name", field = "lower_email"),
    index(name = "chosen_name", field = "LOWER(email)")
)]
pub struct ExpressionNameCollision {}

#[entity(
    store = "IndexNamesStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "score", value(item(prim = "Nat64"))),
        field(name = "active", value(item(prim = "Bool")))
    ),
    index(name = "chosen_name", field = "score", predicate = "active = true"),
    index(name = "chosen_name", field = "score", predicate = "active = false")
)]
pub struct PartialNameCollision {}

fn main() {}
