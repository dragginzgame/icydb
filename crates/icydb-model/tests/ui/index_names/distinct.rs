use icydb_model::prelude::*;

#[canister(memory_namespace = "distinct_index_names")]
pub struct DistinctIndexNamesCanister {}

#[store(canister = "DistinctIndexNamesCanister", storage(heap()))]
pub struct DistinctIndexNamesStore {}

#[entity(
    store = "DistinctIndexNamesStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "score", value(item(prim = "Nat64")))
    ),
    index(field = "score"),
    index(field = "score", unique)
)]
pub struct UniqueAndNonUnique {}

#[entity(
    store = "DistinctIndexNamesStore",
    version = 1,
    pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "email", value(item(prim = "Text", max_len = 32)))
    ),
    index(field = "email"),
    index(field = "LOWER(email)")
)]
pub struct FieldAndExpression {}

#[entity(
    store = "DistinctIndexNamesStore", version = 1, pk(field = "id"),
    fields(
        field(name = "id", value(item(prim = "Nat64"))),
        field(name = "score", value(item(prim = "Nat64"))),
        field(name = "tenant", value(item(prim = "Nat64"))),
        field(name = "active", value(item(prim = "Bool")))
    ),
    index(field = "score", predicate = "active = true"),
    index(fields = ["score", "tenant"], predicate = "active = false")
)]
pub struct PartialDistinct {}

fn main() {}
