use icydb_model::prelude::*;

#[entity(store = "UnusedStore", version = 1, pk(field = "id"),
    constraint(name = "nonnegative", check = "qty >= -1"),
    fields(field(name = "id", value(item(prim = "Nat64"))), field(name = "qty", value(item(prim = "Nat32")))))]
pub struct NegativeUnsignedCheck;

#[entity(store = "UnusedStore", version = 1, pk(field = "id"),
    index(field = "qty", predicate = "qty > 1.5"),
    fields(field(name = "id", value(item(prim = "Nat64"))), field(name = "qty", value(item(prim = "Nat32")))))]
pub struct FractionalUnsignedIndex;

fn main() {}
