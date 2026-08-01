use icydb_model::prelude::*;

#[list(item(prim = "Nat8"), traits(add(IntoIterator)))]
pub struct RedundantListIntoIterator {}

#[map(
    key(prim = "Nat8"),
    value(item(prim = "Nat16")),
    traits(add(FromIterator))
)]
pub struct RedundantMapFromIterator {}

#[record(
    fields(field(name = "value", value(item(prim = "Nat8")))),
    traits(add(IntoIterator))
)]
pub struct UnsupportedRecordIntoIterator {}

fn main() {}
