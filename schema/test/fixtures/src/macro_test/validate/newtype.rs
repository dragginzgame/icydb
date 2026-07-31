use icydb_model::prelude::*;

///
/// Blob
///

#[newtype(item(
    prim = "Blob",
    unbounded,
    validator(path = "base::validator::len::Max", args(500))
))]
pub struct Blob {}
