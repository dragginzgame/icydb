use icydb::design::prelude::*;

///
/// Blob
///

#[newtype(
    primitive = "Blob",
    item(
        prim = "Blob",
        validator(path = "base::validator::len::Max", args(500))
    )
)]
pub struct Blob {}
