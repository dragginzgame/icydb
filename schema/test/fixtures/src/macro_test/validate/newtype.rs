use icydb::design::prelude::*;

///
/// Blob
///

#[newtype(
    source_key = "schema/test/fixtures/src/macro_test/validate/newtype.rs::newtype::1",
    primitive = "Blob",
    item(
        prim = "Blob",
        unbounded,
        validator(path = "base::validator::len::Max", args(500))
    )
)]
pub struct Blob {}
