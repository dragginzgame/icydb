use icydb::design::prelude::*;

#[newtype(source_key = "testing/macro-tests/tests/ui/default_newtype_missing_constructor.rs::newtype::1",
    primitive = "Ulid",
    item(prim = "Ulid"),
    traits(add(Default))
)]
pub struct MissingNewtypeDefault;

fn main() {}
