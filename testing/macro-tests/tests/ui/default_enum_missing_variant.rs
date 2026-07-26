use icydb::design::prelude::*;

#[enum_(source_key = "testing/macro-tests/tests/ui/default_enum_missing_variant.rs::enum_::nested::1",
    variant(source_key = "Pending", ident = "Pending"),
    variant(source_key = "Active", ident = "Active"),
    traits(add(Default))
)]
pub struct MissingEnumDefault;

fn main() {}
