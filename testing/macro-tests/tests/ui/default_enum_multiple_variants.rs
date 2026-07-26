use icydb::design::prelude::*;

#[enum_(source_key = "testing/macro-tests/tests/ui/default_enum_multiple_variants.rs::enum_::nested::1",
    variant(source_key = "Pending", ident = "Pending", default),
    variant(source_key = "Active", ident = "Active", default),
    traits(add(Default))
)]
pub struct MultipleEnumDefaults;

fn main() {}
