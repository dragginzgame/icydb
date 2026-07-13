use icydb::design::prelude::*;

#[enum_(
    variant(ident = "Pending", default),
    variant(ident = "Active", default),
    traits(add(Default))
)]
pub struct MultipleEnumDefaults;

fn main() {}
