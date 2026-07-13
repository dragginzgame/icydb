use icydb::design::prelude::*;

#[enum_(
    variant(ident = "Pending"),
    variant(ident = "Active"),
    traits(add(Default))
)]
pub struct MissingEnumDefault;

fn main() {}
