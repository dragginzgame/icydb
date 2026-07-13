use icydb::design::prelude::*;

#[enum_(
    variant(
        ident = "Principal",
        value(item(prim = "Principal")),
        default
    ),
    variant(ident = "Missing"),
    traits(add(Default))
)]
pub struct InvalidPayloadDefault;

fn main() {}
