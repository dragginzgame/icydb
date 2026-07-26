use icydb::design::prelude::*;

#[enum_(source_key = "testing/macro-tests/tests/ui/default_enum_payload_bound.rs::enum_::nested::1",
    variant(source_key = "Principal", ident = "Principal",
        value(item(prim = "Principal")),
        default
    ),
    variant(source_key = "Missing", ident = "Missing"),
    traits(add(Default))
)]
pub struct InvalidPayloadDefault;

fn main() {}
