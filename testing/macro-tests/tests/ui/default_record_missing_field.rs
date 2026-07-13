use icydb::design::prelude::*;

#[record(
    fields(
        field(ident = "name", value(item(prim = "Text", unbounded))),
        field(ident = "region", value(item(prim = "Text", unbounded)))
    ),
    traits(add(Default))
)]
pub struct MissingRecordDefault;

fn main() {}
