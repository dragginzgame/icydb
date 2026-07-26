use icydb::design::prelude::*;

#[record(source_key = "testing/macro-tests/tests/ui/default_record_missing_field.rs::record::1",
    fields(
        field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded))),
        field(source_key = "region", ident = "region", value(item(prim = "Text", unbounded)))
    ),
    traits(add(Default))
)]
pub struct MissingRecordDefault;

fn main() {}
