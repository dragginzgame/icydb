use icydb::design::prelude::*;

///
/// Record
///

#[record(
    source_key = "schema/test/fixtures/src/macro_test/validate/record.rs::record::1",
    fields(
        field(
            source_key = "duration_ms",
            ident = "duration_ms",
            value(item(
                prim = "Nat32",
                validator(path = "base::validator::num::Range", args(180000, 604800000))
            ))
        ),
        field(
            source_key = "attempts",
            ident = "attempts",
            value(item(
                prim = "Nat32",
                validator(path = "base::validator::num::Range", args(1, 20))
            ))
        ),
        field(
            source_key = "bytes",
            ident = "bytes",
            value(item(
                prim = "Blob",
                unbounded,
                validator(path = "base::validator::len::Max", args(500))
            )),
        )
    )
)]
pub struct Record {}
