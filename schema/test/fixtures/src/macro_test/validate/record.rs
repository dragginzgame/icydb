use icydb_model::prelude::*;

///
/// Record
///

#[record(fields(
    field(
        name = "duration_ms",
        value(item(
            prim = "Nat32",
            validator(path = "base::validator::num::Range", args(180000, 604800000))
        ))
    ),
    field(
        name = "attempts",
        value(item(
            prim = "Nat32",
            validator(path = "base::validator::num::Range", args(1, 20))
        ))
    ),
    field(
        name = "bytes",
        value(item(
            prim = "Blob",
            unbounded,
            validator(path = "base::validator::len::Max", args(500))
        )),
    )
))]
pub struct Record {}
