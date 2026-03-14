use icydb::design::prelude::*;

///
/// OptionalThreshold
///
/// Demonstrates optional validation: the field is only validated when set.
/// - `None` is allowed.
/// - `Some(n)` must satisfy `n > 10`.
///
/// The `opt` flag makes the field optional; the validator still
/// operates on the inner value when present.
///

#[record(fields(field(
    ident = "threshold",
    value(
        opt,
        item(prim = "Nat32", validator(path = "base::validator::num::Gt", args(10)))
    )
)))]
pub struct OptionalThreshold {}
