use icydb_model::prelude::*;

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

#[record(
    source_key = "schema/test/fixtures/src/macro_test/validate/option.rs::record::1",
    fields(field(
        source_key = "threshold",
        ident = "threshold",
        value(
            opt,
            item(prim = "Nat32", validator(path = "base::validator::num::Gt", args(10)))
        )
    ))
)]
pub struct OptionalThreshold {}
