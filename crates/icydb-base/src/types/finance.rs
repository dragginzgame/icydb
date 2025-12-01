use crate::prelude::*;

///
/// Usd
///
/// Decimal amount denominated in USD.
/// - Enforces at most 2 decimal places.
/// - Must be non-negative.
///

#[newtype(
    primitive = "Decimal",
    item(prim = "Decimal"),
    ty(
        sanitizer(path = "sanitizer::num::RoundDecimalPlaces", args(2u32)),
        validator(path = "validator::decimal::MaxDecimalPlaces", args(2)),
        validator(path = "validator::num::Gte", args(0))
    )
)]
pub struct Usd {}
