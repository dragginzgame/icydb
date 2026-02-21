use crate::design::prelude::*;

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
        sanitizer(path = "base::sanitizer::num::RoundDecimalPlaces", args(2)),
        validator(path = "base::validator::decimal::MaxDecimalPlaces", args(2)),
        validator(path = "base::validator::num::Gte", args(0))
    )
)]
pub struct Usd {}

///
/// E8s
///
/// Decimal amount constrained to at most 8 decimal places and non-negative.
///

#[newtype(
    primitive = "Decimal",
    item(prim = "Decimal", scale = 8),
    ty(
        validator(path = "base::validator::decimal::MaxDecimalPlaces", args(8)),
        validator(path = "base::validator::num::Gte", args(0))
    )
)]
pub struct E8s {}

///
/// E18s
///
/// Decimal amount constrained to at most 18 decimal places and non-negative.
///

#[newtype(
    primitive = "Decimal",
    item(prim = "Decimal", scale = 18),
    ty(
        validator(path = "base::validator::decimal::MaxDecimalPlaces", args(18)),
        validator(path = "base::validator::num::Gte", args(0))
    )
)]
pub struct E18s {}
