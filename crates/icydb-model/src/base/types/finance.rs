//! Module: base::types::finance
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// Usd
///
/// Decimal amount denominated in USD.
/// - Enforces at most 2 decimal places.
/// - Must be non-negative.
///

#[newtype(
    item(prim = "Decimal", scale = 2),
    ty(
        normalizer(path = "base::normalizer::num::RoundDecimalPlaces", args(2)),
        rule(name = "nonnegative", kind = "numeric_minimum_inclusive", args(0))
    )
)]
pub struct Usd {}

///
/// E8s
///
/// Decimal amount constrained to at most 8 decimal places and non-negative.
///

#[newtype(
    item(prim = "Decimal", scale = 8),
    ty(rule(name = "nonnegative", kind = "numeric_minimum_inclusive", args(0)))
)]
pub struct E8s {}

///
/// E18s
///
/// Decimal amount constrained to at most 18 decimal places and non-negative.
///

#[newtype(
    item(prim = "Decimal", scale = 18),
    ty(rule(name = "nonnegative", kind = "numeric_minimum_inclusive", args(0)))
)]
pub struct E18s {}
