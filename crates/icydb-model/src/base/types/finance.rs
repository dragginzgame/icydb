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
    source_key = "crates/icydb/src/base/types/finance.rs::newtype::1",
    primitive = "Decimal",
    item(prim = "Decimal", scale = 2),
    ty(
        normalizer(path = "base::normalizer::num::RoundDecimalPlaces", args(2)),
        rule(
            source_key = "icydb.base.rule.finance.usd.nonnegative.v1",
            kind = "numeric_minimum_inclusive",
            args(0)
        )
    )
)]
pub struct Usd {}

///
/// E8s
///
/// Decimal amount constrained to at most 8 decimal places and non-negative.
///

#[newtype(
    source_key = "crates/icydb/src/base/types/finance.rs::newtype::2",
    primitive = "Decimal",
    item(prim = "Decimal", scale = 8),
    ty(rule(
        source_key = "icydb.base.rule.finance.e8s.nonnegative.v1",
        kind = "numeric_minimum_inclusive",
        args(0)
    ))
)]
pub struct E8s {}

///
/// E18s
///
/// Decimal amount constrained to at most 18 decimal places and non-negative.
///

#[newtype(
    source_key = "crates/icydb/src/base/types/finance.rs::newtype::3",
    primitive = "Decimal",
    item(prim = "Decimal", scale = 18),
    ty(rule(
        source_key = "icydb.base.rule.finance.e18s.nonnegative.v1",
        kind = "numeric_minimum_inclusive",
        args(0)
    ))
)]
pub struct E18s {}
