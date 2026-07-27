//! Module: base::types::num
//!
//! Responsibility: base domain type declarations.
//! Does not own: runtime storage, query execution, or validator implementation internals.
//! Boundary: declares macro-modeled domain wrappers and records for downstream schemas.

use crate::prelude::*;

///
/// Degrees (°)
///

#[newtype(
    source_key = "crates/icydb/src/base/types/num.rs::newtype::1",
    primitive = "Nat16",
    item(prim = "Nat16"),
    ty(rule(
        source_key = "icydb.base.rule.num.degrees.range.v1",
        kind = "numeric_range_inclusive",
        args(0, 360)
    ))
)]
pub struct Degrees {}

///
/// Percent
///
/// basic percentage as an integer
///

#[newtype(
    source_key = "crates/icydb/src/base/types/num.rs::newtype::2",
    primitive = "Nat8",
    item(prim = "Nat8"),
    ty(rule(
        source_key = "icydb.base.rule.num.percent.range.v1",
        kind = "numeric_range_inclusive",
        args(0, 100)
    ))
)]
pub struct Percent {}

///
/// PercentModifier
///

#[newtype(
    source_key = "crates/icydb/src/base/types/num.rs::newtype::3",
    primitive = "Nat16",
    item(prim = "Nat16"),
    ty(rule(
        source_key = "icydb.base.rule.num.percent_modifier.range.v1",
        kind = "numeric_range_inclusive",
        args(0, 10_000)
    ))
)]
pub struct PercentModifier {}

///
/// DecimalRange
///

#[record(
    source_key = "crates/icydb/src/base/types/num.rs::record::1",
    fields(
        field(
            source_key = "min",
            ident = "min",
            value(item(prim = "Decimal", scale = 18))
        ),
        field(
            source_key = "max",
            ident = "max",
            value(item(prim = "Decimal", scale = 18))
        ),
    ),
    traits(remove(ValidateCustom))
)]
pub struct DecimalRange {}

impl DecimalRange {
    #[must_use]
    pub const fn new(min: Decimal, max: Decimal) -> Self {
        Self { min, max }
    }
}

impl ValidateCustom for DecimalRange {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        let validator = base::validator::num::Lte::new(self.max);

        validator.validate(&self.min, ctx);
    }
}

///
/// DurationRange
///

#[record(
    source_key = "crates/icydb/src/base/types/num.rs::record::2",
    fields(
        field(source_key = "min", ident = "min", value(item(prim = "Duration"))),
        field(source_key = "max", ident = "max", value(item(prim = "Duration"))),
    ),
    traits(remove(ValidateCustom))
)]
pub struct DurationRange {}

impl DurationRange {
    #[must_use]
    pub const fn new(min: Duration, max: Duration) -> Self {
        Self { min, max }
    }
}

impl ValidateCustom for DurationRange {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        let validator = base::validator::num::Lte::new(self.max);

        validator.validate(&self.min, ctx);
    }
}

///
/// Int32Range
///

#[record(
    source_key = "crates/icydb/src/base/types/num.rs::record::3",
    fields(
        field(source_key = "min", ident = "min", value(item(prim = "Int32"))),
        field(source_key = "max", ident = "max", value(item(prim = "Int32"))),
    ),
    traits(remove(ValidateCustom))
)]
pub struct Int32Range {}

impl Int32Range {
    #[must_use]
    pub const fn new(min: i32, max: i32) -> Self {
        Self { min, max }
    }
}

impl ValidateCustom for Int32Range {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        let validator = base::validator::num::Lte::new(self.max);

        validator.validate(&self.min, ctx);
    }
}

///
/// Nat32Range
///

#[record(
    source_key = "crates/icydb/src/base/types/num.rs::record::4",
    fields(
        field(source_key = "min", ident = "min", value(item(prim = "Nat32"))),
        field(source_key = "max", ident = "max", value(item(prim = "Nat32"))),
    ),
    traits(remove(ValidateCustom))
)]
pub struct Nat32Range {}

impl Nat32Range {
    #[must_use]
    pub const fn new(min: u32, max: u32) -> Self {
        Self { min, max }
    }
}

impl ValidateCustom for Nat32Range {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        let validator = base::validator::num::Lte::new(self.max);

        validator.validate(&self.min, ctx);
    }
}
