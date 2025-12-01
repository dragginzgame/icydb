use crate::prelude::*;

///
/// Degrees (°)
///

#[newtype(
    primitive = "Nat16",
    item(prim = "Nat16"),
    ty(validator(path = "validator::num::Range", args(0, 360)))
)]
pub struct Degrees {}

///
/// Percent
///
/// basic percentage as an integer
///

#[newtype(
    primitive = "Nat8",
    item(prim = "Nat8"),
    ty(validator(path = "validator::num::Range", args(0, 100)))
)]
pub struct Percent {}

///
/// PercentModifier
///

#[newtype(
    primitive = "Nat16",
    item(prim = "Nat16"),
    ty(validator(path = "validator::num::Range", args(0, 10_000)))
)]
pub struct PercentModifier {}

///
/// DecimalRange
///

#[record(
    fields(
        field(ident = "min", value(item(prim = "Decimal"))),
        field(ident = "max", value(item(prim = "Decimal"))),
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
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        validator::num::Lte::new(self.max)
            .validate(&self.min)
            .map_err(ErrorTree::from)
    }
}

///
/// DurationRange
///

#[record(
    fields(
        field(ident = "min", value(item(prim = "Duration"))),
        field(ident = "max", value(item(prim = "Duration"))),
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
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        validator::num::Lte::new(self.max)
            .validate(&self.min)
            .map_err(ErrorTree::from)
    }
}

///
/// Int32Range
///

#[record(
    fields(
        field(ident = "min", value(item(prim = "Int32"))),
        field(ident = "max", value(item(prim = "Int32"))),
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
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        validator::num::Lte::new(self.max)
            .validate(&self.min)
            .map_err(ErrorTree::from)
    }
}

///
/// Nat32Range
///

#[record(
    fields(
        field(ident = "min", value(item(prim = "Nat32"))),
        field(ident = "max", value(item(prim = "Nat32"))),
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
    fn validate_custom(&self) -> Result<(), ErrorTree> {
        validator::num::Lte::new(self.max)
            .validate(&self.min)
            .map_err(ErrorTree::from)
    }
}
