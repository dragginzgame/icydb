//! Module: types::float::float64
//! Defines the finite `f64` wrapper used by value conversion, comparison, and
//! visitor-driven validation.

use crate::{
    prelude::*,
    traits::{
        Atomic, FieldValue, FieldValueKind, NumericValue, SanitizeAuto, SanitizeCustom,
        ValidateAuto, ValidateCustom, Visitable,
    },
    types::Decimal,
    visitor::VisitorContext,
};
use candid::CandidType;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
};
use thiserror::Error as ThisError;

//
// Float64DecodeError
//

#[derive(Debug, ThisError)]
pub enum Float64DecodeError {
    #[error("invalid float64 length: {len} bytes")]
    InvalidSize { len: usize },

    #[error("non-finite float64 payload")]
    NonFinite,
}

//
// Float64
//
// Finite f64 only; -0.0 canonically stored as 0.0
//

#[repr(transparent)]
#[derive(CandidType, Clone, Copy, Debug, Default, Serialize)]
pub struct Float64(f64);

impl Float64 {
    /// Fallible constructor that rejects non-finite values and normalizes -0.0.
    #[must_use]
    pub fn try_new(v: f64) -> Option<Self> {
        if !v.is_finite() {
            return None;
        }

        // canonicalize -0.0 to 0.0 so Eq/Hash/Ord are consistent
        Some(Self(if v == 0.0 { 0.0 } else { v }))
    }

    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }

    #[must_use]
    pub const fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_bits().to_be_bytes()
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, Float64DecodeError> {
        if bytes.len() != 8 {
            return Err(Float64DecodeError::InvalidSize { len: bytes.len() });
        }

        let mut buf = [0u8; 8];
        buf.copy_from_slice(bytes);
        let value = f64::from_bits(u64::from_be_bytes(buf));
        Self::try_new(value).ok_or(Float64DecodeError::NonFinite)
    }
}

impl fmt::Display for Float64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Atomic for Float64 {}

impl Eq for Float64 {}

impl PartialEq for Float64 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl FieldValue for Float64 {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Float64(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Float64(v) => Some(*v),
            _ => None,
        }
    }
}

impl From<i32> for Float64 {
    fn from(n: i32) -> Self {
        Self(f64::from(n))
    }
}

impl TryFrom<f64> for Float64 {
    type Error = ();
    fn try_from(v: f64) -> Result<Self, Self::Error> {
        Self::try_new(v).ok_or(())
    }
}

impl From<Float64> for f64 {
    fn from(x: Float64) -> Self {
        x.0
    }
}

impl NumericValue for Float64 {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_f64_lossy(self.0)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_f64().and_then(Self::try_new)
    }
}

impl Hash for Float64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0.to_bits()); // stable 8-byte IEEE-754
    }
}

impl Ord for Float64 {
    fn cmp(&self, other: &Self) -> Ordering {
        // safe: no NaN, -0 normalized
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl PartialOrd for Float64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SanitizeAuto for Float64 {}

impl SanitizeCustom for Float64 {}

impl TryFrom<&[u8]> for Float64 {
    type Error = Float64DecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl ValidateAuto for Float64 {}

impl ValidateCustom for Float64 {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        if !self.0.is_finite() {
            ctx.issue("Float64 must be finite");
        }
    }
}

impl Visitable for Float64 {}

impl<'de> Deserialize<'de> for Float64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::try_new(value)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid Float64 value: {value}")))
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::value::{Error as DeError, F64Deserializer};

    #[test]
    fn deserialize_normalizes_negative_zero() {
        let value =
            Float64::deserialize(F64Deserializer::<DeError>::new(-0.0)).expect("deserialize -0.0");
        assert_eq!(value.to_be_bytes(), 0.0f64.to_bits().to_be_bytes());
    }

    #[test]
    fn deserialize_rejects_non_finite() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(Float64::deserialize(F64Deserializer::<DeError>::new(value)).is_err());
        }
    }
}
