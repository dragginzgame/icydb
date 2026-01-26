use crate::{
    prelude::*,
    traits::{
        FieldValue, Inner, NumFromPrimitive, NumToPrimitive, SanitizeAuto, SanitizeCustom,
        UpdateView, ValidateAuto, ValidateCustom, View, ViewError, Visitable,
    },
    visitor::VisitorContext,
};
use candid::CandidType;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};
use thiserror::Error as ThisError;

///
/// Float64
///
/// Finite f64 only; -0.0 canonically stored as 0.0
///

#[repr(transparent)]
#[derive(CandidType, Clone, Copy, Debug, Default, Display, Serialize)]
pub struct Float64(f64);

impl Float64 {
    #[must_use]
    /// Fallible constructor that rejects non-finite values and normalizes -0.0.
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

#[derive(Debug, ThisError)]
pub enum Float64DecodeError {
    #[error("invalid float64 length: {len} bytes")]
    InvalidSize { len: usize },
    #[error("non-finite float64 payload")]
    NonFinite,
}

impl TryFrom<&[u8]> for Float64 {
    type Error = Float64DecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl Eq for Float64 {}

impl PartialEq for Float64 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl FieldValue for Float64 {
    fn to_value(&self) -> Value {
        Value::Float64(*self)
    }
}

impl From<i32> for Float64 {
    fn from(n: i32) -> Self {
        Self(f64::from(n))
    }
}

impl Inner<Self> for Float64 {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
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

#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_possible_truncation)]
impl NumFromPrimitive for Float64 {
    fn from_i64(n: i64) -> Option<Self> {
        Self::try_new(n as f64)
    }

    fn from_u64(n: u64) -> Option<Self> {
        Self::try_new(n as f64)
    }

    fn from_f32(n: f32) -> Option<Self> {
        Self::try_new(f64::from(n))
    }

    fn from_f64(n: f64) -> Option<Self> {
        // reject out-of-range before casting
        if !n.is_finite() {
            return None;
        }

        Self::try_new(n)
    }
}

#[allow(clippy::cast_possible_truncation)]
impl NumToPrimitive for Float64 {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }
    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
    fn to_f32(&self) -> Option<f32> {
        Some(self.0 as f32)
    }
    fn to_f64(&self) -> Option<f64> {
        Some(self.0)
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

impl UpdateView for Float64 {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) -> Result<(), ViewError> {
        *self = v;
        Ok(())
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

impl View for Float64 {
    type ViewType = f64;

    fn to_view(&self) -> Self::ViewType {
        self.0
    }

    // NOTE: Non-finite view inputs are rejected during view conversion to avoid panics.
    fn from_view(view: f64) -> Result<Self, ViewError> {
        Self::try_new(view).ok_or(ViewError::Float64NonFinite { value: view })
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

///
/// TESTS
///

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

    #[test]
    fn from_view_rejects_non_finite() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(Float64::from_view(value).is_err());
        }
    }
}
