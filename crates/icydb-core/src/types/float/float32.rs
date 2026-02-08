use crate::{
    prelude::*,
    traits::{
        AsView, FieldValue, FieldValueKind, Inner, NumFromPrimitive, NumToPrimitive, SanitizeAuto,
        SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, Visitable,
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
/// Float32
///
/// Finite f32 only; -0.0 canonically stored as 0.0
///

#[repr(transparent)]
#[derive(CandidType, Clone, Copy, Debug, Default, Display, Serialize)]
pub struct Float32(f32);

impl Float32 {
    #[must_use]
    /// Fallible constructor that rejects non-finite values and normalizes -0.0.
    pub fn try_new(v: f32) -> Option<Self> {
        if !v.is_finite() {
            return None;
        }

        // canonicalize -0.0 → 0.0
        Some(Self(if v == 0.0 { 0.0 } else { v }))
    }

    #[must_use]
    pub const fn get(self) -> f32 {
        self.0
    }

    #[must_use]
    pub const fn to_be_bytes(&self) -> [u8; 4] {
        self.0.to_bits().to_be_bytes()
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, Float32DecodeError> {
        if bytes.len() != 4 {
            return Err(Float32DecodeError::InvalidSize { len: bytes.len() });
        }

        let mut buf = [0u8; 4];
        buf.copy_from_slice(bytes);
        let value = f32::from_bits(u32::from_be_bytes(buf));
        Self::try_new(value).ok_or(Float32DecodeError::NonFinite)
    }
}

impl AsView for Float32 {
    type ViewType = f32;

    fn as_view(&self) -> Self::ViewType {
        self.0
    }

    // NOTE: View inputs are normalized to preserve invariants (finite only, -0.0 → 0.0).
    fn from_view(view: f32) -> Self {
        let normalized = if view.is_finite() {
            if view == 0.0 { 0.0 } else { view }
        } else {
            0.0
        };

        Self::try_new(normalized).unwrap_or(Self(0.0))
    }
}

impl Eq for Float32 {}

impl PartialEq for Float32 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl FieldValue for Float32 {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Float32(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Float32(v) => Some(*v),
            _ => None,
        }
    }
}

#[allow(clippy::cast_precision_loss)]
impl From<i32> for Float32 {
    fn from(n: i32) -> Self {
        Self(n as f32)
    }
}

impl Inner<Self> for Float32 {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl TryFrom<f32> for Float32 {
    type Error = ();
    fn try_from(v: f32) -> Result<Self, Self::Error> {
        Self::try_new(v).ok_or(())
    }
}

impl From<Float32> for f32 {
    fn from(x: Float32) -> Self {
        x.0
    }
}

#[allow(clippy::cast_precision_loss)]
#[allow(clippy::cast_possible_truncation)]
impl NumFromPrimitive for Float32 {
    fn from_i64(n: i64) -> Option<Self> {
        // i64 always finite in f32 (though not exact)
        Self::try_new(n as f32)
    }

    fn from_u64(n: u64) -> Option<Self> {
        Self::try_new(n as f32)
    }

    fn from_f32(n: f32) -> Option<Self> {
        Self::try_new(n)
    }

    fn from_f64(n: f64) -> Option<Self> {
        // reject out-of-range before casting
        if !n.is_finite() {
            return None;
        }
        if n < f64::from(f32::MIN) || n > f64::from(f32::MAX) {
            return None;
        }

        Self::try_new(n as f32)
    }
}

impl NumToPrimitive for Float32 {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }
    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
    fn to_f32(&self) -> Option<f32> {
        Some(self.0)
    }
    fn to_f64(&self) -> Option<f64> {
        Some(f64::from(self.0))
    }
}

impl Hash for Float32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.0.to_bits()); // stable 4-byte IEEE-754
    }
}

impl Ord for Float32 {
    fn cmp(&self, other: &Self) -> Ordering {
        // safe: no NaN, -0 normalized
        self.0.partial_cmp(&other.0).unwrap()
    }
}

impl PartialOrd for Float32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl SanitizeAuto for Float32 {}

impl SanitizeCustom for Float32 {}

///
/// Float32DecodeError
///

#[derive(Debug, ThisError)]
pub enum Float32DecodeError {
    #[error("invalid float32 length: {len} bytes")]
    InvalidSize { len: usize },
    #[error("non-finite float32 payload")]
    NonFinite,
}

impl TryFrom<&[u8]> for Float32 {
    type Error = Float32DecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl UpdateView for Float32 {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) -> Result<(), crate::traits::Error> {
        *self = v;

        Ok(())
    }
}

impl ValidateAuto for Float32 {}

impl ValidateCustom for Float32 {
    fn validate_custom(&self, ctx: &mut dyn VisitorContext) {
        if !self.0.is_finite() {
            ctx.issue("Float32 must be finite");
        }
    }
}

impl Visitable for Float32 {}

impl<'de> Deserialize<'de> for Float32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = f32::deserialize(deserializer)?;
        Self::try_new(value)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid Float32 value: {value}")))
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::value::{Error as DeError, F32Deserializer};

    #[test]
    fn deserialize_normalizes_negative_zero() {
        let value =
            Float32::deserialize(F32Deserializer::<DeError>::new(-0.0)).expect("deserialize -0.0");
        assert_eq!(value.to_be_bytes(), 0.0f32.to_bits().to_be_bytes());
    }

    #[test]
    fn deserialize_rejects_non_finite() {
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(Float32::deserialize(F32Deserializer::<DeError>::new(value)).is_err());
        }
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn from_view_normalizes_non_finite() {
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let normalized = Float32::from_view(value);
            assert_eq!(normalized.get(), 0.0);
        }
    }

    #[test]
    fn from_view_normalizes_negative_zero() {
        let normalized = Float32::from_view(-0.0);
        assert_eq!(normalized.to_be_bytes(), 0.0f32.to_bits().to_be_bytes());
    }
}
