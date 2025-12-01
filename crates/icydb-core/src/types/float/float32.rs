use crate::{
    Value,
    db::primitives::{DecimalListFilterKind, DecimalRangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, NumFromPrimitive, NumToPrimitive, SanitizeAuto,
        SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
    },
};
use candid::CandidType;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
};

///
/// Float32
///
/// Finite f32 only; -0.0 canonically stored as 0.0
///

#[repr(transparent)]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Display, Serialize)]
pub struct Float32(f32);

impl Float32 {
    #[must_use]
    pub fn try_new(v: f32) -> Option<Self> {
        if !v.is_finite() {
            return None;
        }

        // canonicalize -0.0 → 0.0
        Some(Self(if v == 0.0 { 0.0 } else { v }))
    }

    #[must_use]
    /// Construct from an f32 or return 0.0 if the value is non‑finite.
    /// Canonicalizes -0.0 → 0.0. Use `try_new` to handle errors explicitly.
    pub fn new_or_zero(v: f32) -> Self {
        Self::try_new(v).unwrap_or(Self(0.0))
    }

    #[must_use]
    pub const fn get(self) -> f32 {
        self.0
    }

    #[must_use]
    pub const fn to_be_bytes(&self) -> [u8; 4] {
        self.0.to_bits().to_be_bytes()
    }
}

impl Eq for Float32 {}

impl PartialEq for Float32 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl FieldValue for Float32 {
    fn to_value(&self) -> Value {
        Value::Float32(*self)
    }
}

impl Filterable for Float32 {
    type Filter = DecimalRangeFilterKind;
    type ListFilter = DecimalListFilterKind;
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

impl UpdateView for Float32 {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Float32 {}

impl ValidateCustom for Float32 {}

impl View for Float32 {
    type ViewType = f32;

    fn to_view(&self) -> Self::ViewType {
        self.0
    }

    fn from_view(view: Self::ViewType) -> Self {
        // Preserve invariants: finite only, canonicalize -0.0 → 0.0
        // Fallback to 0.0 for non‑finite inputs to avoid propagating NaN/Inf
        Self::try_new(view).unwrap_or(Self(0.0))
    }
}

impl Visitable for Float32 {}
