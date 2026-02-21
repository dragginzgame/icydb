use crate::{
    design::prelude::*,
    traits::{NumCast, NumFromPrimitive},
};

/// Return true when the finite float has a non-zero fractional component.
const fn has_fractional_part(value: f64) -> bool {
    value.is_finite() && value.to_bits() != value.trunc().to_bits()
}

/// Convert an arbitrary numeric value into Decimal with fractional preservation.
///
/// This keeps fractional float literals (e.g. `0.5`) from being truncated through
/// `to_i64`/`to_u64` before conversion.
pub(crate) fn try_cast_decimal<N: NumCast + Clone>(value: &N) -> Option<Decimal> {
    if let Some(float) = value.to_f64()
        && has_fractional_part(float)
    {
        return Decimal::from_f64_lossy(float);
    }

    value
        .to_i64()
        .and_then(Decimal::from_i64)
        .or_else(|| value.to_u64().and_then(Decimal::from_u64))
        .or_else(|| value.to_f64().and_then(Decimal::from_f64_lossy))
}
