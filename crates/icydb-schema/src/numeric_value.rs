//! Responsibility: scalar and newtype conversion through the canonical
//! decimal numeric representation.
//! Does not own: database coercion, arithmetic, comparison, or ordering policy.
//! Boundary: typed numeric values <-> `Decimal` for generic typed policies.

use crate::{Decimal, Duration, Float32, Float64, Timestamp};

/// Fallible numeric round-trip contract used by generic validators and sanitizers.
///
/// Implementors convert through `Decimal` so typed numeric policy stays
/// explicit and local. Database evaluation semantics remain under `db::numeric`.
pub trait NumericValue: Sized {
    /// Convert this value into `Decimal` for generic numeric handling.
    fn try_to_decimal(&self) -> Option<Decimal>;

    /// Rebuild the value from `Decimal` after generic numeric handling.
    fn try_from_decimal(value: Decimal) -> Option<Self>;
}

macro_rules! impl_numeric_value_signed {
    ($($ty:ty),* $(,)?) => {
        $(
            impl NumericValue for $ty {
                fn try_to_decimal(&self) -> Option<Decimal> {
                    Decimal::from_i128(i128::from(*self))
                }

                fn try_from_decimal(value: Decimal) -> Option<Self> {
                    value.to_i128().and_then(|inner| Self::try_from(inner).ok())
                }
            }
        )*
    };
}

macro_rules! impl_numeric_value_unsigned {
    ($($ty:ty),* $(,)?) => {
        $(
            impl NumericValue for $ty {
                fn try_to_decimal(&self) -> Option<Decimal> {
                    Decimal::from_u128(u128::from(*self))
                }

                fn try_from_decimal(value: Decimal) -> Option<Self> {
                    value.to_u128().and_then(|inner| Self::try_from(inner).ok())
                }
            }
        )*
    };
}

impl_numeric_value_signed!(i8, i16, i32, i64, i128);
impl_numeric_value_unsigned!(u8, u16, u32, u64, u128);

impl NumericValue for isize {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_i128(i128::try_from(*self).ok()?)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_i128().and_then(|inner| Self::try_from(inner).ok())
    }
}

impl NumericValue for usize {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_u128(u128::try_from(*self).ok()?)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_u128().and_then(|inner| Self::try_from(inner).ok())
    }
}

impl NumericValue for f32 {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_f32_lossy(*self)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_f32()
    }
}

impl NumericValue for f64 {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_f64_lossy(*self)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_f64()
    }
}

impl NumericValue for Duration {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_u64(self.as_millis())
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_u64().map(Self::from_millis)
    }
}

impl NumericValue for Float32 {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_f32_lossy(self.get())
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_f32().and_then(Self::try_new)
    }
}

impl NumericValue for Float64 {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_f64_lossy(self.get())
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_f64().and_then(Self::try_new)
    }
}

impl NumericValue for Timestamp {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_i64(self.as_millis())
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_i64().map(Self::from_millis)
    }
}
