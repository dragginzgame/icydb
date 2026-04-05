use crate::types::Decimal;

///
/// NumericValue
///
/// Fallible numeric round-trip contract used by generic validators and sanitizers.
/// Implementors convert through `Decimal` so numeric policy stays explicit and local.
///

pub trait NumericValue: Sized {
    /// Convert this value into Decimal for generic numeric handling.
    fn try_to_decimal(&self) -> Option<Decimal>;

    /// Rebuild the value from Decimal after generic numeric handling.
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
