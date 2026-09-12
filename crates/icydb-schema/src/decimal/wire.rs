use crate::{TypeParseError, decimal::Decimal};
use candid::CandidType;
use serde::{Deserialize, Serialize};

impl CandidType for Decimal {
    fn ty() -> candid::types::Type {
        <String as CandidType>::ty()
    }

    fn _ty() -> candid::types::Type {
        candid::types::TypeInner::Text.into()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_text(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Candid and Serde both emit text, including non-human-readable formats.
        let text = String::deserialize(deserializer)?;
        text.parse::<Self>()
            .map_err(|_| serde::de::Error::custom(TypeParseError::InvalidDecimal))
    }
}

impl Serialize for Decimal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

// lossy f32 done on purpose as these ORM floats aren't designed for NaN etc.
impl From<f32> for Decimal {
    fn from(n: f32) -> Self {
        Self::from_f32_lossy(n).unwrap_or(Self::ZERO)
    }
}

impl From<f64> for Decimal {
    fn from(n: f64) -> Self {
        Self::from_f64_lossy(n).unwrap_or(Self::ZERO)
    }
}

macro_rules! impl_decimal_from_signed_int {
    ( $( $type:ty ),* ) => {
        $(
            impl From<$type> for Decimal {
                fn from(n: $type) -> Self {
                    Self {
                        mantissa: i128::from(n),
                        scale: 0,
                    }
                }
            }
        )*
    };
}

macro_rules! impl_decimal_from_unsigned_int {
    ( $( $type:ty ),* ) => {
        $(
            impl From<$type> for Decimal {
                fn from(n: $type) -> Self {
                    Self {
                        mantissa: i128::from(n),
                        scale: 0,
                    }
                }
            }
        )*
    };
}

impl_decimal_from_unsigned_int!(u8, u16, u32, u64);
impl_decimal_from_signed_int!(i8, i16, i32, i64, i128);

impl From<u128> for Decimal {
    fn from(n: u128) -> Self {
        let mantissa = i128::try_from(n).unwrap_or(i128::MAX);
        Self { mantissa, scale: 0 }
    }
}
