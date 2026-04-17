use crate::types::decimal::Decimal;
use candid::CandidType;
use serde::Deserialize;
use serde_bytes::ByteBuf;

impl CandidType for Decimal {
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
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum DecimalPayload {
            Binary((ByteBuf, u32)),
            Text(String),
        }

        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            return s.parse::<Self>().map_err(serde::de::Error::custom);
        }

        // Candid currently reports non-human-readable, but Decimal's Candid wire type is `text`.
        // Accept both payloads here so Candid decode remains correct while binary formats
        // continue to use the canonical `(mantissa_bytes, scale)` shape.
        let payload: DecimalPayload = Deserialize::deserialize(deserializer)?;
        let (mantissa_bytes, scale) = match payload {
            DecimalPayload::Binary(parts) => parts,
            DecimalPayload::Text(s) => {
                return s.parse::<Self>().map_err(serde::de::Error::custom);
            }
        };

        if mantissa_bytes.len() != 16 {
            return Err(serde::de::Error::custom(format!(
                "invalid decimal mantissa length: {} bytes (expected 16)",
                mantissa_bytes.len()
            )));
        }

        let mut mantissa_buf = [0u8; 16];
        mantissa_buf.copy_from_slice(mantissa_bytes.as_ref());
        let mantissa = i128::from_be_bytes(mantissa_buf);

        Self::checked_from_mantissa_scale(mantissa, scale)
            .ok_or_else(|| serde::de::Error::custom("invalid decimal binary payload"))
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
