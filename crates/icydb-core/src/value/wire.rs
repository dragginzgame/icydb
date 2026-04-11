//! Module: value::wire
//! Defines serde-only wire helpers that rebuild public runtime value wrappers
//! from persisted payload shapes.

use crate::{
    types::*,
    value::{MapValueError, Value, ValueEnum},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};
use serde::{Deserialize, Deserializer, de};
use serde_bytes::ByteBuf;

///
/// IntBigWire
///
/// IntBigWire accepts the persisted bigint `(sign, limbs)` payload shape and
/// rebuilds the public `Int` wrapper without routing through the derived
/// `Deserialize` form of `candid::Int`.
///

struct IntBigWire(Int);

impl IntBigWire {
    fn into_inner(self) -> Int {
        self.0
    }
}

impl<'de> Deserialize<'de> for IntBigWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (sign, limbs): (i8, Vec<u32>) = Deserialize::deserialize(deserializer)?;
        let sign = match sign {
            -1 => BigIntSign::Minus,
            0 => BigIntSign::NoSign,
            1 => BigIntSign::Plus,
            _ => return Err(de::Error::custom(format!("invalid bigint sign {sign}"))),
        };
        let magnitude = BigUint::new(limbs);

        Ok(Self(Int::from(WrappedInt::from(BigInt::from_biguint(
            sign, magnitude,
        )))))
    }
}

///
/// UintBigWire
///
/// UintBigWire accepts the persisted biguint limb payload shape and rebuilds
/// the public `Nat` wrapper without routing through the derived `Deserialize`
/// form of `candid::Nat`.
///

struct UintBigWire(Nat);

impl UintBigWire {
    fn into_inner(self) -> Nat {
        self.0
    }
}

impl<'de> Deserialize<'de> for UintBigWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let limbs: Vec<u32> = Deserialize::deserialize(deserializer)?;
        Ok(Self(Nat::from(WrappedNat::from(BigUint::new(limbs)))))
    }
}

///
/// ValueWire
/// Serde decode shape used to re-check Value::Map invariants during deserialization.
///

#[derive(Deserialize)]
enum ValueWire {
    Account(Account),
    Blob(ByteBuf),
    Bool(bool),
    Date(Date),
    Decimal(Decimal),
    Duration(Duration),
    Enum(ValueEnum),
    Float32(Float32),
    Float64(Float64),
    Int(i64),
    Int128(Int128),
    IntBig(IntBigWire),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Null,
    Principal(Principal),
    Subaccount(Subaccount),
    Text(String),
    Timestamp(Timestamp),
    Uint(u64),
    Uint128(Nat128),
    UintBig(UintBigWire),
    Ulid(Ulid),
    Unit,
}

impl ValueWire {
    // Decode recursively while enforcing runtime map invariants.
    fn into_value(self) -> Result<Value, MapValueError> {
        match self {
            Self::Account(v) => Ok(Value::Account(v)),
            Self::Blob(v) => Ok(Value::Blob(v.into_vec())),
            Self::Bool(v) => Ok(Value::Bool(v)),
            Self::Date(v) => Ok(Value::Date(v)),
            Self::Decimal(v) => Ok(Value::Decimal(v)),
            Self::Duration(v) => Ok(Value::Duration(v)),
            Self::Enum(v) => Ok(Value::Enum(v)),
            Self::Float32(v) => Ok(Value::Float32(v)),
            Self::Float64(v) => Ok(Value::Float64(v)),
            Self::Int(v) => Ok(Value::Int(v)),
            Self::Int128(v) => Ok(Value::Int128(v)),
            Self::IntBig(v) => Ok(Value::IntBig(v.into_inner())),
            Self::List(items) => {
                let items = items
                    .into_iter()
                    .map(Self::into_value)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Value::List(items))
            }
            Self::Map(entries) => {
                let entries = entries
                    .into_iter()
                    .map(|(key, value)| Ok((key.into_value()?, value.into_value()?)))
                    .collect::<Result<Vec<_>, MapValueError>>()?;
                Value::from_map(entries)
            }
            Self::Null => Ok(Value::Null),
            Self::Principal(v) => Ok(Value::Principal(v)),
            Self::Subaccount(v) => Ok(Value::Subaccount(v)),
            Self::Text(v) => Ok(Value::Text(v)),
            Self::Timestamp(v) => Ok(Value::Timestamp(v)),
            Self::Uint(v) => Ok(Value::Uint(v)),
            Self::Uint128(v) => Ok(Value::Uint128(v)),
            Self::UintBig(v) => Ok(Value::UintBig(v.into_inner())),
            Self::Ulid(v) => Ok(Value::Ulid(v)),
            Self::Unit => Ok(Value::Unit),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ValueWire::deserialize(deserializer)?;
        wire.into_value().map_err(serde::de::Error::custom)
    }
}
