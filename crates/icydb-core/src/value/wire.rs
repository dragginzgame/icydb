//! Module: value::wire
//! Defines serde-only decode helpers that rebuild public runtime value wrappers
//! from the stable `Value` enum wire shape.

use crate::{
    types::*,
    value::{VALUE_WIRE_TYPE_NAME, VALUE_WIRE_VARIANT_LABELS, Value, ValueWireVariant},
};
use candid::{Int as WrappedInt, Nat as WrappedNat};
use num_bigint::{BigInt, BigUint, Sign as BigIntSign};
use serde::{
    Deserialize, Deserializer,
    de::{self, EnumAccess, VariantAccess, Visitor},
};
use serde_bytes::ByteBuf;
use std::{fmt, marker::PhantomData};

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
/// ValueWireVisitor
///
/// ValueWireVisitor decodes the stable externally tagged `Value` wire shape
/// directly through serde enum access and re-applies runtime map invariants.
///

struct ValueWireVisitor(PhantomData<()>);

impl ValueWireVisitor {
    fn decode_map_entries<E>(entries: Vec<(Value, Value)>) -> Result<Value, E>
    where
        E: de::Error,
    {
        Value::from_map(entries).map_err(E::custom)
    }
}

impl<'de> Visitor<'de> for ValueWireVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the stable Value enum wire shape")
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: EnumAccess<'de>,
    {
        let (variant, payload) = data.variant::<String>()?;

        let Some(variant_tag) = ValueWireVariant::from_label(variant.as_str()) else {
            return Err(de::Error::unknown_variant(
                variant.as_str(),
                VALUE_WIRE_VARIANT_LABELS,
            ));
        };

        match variant_tag {
            ValueWireVariant::Account => Ok(Value::Account(payload.newtype_variant()?)),
            ValueWireVariant::Blob => Ok(Value::Blob(
                payload.newtype_variant::<ByteBuf>()?.into_vec(),
            )),
            ValueWireVariant::Bool => Ok(Value::Bool(payload.newtype_variant()?)),
            ValueWireVariant::Date => Ok(Value::Date(payload.newtype_variant()?)),
            ValueWireVariant::Decimal => Ok(Value::Decimal(payload.newtype_variant()?)),
            ValueWireVariant::Duration => Ok(Value::Duration(payload.newtype_variant()?)),
            ValueWireVariant::Enum => Ok(Value::Enum(payload.newtype_variant()?)),
            ValueWireVariant::Float32 => Ok(Value::Float32(payload.newtype_variant()?)),
            ValueWireVariant::Float64 => Ok(Value::Float64(payload.newtype_variant()?)),
            ValueWireVariant::Int => Ok(Value::Int(payload.newtype_variant()?)),
            ValueWireVariant::Int128 => Ok(Value::Int128(payload.newtype_variant()?)),
            ValueWireVariant::IntBig => Ok(Value::IntBig(
                payload.newtype_variant::<IntBigWire>()?.into_inner(),
            )),
            ValueWireVariant::List => Ok(Value::List(payload.newtype_variant()?)),
            ValueWireVariant::Map => {
                let entries = payload.newtype_variant::<Vec<(Value, Value)>>()?;
                Self::decode_map_entries(entries)
            }
            ValueWireVariant::Null => {
                payload.unit_variant()?;
                Ok(Value::Null)
            }
            ValueWireVariant::Principal => Ok(Value::Principal(payload.newtype_variant()?)),
            ValueWireVariant::Subaccount => Ok(Value::Subaccount(payload.newtype_variant()?)),
            ValueWireVariant::Text => Ok(Value::Text(payload.newtype_variant()?)),
            ValueWireVariant::Timestamp => Ok(Value::Timestamp(payload.newtype_variant()?)),
            ValueWireVariant::Uint => Ok(Value::Uint(payload.newtype_variant()?)),
            ValueWireVariant::Uint128 => Ok(Value::Uint128(payload.newtype_variant()?)),
            ValueWireVariant::UintBig => Ok(Value::UintBig(
                payload.newtype_variant::<UintBigWire>()?.into_inner(),
            )),
            ValueWireVariant::Ulid => Ok(Value::Ulid(payload.newtype_variant()?)),
            ValueWireVariant::Unit => {
                payload.unit_variant()?;
                Ok(Value::Unit)
            }
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_enum(
            VALUE_WIRE_TYPE_NAME,
            VALUE_WIRE_VARIANT_LABELS,
            ValueWireVisitor(PhantomData),
        )
    }
}
