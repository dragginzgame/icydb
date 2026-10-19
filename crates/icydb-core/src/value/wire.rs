//! Module: value::wire
//! Defines serde-only decode helpers that rebuild public runtime value wrappers
//! from the stable `Value` enum wire shape.

use crate::value::{VALUE_WIRE_TYPE_NAME, VALUE_WIRE_VARIANT_LABELS, Value, ValueWireVariant};

use serde::{
    Deserialize, Deserializer,
    de::{self, EnumAccess, VariantAccess, Visitor},
};
use serde_bytes::ByteBuf;
use std::{fmt, marker::PhantomData};

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
            ValueWireVariant::Int64 => Ok(Value::Int64(payload.newtype_variant()?)),
            ValueWireVariant::Int128 => Ok(Value::Int128(payload.newtype_variant()?)),
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
            ValueWireVariant::Nat64 => Ok(Value::Nat64(payload.newtype_variant()?)),
            ValueWireVariant::Nat128 => Ok(Value::Nat128(payload.newtype_variant()?)),
            ValueWireVariant::Ulid => Ok(Value::Ulid(payload.newtype_variant()?)),
            ValueWireVariant::Unit => {
                payload.unit_variant()?;
                Ok(Value::Unit)
            }
            ValueWireVariant::U256 => Ok(Value::U256(payload.newtype_variant()?)),
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
