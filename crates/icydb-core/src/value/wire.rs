use crate::{
    types::*,
    value::{MapValueError, Value, ValueEnum},
};
use serde::{Deserialize, Deserializer};

///
/// ValueWire
/// Serde decode shape used to re-check Value::Map invariants during deserialization.
///

#[derive(Deserialize)]
enum ValueWire {
    Account(Account),
    Blob(Vec<u8>),
    Bool(bool),
    Date(Date),
    Decimal(Decimal),
    Duration(Duration),
    Enum(ValueEnum),
    Float32(Float32),
    Float64(Float64),
    Int(i64),
    Int128(Int128),
    IntBig(Int),
    List(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Null,
    Principal(Principal),
    Subaccount(Subaccount),
    Text(String),
    Timestamp(Timestamp),
    Uint(u64),
    Uint128(Nat128),
    UintBig(Nat),
    Ulid(Ulid),
    Unit,
}

impl ValueWire {
    // Decode recursively while enforcing runtime map invariants.
    fn into_value(self) -> Result<Value, MapValueError> {
        match self {
            Self::Account(v) => Ok(Value::Account(v)),
            Self::Blob(v) => Ok(Value::Blob(v)),
            Self::Bool(v) => Ok(Value::Bool(v)),
            Self::Date(v) => Ok(Value::Date(v)),
            Self::Decimal(v) => Ok(Value::Decimal(v)),
            Self::Duration(v) => Ok(Value::Duration(v)),
            Self::Enum(v) => Ok(Value::Enum(v)),
            Self::Float32(v) => Ok(Value::Float32(v)),
            Self::Float64(v) => Ok(Value::Float64(v)),
            Self::Int(v) => Ok(Value::Int(v)),
            Self::Int128(v) => Ok(Value::Int128(v)),
            Self::IntBig(v) => Ok(Value::IntBig(v)),
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
            Self::UintBig(v) => Ok(Value::UintBig(v)),
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
