use crate::{
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Int, Int128, Nat, Nat128, Principal,
        Subaccount, Timestamp, Ulid,
    },
    value::{Value, ValueEnum},
};
use candid::CandidType;
use serde::Deserialize;

//
// OutputValue
//
// Public output-side value boundary used by API and wire surfaces.
// This stays separate from runtime `Value` so public result payloads can move
// off the internal execution representation without forcing a storage or
// planner rewrite in the same slice.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum OutputValue {
    Account(Account),
    Blob(Vec<u8>),
    Bool(bool),
    Date(Date),
    Decimal(Decimal),
    Duration(Duration),
    Enum(OutputValueEnum),
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
    Nat(u64),
    Nat128(Nat128),
    NatBig(Nat),
    Ulid(Ulid),
    Unit,
}

//
// OutputValueEnum
//
// Output-side enum payload contract paired with `OutputValue`.
// Payload stays recursive through `OutputValue` so public boundary conversion
// remains total for data-carrying enum values already representable by
// runtime `Value`.
//

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OutputValueEnum {
    variant: String,
    path: Option<String>,
    payload: Option<Box<OutputValue>>,
}

impl OutputValueEnum {
    #[must_use]
    pub const fn variant(&self) -> &str {
        self.variant.as_str()
    }

    #[must_use]
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    #[must_use]
    pub fn payload(&self) -> Option<&OutputValue> {
        self.payload.as_deref()
    }
}

impl From<Value> for OutputValue {
    fn from(value: Value) -> Self {
        Self::from(&value)
    }
}

impl From<&Value> for OutputValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Account(value) => Self::Account(*value),
            Value::Blob(value) => Self::Blob(value.clone()),
            Value::Bool(value) => Self::Bool(*value),
            Value::Date(value) => Self::Date(*value),
            Value::Decimal(value) => Self::Decimal(*value),
            Value::Duration(value) => Self::Duration(*value),
            Value::Enum(value) => Self::Enum(OutputValueEnum::from(value)),
            Value::Float32(value) => Self::Float32(*value),
            Value::Float64(value) => Self::Float64(*value),
            Value::Int(value) => Self::Int(*value),
            Value::Int128(value) => Self::Int128(*value),
            Value::IntBig(value) => Self::IntBig(value.clone()),
            Value::List(items) => Self::List(items.iter().map(Self::from).collect()),
            Value::Map(entries) => Self::Map(
                entries
                    .iter()
                    .map(|(key, value)| (Self::from(key), Self::from(value)))
                    .collect(),
            ),
            Value::Null => Self::Null,
            Value::Principal(value) => Self::Principal(*value),
            Value::Subaccount(value) => Self::Subaccount(*value),
            Value::Text(value) => Self::Text(value.clone()),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::Nat(value) => Self::Nat(*value),
            Value::Nat128(value) => Self::Nat128(*value),
            Value::NatBig(value) => Self::NatBig(value.clone()),
            Value::Ulid(value) => Self::Ulid(*value),
            Value::Unit => Self::Unit,
        }
    }
}

impl From<ValueEnum> for OutputValueEnum {
    fn from(value: ValueEnum) -> Self {
        Self::from(&value)
    }
}

impl From<&ValueEnum> for OutputValueEnum {
    fn from(value: &ValueEnum) -> Self {
        Self {
            variant: value.variant().to_string(),
            path: value.path().map(ToString::to_string),
            payload: value
                .payload()
                .map(|payload| Box::new(OutputValue::from(payload))),
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::value::{OutputValue, OutputValueEnum, Value, ValueEnum};

    #[test]
    fn output_value_from_runtime_value_keeps_recursive_collection_shape() {
        let runtime = Value::List(vec![
            Value::Nat(7),
            Value::Map(vec![(Value::Text("x".to_string()), Value::Bool(true))]),
        ]);

        assert_eq!(
            OutputValue::from(runtime),
            OutputValue::List(vec![
                OutputValue::Nat(7),
                OutputValue::Map(vec![(
                    OutputValue::Text("x".to_string()),
                    OutputValue::Bool(true),
                )]),
            ]),
        );
    }

    #[test]
    fn output_value_enum_from_runtime_enum_keeps_payload() {
        let runtime =
            ValueEnum::new("Example", Some("test::OutputEnum")).with_payload(Value::Nat(9));

        assert_eq!(
            OutputValueEnum::from(runtime),
            OutputValueEnum {
                variant: "Example".to_string(),
                path: Some("test::OutputEnum".to_string()),
                payload: Some(Box::new(OutputValue::Nat(9))),
            },
        );
    }
}
